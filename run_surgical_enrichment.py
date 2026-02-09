#!/usr/bin/env python3
"""
Surgical enrichment runner — processes leads in cohorts using single-tool modes
with strict per-lead guardrails and bulletproof progress saving.

Usage:
    python run_surgical_enrichment.py --cohort A --mode contact_recovery
    python run_surgical_enrichment.py --cohort A --mode false_positive_cleanup
    python run_surgical_enrichment.py --cohort B --mode final_confirmation
    python run_surgical_enrichment.py --cohort C --mode email_verification
    python run_surgical_enrichment.py --cohort A --mode contact_recovery --limit 50
    python run_surgical_enrichment.py --cohort A --mode contact_recovery --force
    python run_surgical_enrichment.py --stats
"""

import sys
import os
import argparse
import signal
import time
import threading
from datetime import date, datetime
from contextlib import contextmanager

sys.path.insert(0, '.')
from main import load_leads_from_csv
from src.enricher import LeadEnricher, _is_empty, OpenAICostTracker
from src.utils import save_leads_to_csv, extract_domain, guess_email, generate_email_guesses, normalize_name, clean_email, make_request

MAX_PAGES_PER_LEAD = 3
MAX_HTTP_REQUESTS_PER_LEAD = 5
MAX_OPENAI_CALLS_PER_LEAD = 1
HARD_TIMEOUT_SECONDS = 30
MAX_ATTEMPTS_BEFORE_FINAL = 2

INPUT_FILE = 'unit8_leads_enriched.csv'
OUTPUT_FILE = 'unit8_leads_enriched.csv'


class RequestCounter:
    def __init__(self, max_requests):
        self.count = 0
        self.max_requests = max_requests

    def increment(self):
        self.count += 1
        if self.count > self.max_requests:
            raise RequestLimitExceeded(f"Exceeded {self.max_requests} HTTP requests for this lead")

    def reset(self):
        self.count = 0


class RequestLimitExceeded(Exception):
    pass


class LeadTimeout(Exception):
    pass


class SurgicalEnricher(LeadEnricher):
    """Subclass that enforces single-tool mode and per-lead guardrails."""

    def __init__(self, mode='contact_recovery'):
        super().__init__()
        self.mode = mode
        self.request_counter = RequestCounter(MAX_HTTP_REQUESTS_PER_LEAD)
        self.page_counter = 0
        self._disable_tools_for_mode(mode)

    def _disable_tools_for_mode(self, mode):
        if mode == 'contact_recovery':
            self.companies_house_api_key = ""
            self.openai_api_key = ""
            self.linkedin_max_attempts = 0
        elif mode == 'false_positive_cleanup':
            self.companies_house_api_key = ""
            self.linkedin_max_attempts = 0
        elif mode == 'email_verification':
            self.companies_house_api_key = ""
            self.openai_api_key = ""
            self.linkedin_max_attempts = 0
        elif mode == 'final_confirmation':
            self.companies_house_api_key = ""
            self.linkedin_max_attempts = 0

    def reset_per_lead_counters(self):
        self.request_counter.reset()
        self.page_counter = 0

    def _enrich_from_website(self, lead):
        if self.mode == 'false_positive_cleanup':
            return {
                'email': '', 'generic_email': '', 'contact': '',
                'contacts': [], 'source': '', 'text': '',
                'known_email_format': '', '_notes': ['website_skipped:false_positive_mode'],
                'company_number': ''
            }
        if self.mode == 'final_confirmation':
            return {
                'email': '', 'generic_email': '', 'contact': '',
                'contacts': [], 'source': '', 'text': self._get_minimal_text(lead),
                'known_email_format': '', '_notes': ['website_minimal:confirmation_mode'],
                'company_number': ''
            }

        result = {
            'email': '', 'generic_email': '', 'contact': '',
            'contacts': [], 'source': '', 'text': '',
            'known_email_format': '', '_notes': [], 'company_number': ''
        }

        if not lead.website:
            return result

        from bs4 import BeautifulSoup
        from urllib.parse import urlparse

        try:
            self.request_counter.increment()
            response = make_request(lead.website, timeout=8)
            if not response or response.status_code >= 400:
                return result

            homepage_soup = BeautifulSoup(response.text, 'lxml')
            homepage_text = homepage_soup.get_text(separator=' ')
            result['text'] = homepage_text
            site_domain = extract_domain(lead.website)

            found_email = self._find_email(homepage_soup, homepage_text, site_domain)
            if found_email:
                if self._is_generic_email(found_email):
                    result['generic_email'] = found_email
                else:
                    result['email'] = found_email
                result['source'] = 'website'

            found_contact = self._find_contact_name(homepage_soup)
            if found_contact:
                result['contact'] = found_contact
                result['source'] = 'website'

            multi = self._find_multiple_contacts(homepage_soup)
            if multi:
                result['contacts'].extend(multi)

            parsed_url = urlparse(response.url)
            root_url = f"{parsed_url.scheme}://{parsed_url.netloc}/"
            discovered = self._discover_nav_pages(homepage_soup, root_url)

            pages_to_visit = discovered[:MAX_PAGES_PER_LEAD - 1]
            for page_url in pages_to_visit:
                try:
                    self.request_counter.increment()
                    self.page_counter += 1
                    resp = make_request(page_url, timeout=8)
                    if not resp or resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, 'lxml')
                    page_text = soup.get_text(separator=' ')
                    result['text'] += "\n" + page_text

                    if not result['email']:
                        found = self._find_email(soup, page_text, site_domain)
                        if found:
                            if self._is_generic_email(found):
                                if not result['generic_email']:
                                    result['generic_email'] = found
                            else:
                                result['email'] = found
                            result['source'] = 'website'

                    if not result['contact']:
                        found = self._find_contact_name(soup)
                        if found:
                            result['contact'] = found
                            result['source'] = 'website'

                    if len(result['contacts']) < 8:
                        multi = self._find_multiple_contacts(soup, max_contacts=8)
                        for c in multi:
                            if c['name'].lower() not in {x['name'].lower() for x in result['contacts']}:
                                result['contacts'].append(c)

                except RequestLimitExceeded:
                    result['_notes'].append('request_limit_reached')
                    break
                except Exception:
                    continue

        except RequestLimitExceeded:
            result['_notes'].append('request_limit_reached')
        except Exception as e:
            result['_notes'].append(f'error:{str(e)[:60]}')

        return result

    def _get_minimal_text(self, lead):
        if not lead.website:
            return ""
        try:
            from bs4 import BeautifulSoup
            self.request_counter.increment()
            response = make_request(lead.website, timeout=8)
            if response and response.status_code == 200:
                soup = BeautifulSoup(response.text, 'lxml')
                return soup.get_text(separator=' ')[:3000]
        except Exception:
            pass
        return ""

    def _get_director_from_companies_house(self, company_name):
        return ""

    def _search_linkedin_for_contact(self, lead):
        return ""


def select_cohort(leads, cohort, force=False):
    today_str = str(date.today())
    targets = []

    for idx, lead in enumerate(leads):
        if not force and lead.last_enriched_date == today_str:
            continue

        if lead.missing_name_final == 'true':
            continue

        if cohort == 'A':
            if _is_empty(lead.contact_name) and lead.website:
                is_social = lead.website and any(d in lead.website.lower() for d in
                    ['facebook.com', 'fb.com', 'instagram.com', 'twitter.com'])
                if not is_social:
                    targets.append(idx)
        elif cohort == 'B':
            if not _is_empty(lead.contact_name) and lead.confidence_score:
                try:
                    conf = float(lead.confidence_score)
                    if conf <= 3:
                        targets.append(idx)
                except (ValueError, TypeError):
                    pass
        elif cohort == 'C':
            if not _is_empty(lead.email) and lead.email_guessed == 'true':
                targets.append(idx)

    return targets


def should_mark_final(lead):
    attempts = 0
    try:
        attempts = int(lead.enrichment_attempts) if lead.enrichment_attempts and lead.enrichment_attempts.isdigit() else 0
    except (ValueError, TypeError):
        attempts = 0

    if attempts < MAX_ATTEMPTS_BEFORE_FINAL:
        return False

    no_website = not lead.website
    social_only = lead.website and any(d in lead.website.lower() for d in
        ['facebook.com', 'fb.com', 'instagram.com'])
    no_company_number = not getattr(lead, 'company_number', None)
    no_name = _is_empty(lead.contact_name)

    if (no_website or social_only) and no_name:
        return True
    if no_name and attempts >= MAX_ATTEMPTS_BEFORE_FINAL:
        return True

    return False


def enrich_single_lead(enricher, lead, mode):
    enricher.reset_per_lead_counters()
    notes = []

    try:
        if mode == 'false_positive_cleanup':
            return run_false_positive_cleanup(enricher, lead, notes)
        elif mode == 'final_confirmation':
            return run_final_confirmation(enricher, lead, notes)
        else:
            enricher.enrich(lead, skip_if_complete=False)
            return lead, notes

    except RequestLimitExceeded:
        notes.append('request_limit_exceeded')
    except LeadTimeout:
        notes.append('timeout_exceeded')
    except Exception as e:
        notes.append(f'surgical_error:{str(e)[:60]}')

    return lead, notes


def run_false_positive_cleanup(enricher, lead, notes):
    if _is_empty(lead.contact_name):
        return lead, notes

    if not enricher.openai_api_key:
        actual_key = os.environ.get("OPENAI_API_KEY", "")
        if actual_key:
            enricher.openai_api_key = actual_key

    if not enricher.openai_api_key:
        notes.append('skipped:no_openai_key')
        return lead, notes

    if not enricher.cost_tracker.can_make_call():
        notes.append('skipped:openai_budget_exhausted')
        return lead, notes

    website_text = ""
    if lead.website:
        website_text = enricher._get_minimal_text(lead)

    prompt = f"""Analyze this contact name for a UK business lead.

Company: {lead.company_name}
Contact Name: {lead.contact_name}
Website: {lead.website or 'N/A'}
Sector: {lead.sector or 'N/A'}

Website text snippet: {website_text[:1500] if website_text else 'N/A'}

Is "{lead.contact_name}" a real person's name or a false positive (e.g., a business term, 
UI element, street name, medical term, or page fragment)?

Reply with EXACTLY one line:
REAL: [reason] 
or
FALSE: [what it actually is]"""

    try:
        from openai import OpenAI
        client = OpenAI(api_key=enricher.openai_api_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You classify whether extracted text is a real person name or a false positive. Be strict."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=100,
            temperature=0
        )
        enricher.cost_tracker.record_call(response.usage.total_tokens if response.usage else 200)
        answer = response.choices[0].message.content.strip()
        notes.append(f'fp_check:{answer[:80]}')

        if answer.upper().startswith('FALSE'):
            old_name = lead.contact_name
            lead.contact_name = ""
            lead.contact_verified = ""
            lead.contact_source = ""
            lead.name_review_needed = ""
            notes.append(f'false_positive_removed:{old_name}')
            print(f"    [FP Cleanup] Removed false positive: {old_name} -> {answer}")
        else:
            lead.contact_verified = "true"
            notes.append('name_confirmed_real')
            print(f"    [FP Cleanup] Confirmed real: {lead.contact_name}")

    except Exception as e:
        notes.append(f'fp_check_error:{str(e)[:60]}')

    return lead, notes


def run_final_confirmation(enricher, lead, notes):
    if _is_empty(lead.contact_name):
        return lead, notes

    if not enricher.openai_api_key:
        actual_key = os.environ.get("OPENAI_API_KEY", "")
        if actual_key:
            enricher.openai_api_key = actual_key

    if not enricher.openai_api_key or not enricher.cost_tracker.can_make_call():
        notes.append('skipped:no_openai_or_budget')
        return lead, notes

    prompt = f"""For this UK wellness/health business, confirm the contact details are correct.

Company: {lead.company_name}
Contact Name: {lead.contact_name}
Role/Title: {lead.contact_titles or 'Unknown'}
Email: {lead.email or 'None'}
Website: {lead.website or 'N/A'}
Confidence Score: {lead.confidence_score}

Is this contact likely the right person to approach about office space?
Reply: YES/NO and a one-sentence reason."""

    try:
        from openai import OpenAI
        client = OpenAI(api_key=enricher.openai_api_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You confirm business lead contact accuracy. Be concise."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=100,
            temperature=0
        )
        enricher.cost_tracker.record_call(response.usage.total_tokens if response.usage else 150)
        answer = response.choices[0].message.content.strip()
        notes.append(f'confirmation:{answer[:80]}')
        print(f"    [Confirm] {lead.contact_name}: {answer[:60]}")

    except Exception as e:
        notes.append(f'confirmation_error:{str(e)[:60]}')

    return lead, notes


def run_with_timeout(func, args_tuple, timeout_sec):
    container = {'result': None, 'error': None}

    def target():
        try:
            container['result'] = func(*args_tuple)
        except Exception as e:
            container['error'] = e

    thread = threading.Thread(target=target)
    thread.daemon = True
    thread.start()
    thread.join(timeout=timeout_sec)

    if thread.is_alive():
        raise LeadTimeout(f"Lead processing exceeded {timeout_sec}s timeout")

    if container['error'] is not None:
        raise container['error']

    return container['result']


def print_stats(leads):
    total = len(leads)
    has_contact = sum(1 for l in leads if not _is_empty(l.contact_name))
    missing_contact = total - has_contact
    has_email = sum(1 for l in leads if not _is_empty(l.email))
    marked_final = sum(1 for l in leads if l.missing_name_final == 'true')

    conf_scores = []
    for l in leads:
        try:
            if l.confidence_score:
                conf_scores.append(float(l.confidence_score))
        except (ValueError, TypeError):
            pass

    low_conf = sum(1 for c in conf_scores if c <= 3)
    guessed_email = sum(1 for l in leads if l.email_guessed == 'true')

    print()
    print("=" * 60)
    print("DATASET STATISTICS")
    print("=" * 60)
    print(f"  Total leads:           {total}")
    print(f"  With contact_name:     {has_contact} ({has_contact/total*100:.1f}%)")
    print(f"  Missing contact_name:  {missing_contact} ({missing_contact/total*100:.1f}%)")
    print(f"  With email:            {has_email} ({has_email/total*100:.1f}%)")
    print(f"  Guessed emails:        {guessed_email}")
    print(f"  Low confidence (<=3):  {low_conf}")
    print(f"  Marked final:          {marked_final}")
    print()
    print("  Cohort sizes (excluding already-enriched-today and marked-final):")
    a = len(select_cohort(leads, 'A'))
    b = len(select_cohort(leads, 'B'))
    c = len(select_cohort(leads, 'C'))
    print(f"    Cohort A (missing contact):  {a}")
    print(f"    Cohort B (low confidence):   {b}")
    print(f"    Cohort C (unverified email): {c}")
    print()


def main():
    parser = argparse.ArgumentParser(description='Surgical lead enrichment')
    parser.add_argument('--cohort', choices=['A', 'B', 'C'],
                        help='Cohort to process: A=missing contact, B=low confidence, C=unverified email')
    parser.add_argument('--mode', choices=['contact_recovery', 'false_positive_cleanup',
                                           'email_verification', 'final_confirmation'],
                        help='Single-tool run mode')
    parser.add_argument('--limit', type=int, default=0, help='Max leads to process (0=all)')
    parser.add_argument('--force', action='store_true', help='Process even if enriched today')
    parser.add_argument('--input', default=INPUT_FILE, help='Input CSV file')
    parser.add_argument('--output', default=OUTPUT_FILE, help='Output CSV file')
    parser.add_argument('--stats', action='store_true', help='Show dataset stats and exit')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be processed without running')
    args = parser.parse_args()

    leads = load_leads_from_csv(args.input)
    print(f"Loaded {len(leads)} leads from {args.input}")

    if args.stats:
        print_stats(leads)
        return

    if not args.cohort or not args.mode:
        print("ERROR: --cohort and --mode are required (or use --stats)")
        parser.print_help()
        return

    targets = select_cohort(leads, args.cohort, force=args.force)
    if args.limit > 0:
        targets = targets[:args.limit]

    print()
    print("=" * 60)
    print(f"SURGICAL ENRICHMENT")
    print(f"  Cohort:  {args.cohort} ({'missing contact' if args.cohort == 'A' else 'low confidence' if args.cohort == 'B' else 'unverified email'})")
    print(f"  Mode:    {args.mode}")
    print(f"  Targets: {len(targets)} leads")
    print(f"  Force:   {args.force}")
    print(f"  Limits:  {MAX_PAGES_PER_LEAD} pages, {MAX_HTTP_REQUESTS_PER_LEAD} requests, {HARD_TIMEOUT_SECONDS}s timeout")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    if args.dry_run:
        print("\n[DRY RUN] Would process these leads:")
        for i, idx in enumerate(targets[:20]):
            lead = leads[idx]
            attempts = lead.enrichment_attempts or '0'
            conf = lead.confidence_score or 'N/A'
            print(f"  {i+1}. {lead.company_name[:45]} | attempts={attempts} | conf={conf} | contact={lead.contact_name or '(none)'}")
        if len(targets) > 20:
            print(f"  ... and {len(targets)-20} more")
        return

    if len(targets) == 0:
        print("\nNo leads match this cohort. Nothing to do.")
        print_stats(leads)
        return

    enricher = SurgicalEnricher(mode=args.mode)
    stats = {
        'processed': 0, 'gained_contact': 0, 'lost_contact': 0,
        'gained_email': 0, 'errors': 0, 'timeouts': 0,
        'request_limits': 0, 'marked_final': 0, 'fp_removed': 0,
        'confirmed_real': 0
    }

    start_time = time.time()

    for count, idx in enumerate(targets):
        lead = leads[idx]

        current_attempts = int(lead.enrichment_attempts) if lead.enrichment_attempts and lead.enrichment_attempts.isdigit() else 0
        lead.enrichment_attempts = str(current_attempts + 1)
        lead.last_enriched_date = str(date.today())

        old_contact = lead.contact_name
        old_email = lead.email
        stats['processed'] += 1

        try:
            result = run_with_timeout(
                enrich_single_lead,
                (enricher, lead, args.mode),
                HARD_TIMEOUT_SECONDS
            )
            lead_result, extra_notes = result

            if extra_notes:
                existing = lead.refinement_notes or ""
                new = "; ".join(extra_notes)
                lead.refinement_notes = f"{existing}; {new}".strip("; ") if existing else new

                if any('timeout' in n for n in extra_notes):
                    stats['timeouts'] += 1
                if any('request_limit' in n for n in extra_notes):
                    stats['request_limits'] += 1
                if any('false_positive_removed' in n for n in extra_notes):
                    stats['fp_removed'] += 1
                    stats['lost_contact'] += 1
                if any('name_confirmed_real' in n for n in extra_notes):
                    stats['confirmed_real'] += 1

        except LeadTimeout:
            stats['timeouts'] += 1
            existing = lead.refinement_notes or ""
            lead.refinement_notes = f"{existing}; timeout_exceeded".strip("; ")
            print(f"    [TIMEOUT] {lead.company_name} exceeded {HARD_TIMEOUT_SECONDS}s")

        except Exception as e:
            stats['errors'] += 1
            print(f"    [ERROR] {lead.company_name}: {e}")

        if not _is_empty(lead.contact_name) and _is_empty(old_contact):
            stats['gained_contact'] += 1
        if not _is_empty(lead.email) and _is_empty(old_email):
            stats['gained_email'] += 1

        if args.cohort == 'A' and _is_empty(lead.contact_name) and should_mark_final(lead):
            lead.missing_name_final = 'true'
            lead.enrichment_status = 'missing_name_final'
            stats['marked_final'] += 1
            existing = lead.refinement_notes or ""
            lead.refinement_notes = f"{existing}; marked_missing_name_final".strip("; ")
            print(f"    [FINAL] {lead.company_name} marked as permanently unreachable")

        save_leads_to_csv(leads, args.output, mode='w')

        if (count + 1) % 10 == 0 or count == len(targets) - 1:
            elapsed = time.time() - start_time
            rate = stats['processed'] / elapsed * 60 if elapsed > 0 else 0
            print(f"\n  --- Progress: {stats['processed']}/{len(targets)} ({rate:.0f}/min) "
                  f"| +contact: {stats['gained_contact']} | +email: {stats['gained_email']} "
                  f"| final: {stats['marked_final']} | err: {stats['errors']} "
                  f"| timeout: {stats['timeouts']} ---\n")

    elapsed = time.time() - start_time
    print()
    print("=" * 60)
    print("SURGICAL ENRICHMENT COMPLETE")
    print("=" * 60)
    print(f"  Duration:        {elapsed/60:.1f} minutes ({elapsed:.0f}s)")
    print(f"  Processed:       {stats['processed']}")
    print(f"  Gained contact:  {stats['gained_contact']}")
    print(f"  Lost contact:    {stats['lost_contact']} (false positives removed)")
    print(f"  Gained email:    {stats['gained_email']}")
    print(f"  Marked final:    {stats['marked_final']}")
    if args.mode == 'false_positive_cleanup':
        print(f"  FP removed:      {stats['fp_removed']}")
        print(f"  Confirmed real:  {stats['confirmed_real']}")
    print(f"  Timeouts:        {stats['timeouts']}")
    print(f"  Request limits:  {stats['request_limits']}")
    print(f"  Errors:          {stats['errors']}")
    print(f"  Saved to:        {args.output}")

    log_path = 'surgical_enrichment_log.txt'
    with open(log_path, 'a') as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Cohort: {args.cohort} | Mode: {args.mode}\n")
        f.write(f"Duration: {elapsed/60:.1f}min | Processed: {stats['processed']}\n")
        for k, v in stats.items():
            f.write(f"  {k}: {v}\n")
    print(f"  Log appended to: {log_path}")

    print_stats(leads)


if __name__ == '__main__':
    main()
