#!/usr/bin/env python3
"""
Pre-sanitisation email re-scrape — replaces guessed or weak generic emails
with real on-page emails found via website scraping.

Usage:
    python run_email_rescrape.py --stats
    python run_email_rescrape.py --dry-run
    python run_email_rescrape.py --run [--limit N] [--batch-size N]
"""

import sys
import os
import re
import argparse
import time
import threading
from datetime import datetime

sys.path.insert(0, '.')
from main import load_leads_from_csv
from src.enricher import LeadEnricher
from src.utils import (save_leads_to_csv, extract_domain, clean_email,
                       get_all_fieldnames)
from src.models import BusinessLead

INPUT_FILE = 'unit8_leads_enriched.csv'
OUTPUT_FILE = 'unit8_leads_enriched.csv'
HARD_TIMEOUT_SECONDS = 15
BATCH_SIZE = 10

BAD_EMAIL_PATTERNS = [
    'account.suspended@',
    'shopping.cart@',
    'business.software@',
    'subscribe.subscribed@',
    'experience.friendlyand@',
    'extended.hope@',
    'rapid.transformational@',
    'best.body@',
    'spiritual.coaching@',
    'routine.nail@',
    'dock.no@',
    'personalized.approach@',
    'vibrant.world@',
    'learning.space@',
    'let.chat@',
    'who.we@',
    'medical.assurance@',
]

SOCIAL_DOMAINS = ['facebook.com', 'instagram.com', 'twitter.com',
                  'linkedin.com', 'youtube.com', 'tiktok.com']


class RescrapeEnricher(LeadEnricher):
    def __init__(self):
        super().__init__()
        self.google_api_key = ""
        self.companies_house_api_key = ""
        self.openai_api_key = ""
        import requests
        self.session = requests.Session()
        try:
            from fake_useragent import UserAgent
            self.session.headers['User-Agent'] = UserAgent().random
        except Exception:
            self.session.headers['User-Agent'] = (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

    def _enrich_from_companies_house(self, lead):
        return {}

    def _enrich_from_linkedin(self, lead):
        return {}

    def _score_lead_with_ai(self, lead):
        return {}


def has_bad_pattern(email):
    if not email or str(email).lower() == 'nan':
        return False
    email_lower = str(email).lower()
    return any(bp in email_lower for bp in BAD_EMAIL_PATTERNS)


def email_is_in_guesses(email, personal_guesses):
    if not email or not personal_guesses:
        return False
    email_lower = str(email).strip().lower()
    guesses_lower = str(personal_guesses).lower()
    if email_lower == 'nan' or guesses_lower == 'nan':
        return False
    return email_lower in guesses_lower


def is_qualifying(lead):
    website = (lead.website or '').strip()
    if not website or website == 'nan':
        return False, 'no_website'

    if any(d in website.lower() for d in SOCIAL_DOMAINS):
        return False, 'social_only'

    email = (lead.email or '').strip()
    email_guessed = str(getattr(lead, 'email_guessed', '')).lower() == 'true'
    email_type = str(getattr(lead, 'email_type', '')).lower()
    personal_guesses = getattr(lead, 'personal_email_guesses', '') or ''

    notes = (lead.refinement_notes or '').lower()
    if any(marker in notes for marker in [
        'email_replaced_from_website', 'email_rescrape_done',
        'email_rescrape_no_email', 'email_rescrape_timeout',
        'email_rescrape_error', 'website_email_not_found',
        'pages_checked:', 'emails_found:', 'rescrape_skipped'
    ]):
        return False, 'already_rescrape_done'

    if email_guessed:
        return True, 'email_guessed_true'

    if email_type == 'guessed':
        return True, 'email_type_guessed'

    if has_bad_pattern(email):
        return True, 'bad_pattern'

    if email and email != 'nan' and email_is_in_guesses(email, personal_guesses):
        return True, 'email_in_personal_guesses'

    if email_type == 'both':
        return True, 'email_type_both'

    return False, 'not_qualifying'


def domain_matches(email, website_url):
    if not email or not website_url:
        return False
    try:
        email_domain = email.split('@')[1].lower().strip()
        site_domain = extract_domain(website_url).lower().strip()
        if email_domain == site_domain:
            return True
        email_parts = email_domain.split('.')
        site_parts = site_domain.split('.')
        if len(email_parts) >= 2 and len(site_parts) >= 2:
            if email_parts[-2:] == site_parts[-2:]:
                return True
        return False
    except Exception:
        return False


def is_personal_email(email, generic_prefixes):
    if not email or '@' not in email:
        return False
    local = email.split('@')[0].lower()
    return local not in generic_prefixes


EMAIL_RE = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
    re.IGNORECASE
)

JUNK_EMAIL_LOCALS = {
    'example', 'test', 'noreply', 'no-reply', 'donotreply',
    'mailer-daemon', 'postmaster', 'webmaster', 'sentry',
    'wix', 'wordpress', 'squarespace',
}

CONTACT_PATHS = [
    '/contact', '/contact-us', '/contact_us', '/contactus',
    '/about', '/about-us', '/about_us', '/aboutus',
    '/team', '/our-team', '/the-team', '/meet-the-team',
    '/get-in-touch',
]


def extract_emails_from_html(html_text, site_domain):
    if not html_text:
        return []
    emails = set()
    for match in EMAIL_RE.findall(html_text):
        cleaned = clean_email(match.lower().strip())
        if not cleaned or '@' not in cleaned:
            continue
        local = cleaned.split('@')[0]
        if local in JUNK_EMAIL_LOCALS:
            continue
        if cleaned.endswith('.png') or cleaned.endswith('.jpg') or cleaned.endswith('.gif'):
            continue
        if 'sentry.' in cleaned or 'wixpress.' in cleaned:
            continue
        emails.add(cleaned)
    return list(emails)


def fetch_page(session, url, timeout=6):
    try:
        resp = session.get(url, timeout=timeout, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
    except Exception:
        pass
    return None


def scrape_website_for_email(enricher, lead):
    notes = []
    if not lead.website or not lead.website.strip():
        notes.append('rescrape_skipped:no_website')
        return None, None, notes

    from urllib.parse import urljoin, urlparse
    import requests

    website = lead.website.strip().rstrip('/')
    site_domain = extract_domain(website)
    if not site_domain:
        notes.append('rescrape_skipped:no_domain')
        return None, None, notes

    session = enricher.session
    generic_prefixes = set(enricher.generic_email_prefixes)

    all_emails = []

    homepage_html = fetch_page(session, website)
    if homepage_html:
        all_emails.extend(extract_emails_from_html(homepage_html, site_domain))

        from bs4 import BeautifulSoup
        try:
            soup = BeautifulSoup(homepage_html, 'lxml')
            for mailto in soup.select('a[href^="mailto:"]'):
                href = mailto.get('href', '')
                email_part = href.replace('mailto:', '').split('?')[0].strip()
                if email_part and '@' in email_part:
                    cleaned = clean_email(email_part.lower())
                    if cleaned:
                        all_emails.append(cleaned)
        except Exception:
            pass

    parsed = urlparse(website)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    pages_checked = 1
    max_pages = 4

    for path in CONTACT_PATHS:
        if pages_checked >= max_pages:
            break
        page_url = base_url + path
        html = fetch_page(session, page_url, timeout=5)
        if html and len(html) > 500:
            pages_checked += 1
            page_emails = extract_emails_from_html(html, site_domain)
            all_emails.extend(page_emails)

            try:
                soup = BeautifulSoup(html, 'lxml')
                for mailto in soup.select('a[href^="mailto:"]'):
                    href = mailto.get('href', '')
                    email_part = href.replace('mailto:', '').split('?')[0].strip()
                    if email_part and '@' in email_part:
                        cleaned = clean_email(email_part.lower())
                        if cleaned:
                            all_emails.append(cleaned)
            except Exception:
                pass

    domain_emails = [e for e in all_emails if domain_matches(e, website)]

    personal = None
    generic = None

    for email in domain_emails:
        local = email.split('@')[0]
        if local not in generic_prefixes:
            if not personal:
                personal = email
        else:
            if not generic:
                generic = email

    notes.append(f'pages_checked:{pages_checked}')
    notes.append(f'emails_found:{len(set(domain_emails))}')

    return personal, generic, notes


def run_with_timeout(func, args, timeout_sec):
    container = {'result': None, 'error': None}

    def target():
        try:
            container['result'] = func(*args)
        except Exception as e:
            container['error'] = e

    thread = threading.Thread(target=target)
    thread.daemon = True
    thread.start()
    thread.join(timeout=timeout_sec)

    if thread.is_alive():
        return None, ['timeout_exceeded']

    if container['error']:
        return None, [f'thread_error:{str(container["error"])[:60]}']

    return container['result'], []


def analyse_leads(leads):
    qualifying = []
    for idx, lead in enumerate(leads):
        qualifies, reason = is_qualifying(lead)
        if qualifies:
            qualifying.append((idx, reason))
    return qualifying


def main():
    parser = argparse.ArgumentParser(description='Pre-sanitisation email re-scrape')
    parser.add_argument('--stats', action='store_true', help='Show scope statistics')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be processed')
    parser.add_argument('--run', action='store_true', help='Execute the rescrape')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of leads to process')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE, help='Save every N records')
    args = parser.parse_args()

    if not any([args.stats, args.dry_run, args.run]):
        parser.print_help()
        return

    leads = load_leads_from_csv(INPUT_FILE)
    print(f"Loaded {len(leads)} leads from {INPUT_FILE}")

    qualifying = analyse_leads(leads)

    has_email = sum(1 for l in leads if (l.email or '').strip() and str(l.email) != 'nan')

    if args.stats or args.dry_run:
        print(f"\n{'='*60}")
        print("PRE-SANITISATION EMAIL RE-SCRAPE")
        print(f"{'='*60}")
        print(f"  Total leads:            {len(leads)}")
        print(f"  With email:             {has_email}")
        print(f"  Qualifying for rescrape: {len(qualifying)}")
        print()

        by_reason = {}
        for _, reason in qualifying:
            by_reason[reason] = by_reason.get(reason, 0) + 1
        for k, v in sorted(by_reason.items(), key=lambda x: -x[1]):
            print(f"    {k}: {v}")

    if args.dry_run:
        print(f"\n{'='*60}")
        print("DRY RUN — Qualifying leads")
        print(f"{'='*60}")

        for idx, reason in qualifying:
            lead = leads[idx]
            email = (lead.email or 'NO EMAIL')[:45]
            cn = (lead.company_name or '')[:45]
            website = (lead.website or '')[:40]
            print(f"  {idx:>5}. [{reason:<25}] {email}")
            print(f"         {cn}")
            print(f"         {website}")
        return

    if not args.run:
        return

    print(f"\n{'='*60}")
    print("EMAIL RE-SCRAPE — EXECUTING")
    print(f"{'='*60}")

    enricher = RescrapeEnricher()

    targets = qualifying
    if args.limit and args.limit < len(targets):
        targets = targets[:args.limit]

    stats = {
        'processed': 0,
        'replaced_personal': 0,
        'replaced_generic': 0,
        'no_change': 0,
        'errors': 0,
        'timeouts': 0,
        'already_has_better': 0,
    }
    start_time = time.time()

    print(f"\nProcessing {len(targets)} leads...\n")

    for count, (idx, reason) in enumerate(targets):
        lead = leads[idx]
        old_email = (lead.email or '').strip()
        print(f"  [{count+1}/{len(targets)}] {(lead.company_name or '')[:50]}")
        print(f"    Current: {old_email[:50]}  [{reason}]")

        result_tuple, timeout_notes = run_with_timeout(
            scrape_website_for_email, (enricher, lead), HARD_TIMEOUT_SECONDS
        )

        if timeout_notes and 'timeout_exceeded' in timeout_notes:
            stats['timeouts'] += 1
            existing_notes = lead.refinement_notes or ''
            lead.refinement_notes = f"{existing_notes}; email_rescrape_timeout".strip('; ')
            print(f"    TIMEOUT")
        elif result_tuple is not None:
            personal, generic, scrape_notes = result_tuple
            existing_notes = lead.refinement_notes or ''

            if personal and personal != old_email:
                if lead.email and str(lead.email) != 'nan':
                    existing_guesses = lead.personal_email_guesses or ''
                    if lead.email not in str(existing_guesses):
                        lead.personal_email_guesses = f"{existing_guesses}; {lead.email}".strip('; ')

                lead.email = personal
                lead.email_guessed = "false"
                lead.email_type = "personal"
                lead.refinement_notes = f"{existing_notes}; email_replaced_from_website:{old_email}->{personal}".strip('; ')
                stats['replaced_personal'] += 1
                print(f"    REPLACED (personal): {personal}")

            elif generic and generic != old_email:
                if has_bad_pattern(old_email) or not old_email or old_email == 'nan':
                    if lead.email and str(lead.email) != 'nan' and lead.email != generic:
                        existing_guesses = lead.personal_email_guesses or ''
                        if lead.email not in str(existing_guesses):
                            lead.personal_email_guesses = f"{existing_guesses}; {lead.email}".strip('; ')

                    lead.email = generic
                    lead.email_guessed = "false"
                    lead.generic_email = generic
                    lead.email_type = "generic"
                    lead.refinement_notes = f"{existing_notes}; email_replaced_from_website:{old_email}->{generic}".strip('; ')
                    stats['replaced_generic'] += 1
                    print(f"    REPLACED (generic): {generic}")
                else:
                    if generic:
                        lead.generic_email = generic
                    lead.refinement_notes = f"{existing_notes}; website_email_not_found; email_rescrape_done".strip('; ')
                    stats['already_has_better'] += 1
                    print(f"    KEPT existing (generic found but current not bad)")
            else:
                lead.refinement_notes = f"{existing_notes}; website_email_not_found; email_rescrape_done".strip('; ')
                stats['no_change'] += 1
                print(f"    NO CHANGE (no email found on website)")

            if scrape_notes:
                lead.refinement_notes = f"{lead.refinement_notes}; {'; '.join(scrape_notes)}".strip('; ')
        else:
            stats['errors'] += 1
            existing_notes = lead.refinement_notes or ''
            lead.refinement_notes = f"{existing_notes}; email_rescrape_error".strip('; ')
            print(f"    ERROR")

        stats['processed'] += 1

        if (count + 1) % args.batch_size == 0:
            save_leads_to_csv(leads, OUTPUT_FILE, 'w')
            print(f"  --- Checkpoint saved ({count+1}/{len(targets)}) ---")

    save_leads_to_csv(leads, OUTPUT_FILE, 'w')
    print(f"\nSaved {len(leads)} leads to {OUTPUT_FILE}")

    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print("EMAIL RE-SCRAPE COMPLETE")
    print(f"{'='*60}")
    print(f"  Duration:               {elapsed/60:.1f} minutes ({elapsed:.0f}s)")
    print(f"  Leads processed:        {stats['processed']}")
    print(f"  Emails replaced (personal): {stats['replaced_personal']}")
    print(f"  Emails replaced (generic):  {stats['replaced_generic']}")
    print(f"  No change:              {stats['no_change']}")
    print(f"  Already has better:     {stats['already_has_better']}")
    print(f"  Timeouts:               {stats['timeouts']}")
    print(f"  Errors:                 {stats['errors']}")
    print()
    total_replaced = stats['replaced_personal'] + stats['replaced_generic']
    print(f"  TOTAL REPLACED:         {total_replaced}/{stats['processed']}")


if __name__ == '__main__':
    main()
