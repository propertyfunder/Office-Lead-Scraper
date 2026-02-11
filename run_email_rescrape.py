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
HARD_TIMEOUT_SECONDS = 30
BATCH_SIZE = 5

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
        ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
        self.session.headers.update({
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-GB,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
        })

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


def is_qualifying(lead, retry_previous=False):
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

    previously_done = any(marker in notes for marker in [
        'email_rescrape_done', 'email_rescrape_no_email',
        'email_rescrape_timeout', 'email_rescrape_error',
        'website_email_not_found', 'rescrape_skipped'
    ])

    already_replaced = 'email_replaced_from_website' in notes

    if already_replaced:
        return False, 'already_replaced'

    if previously_done and not retry_previous:
        return False, 'already_rescrape_done'

    if previously_done and retry_previous:
        if 'rescrape_v4' in notes:
            return False, 'already_rescrape_v4'
        if not email or email == 'nan' or email.strip() == '':
            return True, 'retry_no_email'
        if email_guessed or email_type == 'guessed':
            return True, 'retry_guessed'
        if has_bad_pattern(email):
            return True, 'retry_bad_pattern'
        return False, 'retry_skip_has_email'

    if not email or email == 'nan' or email.strip() == '':
        return True, 'no_email'

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


FREE_EMAIL_PROVIDERS = {
    'gmail.com', 'googlemail.com', 'hotmail.com', 'hotmail.co.uk',
    'outlook.com', 'live.com', 'live.co.uk', 'yahoo.com', 'yahoo.co.uk',
    'icloud.com', 'me.com', 'aol.com', 'protonmail.com', 'proton.me',
    'btinternet.com', 'sky.com', 'virginmedia.com', 'talktalk.net',
    'mail.com', 'msn.com', 'zoho.com',
}


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
        if email_domain in FREE_EMAIL_PROVIDERS:
            return True
        site_core = site_domain.split('.')[0].replace('-', '').replace('_', '')
        email_core = email_domain.split('.')[0].replace('-', '').replace('_', '')
        if len(site_core) >= 4 and len(email_core) >= 4:
            if site_core in email_core or email_core in site_core:
                return True
        generic_stems = {
            'clinic', 'dental', 'physio', 'health', 'therapy', 'therapist',
            'medical', 'surgery', 'practice', 'centre', 'center', 'studio',
            'wellness', 'massage', 'beauty', 'guildford', 'surrey', 'farnham',
            'godalming', 'woking', 'group', 'online', 'london', 'hampshire',
        }

        def strip_generic(core):
            result = core
            for stem in sorted(generic_stems, key=len, reverse=True):
                result = result.replace(stem, '')
            return result

        site_brand = strip_generic(site_core)
        email_brand = strip_generic(email_core)
        if site_brand and email_brand and len(site_brand) >= 4 and len(email_brand) >= 4:
            if site_brand == email_brand or site_brand in email_brand or email_brand in site_brand:
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
    '/contact-me', '/get-in-touch', '/enquiry', '/enquiries',
    '/about', '/about-us', '/about_us', '/aboutus',
    '/team', '/our-team', '/the-team', '/meet-the-team',
]

CONTACT_LINK_KEYWORDS = [
    'contact', 'get in touch', 'reach us', 'enquir', 'email us',
    'fees', 'book', 'appointment',
]


def decode_cloudflare_email(encoded_str):
    try:
        enc = encoded_str.strip()
        r = int(enc[:2], 16)
        decoded = ''
        for i in range(2, len(enc), 2):
            decoded += chr(int(enc[i:i+2], 16) ^ r)
        if '@' in decoded:
            return decoded.lower().strip()
    except Exception:
        pass
    return None


def extract_emails_from_html(html_text, site_domain):
    if not html_text:
        return []
    emails = set()
    for match in EMAIL_RE.findall(html_text):
        cleaned = clean_email(match.lower().strip())
        if not cleaned or '@' not in cleaned:
            continue
        local, domain = cleaned.split('@', 1)
        if len(local) > 64 or len(cleaned) > 254:
            continue
        if local in JUNK_EMAIL_LOCALS:
            continue
        if cleaned.endswith('.png') or cleaned.endswith('.jpg') or cleaned.endswith('.gif'):
            continue
        if 'sentry.' in cleaned or 'wixpress.' in cleaned:
            continue
        emails.add(cleaned)
    return list(emails)


def extract_emails_from_soup(soup):
    emails = set()
    if not soup:
        return emails
    for mailto in soup.select('a[href^="mailto:"]'):
        href = mailto.get('href', '')
        email_part = href.replace('mailto:', '').split('?')[0].strip()
        if email_part and '@' in email_part:
            cleaned = clean_email(email_part.lower())
            if cleaned:
                emails.add(cleaned)
    for cf_link in soup.select('a[href*="/cdn-cgi/l/email-protection"]'):
        data = cf_link.get('data-cfemail', '')
        if data:
            decoded = decode_cloudflare_email(data)
            if decoded:
                emails.add(decoded)
    for cf_span in soup.select('[data-cfemail]'):
        data = cf_span.get('data-cfemail', '')
        if data:
            decoded = decode_cloudflare_email(data)
            if decoded:
                emails.add(decoded)
    cf_pattern = re.compile(r'/cdn-cgi/l/email-protection#([a-f0-9]+)', re.I)
    for a_tag in soup.find_all('a', href=True):
        m = cf_pattern.search(a_tag['href'])
        if m:
            decoded = decode_cloudflare_email(m.group(1))
            if decoded:
                emails.add(decoded)
    return emails


def extract_schema_emails(soup):
    import json
    emails = set()
    if not soup:
        return emails
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string or '{}')
            items = data if isinstance(data, list) else [data]
            for item in items:
                if isinstance(item, dict):
                    for key in ['email', 'contactPoint']:
                        val = item.get(key, '')
                        if isinstance(val, str) and '@' in val:
                            cleaned = clean_email(val.replace('mailto:', '').lower())
                            if cleaned:
                                emails.add(cleaned)
                        elif isinstance(val, dict) and '@' in str(val.get('email', '')):
                            cleaned = clean_email(str(val['email']).lower())
                            if cleaned:
                                emails.add(cleaned)
                        elif isinstance(val, list):
                            for v in val:
                                if isinstance(v, dict) and '@' in str(v.get('email', '')):
                                    cleaned = clean_email(str(v['email']).lower())
                                    if cleaned:
                                        emails.add(cleaned)
                    graph = item.get('@graph', [])
                    if isinstance(graph, list):
                        for node in graph:
                            if isinstance(node, dict):
                                for key in ['email', 'contactPoint']:
                                    val = node.get(key, '')
                                    if isinstance(val, str) and '@' in val:
                                        cleaned = clean_email(val.replace('mailto:', '').lower())
                                        if cleaned:
                                            emails.add(cleaned)
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass
    return emails


def extract_footer_emails(soup, site_domain):
    emails = set()
    if not soup:
        return emails
    footer_selectors = ['footer', '[class*="footer"]', '[id*="footer"]',
                        '[class*="contact"]', '[id*="contact"]',
                        '[class*="details"]', '[id*="details"]']
    for selector in footer_selectors:
        for el in soup.select(selector):
            text = el.get_text(' ', strip=True)
            for match in EMAIL_RE.findall(text):
                cleaned = clean_email(match.lower().strip())
                if cleaned and cleaned.split('@')[0] not in JUNK_EMAIL_LOCALS:
                    emails.add(cleaned)
            emails.update(extract_emails_from_soup(el))
    return emails


def discover_contact_links(soup, base_url):
    from urllib.parse import urljoin
    discovered = []
    if not soup:
        return discovered
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href'].lower()
        text = a_tag.get_text(strip=True).lower()
        combined = href + ' ' + text
        if any(kw in combined for kw in CONTACT_LINK_KEYWORDS):
            full_url = urljoin(base_url, a_tag['href'])
            if full_url not in discovered:
                discovered.append(full_url)
    return discovered[:6]


def fetch_page(session, url, timeout=6):
    try:
        resp = session.get(url, timeout=timeout, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
    except Exception:
        pass
    return None


def scrape_page_for_emails(session, url, site_domain, timeout=5):
    from bs4 import BeautifulSoup
    html = fetch_page(session, url, timeout=timeout)
    if not html or len(html) < 200:
        return set(), False
    emails = set()
    emails.update(extract_emails_from_html(html, site_domain))
    try:
        soup = BeautifulSoup(html, 'lxml')
        emails.update(extract_emails_from_soup(soup))
        emails.update(extract_footer_emails(soup, site_domain))
        emails.update(extract_schema_emails(soup))
    except Exception:
        pass
    return emails, True


def scrape_website_for_email(enricher, lead):
    from urllib.parse import urljoin, urlparse
    from bs4 import BeautifulSoup

    notes = []
    if not lead.website or not lead.website.strip():
        notes.append('rescrape_skipped:no_website')
        return None, None, notes

    website = lead.website.strip().rstrip('/')
    site_domain = extract_domain(website)
    if not site_domain:
        notes.append('rescrape_skipped:no_domain')
        return None, None, notes

    session = enricher.session
    generic_prefixes = set(enricher.generic_email_prefixes)

    all_emails = set()
    pages_checked = 0
    max_pages = 5
    checked_urls = set()

    homepage_html = fetch_page(session, website)
    pages_checked += 1
    checked_urls.add(website.lower())

    discovered_links = []

    if homepage_html:
        all_emails.update(extract_emails_from_html(homepage_html, site_domain))
        try:
            soup = BeautifulSoup(homepage_html, 'lxml')
            all_emails.update(extract_emails_from_soup(soup))
            all_emails.update(extract_footer_emails(soup, site_domain))
            all_emails.update(extract_schema_emails(soup))
            discovered_links = discover_contact_links(soup, website)
        except Exception:
            pass

    matched_so_far = [e for e in all_emails if domain_matches(e, website)]
    has_personal = any(e.split('@')[0] not in generic_prefixes for e in matched_so_far)

    if not has_personal:
        parsed = urlparse(website)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        contact_urls = []
        for path in ['/contact', '/contact-us', '/contact_us', '/contactus',
                      '/contact-me', '/get-in-touch', '/enquiry', '/enquiries']:
            url = base_url + path
            if url.lower() not in checked_urls:
                contact_urls.append(url)

        for link in discovered_links:
            if link.lower() not in checked_urls and link not in contact_urls:
                contact_urls.append(link)

        other_urls = []
        for path in ['/about', '/about-us', '/about_us', '/aboutus',
                      '/team', '/our-team', '/the-team', '/meet-the-team']:
            url = base_url + path
            if url.lower() not in checked_urls and url not in contact_urls:
                other_urls.append(url)

        for page_url in contact_urls:
            if pages_checked >= max_pages:
                break
            if page_url.lower() in checked_urls:
                continue
            checked_urls.add(page_url.lower())
            page_emails, was_valid = scrape_page_for_emails(session, page_url, site_domain, timeout=5)
            if was_valid:
                pages_checked += 1
                all_emails.update(page_emails)
                new_matched = [e for e in page_emails if domain_matches(e, website)]
                if any(e.split('@')[0] not in generic_prefixes for e in new_matched):
                    break

        if not any(e.split('@')[0] not in generic_prefixes
                   for e in all_emails if domain_matches(e, website)):
            for page_url in other_urls:
                if pages_checked >= max_pages:
                    break
                if page_url.lower() in checked_urls:
                    continue
                checked_urls.add(page_url.lower())
                page_emails, was_valid = scrape_page_for_emails(session, page_url, site_domain, timeout=5)
                if was_valid:
                    pages_checked += 1
                    all_emails.update(page_emails)

    domain_emails = [e for e in all_emails if domain_matches(e, website)]

    personal_candidates = []
    generic_candidates = []

    for email in domain_emails:
        local = email.split('@')[0]
        if local not in generic_prefixes:
            personal_candidates.append(email)
        else:
            generic_candidates.append(email)

    personal = None
    generic = None

    exact_domain = site_domain.lower()
    exact_personal = [e for e in personal_candidates
                      if e.split('@')[1].lower() == exact_domain]
    other_domain_personal = [e for e in personal_candidates
                             if e.split('@')[1].lower() != exact_domain
                             and e.split('@')[1].lower() not in FREE_EMAIL_PROVIDERS]
    free_personal = [e for e in personal_candidates
                     if e.split('@')[1].lower() in FREE_EMAIL_PROVIDERS]

    if exact_personal:
        personal = exact_personal[0]
    elif other_domain_personal:
        personal = other_domain_personal[0]
    elif free_personal:
        personal = free_personal[0]

    exact_generic = [e for e in generic_candidates
                     if e.split('@')[1].lower() == exact_domain]
    if exact_generic:
        generic = exact_generic[0]
    elif generic_candidates:
        generic = generic_candidates[0]

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


def analyse_leads(leads, retry_previous=False):
    qualifying = []
    for idx, lead in enumerate(leads):
        qualifies, reason = is_qualifying(lead, retry_previous=retry_previous)
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
    parser.add_argument('--retry-previous', action='store_true',
                        help='Retry leads that were previously scraped but still have guessed/missing emails')
    args = parser.parse_args()

    if not any([args.stats, args.dry_run, args.run]):
        parser.print_help()
        return

    leads = load_leads_from_csv(INPUT_FILE)
    print(f"Loaded {len(leads)} leads from {INPUT_FILE}")

    qualifying = analyse_leads(leads, retry_previous=args.retry_previous)

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

        is_retry = reason.startswith('retry_')

        if timeout_notes and 'timeout_exceeded' in timeout_notes:
            stats['timeouts'] += 1
            existing_notes = lead.refinement_notes or ''
            marker = 'rescrape_v4_timeout' if is_retry else 'email_rescrape_timeout'
            lead.refinement_notes = f"{existing_notes}; {marker}".strip('; ')
            print(f"    TIMEOUT")
        elif result_tuple is not None:
            personal, generic, scrape_notes = result_tuple
            existing_notes = lead.refinement_notes or ''

            old_is_guessed = (
                str(getattr(lead, 'email_guessed', '')).lower() == 'true'
                or str(getattr(lead, 'email_type', '')).lower() == 'guessed'
                or has_bad_pattern(old_email)
                or not old_email or old_email == 'nan'
            )

            if personal and personal != old_email:
                if lead.email and str(lead.email) != 'nan':
                    existing_guesses = lead.personal_email_guesses or ''
                    if lead.email not in str(existing_guesses):
                        lead.personal_email_guesses = f"{existing_guesses}; {lead.email}".strip('; ')

                lead.email = personal
                lead.email_guessed = "false"
                lead.email_type = "personal"
                v2_tag = '; rescrape_v4' if is_retry else ''
                lead.refinement_notes = f"{existing_notes}; email_replaced_from_website:{old_email}->{personal}{v2_tag}".strip('; ')
                stats['replaced_personal'] += 1
                print(f"    REPLACED (personal): {personal}")

            elif generic and generic != old_email:
                if old_is_guessed:
                    if lead.email and str(lead.email) != 'nan' and lead.email != generic:
                        existing_guesses = lead.personal_email_guesses or ''
                        if lead.email not in str(existing_guesses):
                            lead.personal_email_guesses = f"{existing_guesses}; {lead.email}".strip('; ')

                    lead.email = generic
                    lead.email_guessed = "false"
                    lead.generic_email = generic
                    lead.email_type = "generic"
                    v2_tag = '; rescrape_v4' if is_retry else ''
                    lead.refinement_notes = f"{existing_notes}; email_replaced_from_website:{old_email}->{generic}{v2_tag}".strip('; ')
                    stats['replaced_generic'] += 1
                    print(f"    REPLACED (generic): {generic}")
                else:
                    lead.generic_email = generic
                    v2_tag = '; rescrape_v4' if is_retry else ''
                    lead.refinement_notes = f"{existing_notes}; generic_email_found:{generic}; email_rescrape_done{v2_tag}".strip('; ')
                    stats['already_has_better'] += 1
                    print(f"    KEPT existing (generic found: {generic})")
            else:
                v2_tag = '; rescrape_v4' if is_retry else ''
                lead.refinement_notes = f"{existing_notes}; website_email_not_found; email_rescrape_done{v2_tag}".strip('; ')
                stats['no_change'] += 1
                print(f"    NO CHANGE (no email found on website)")

            if scrape_notes:
                lead.refinement_notes = f"{lead.refinement_notes}; {'; '.join(scrape_notes)}".strip('; ')
        else:
            stats['errors'] += 1
            existing_notes = lead.refinement_notes or ''
            v2_tag = '; rescrape_v4' if is_retry else ''
            lead.refinement_notes = f"{existing_notes}; email_rescrape_error{v2_tag}".strip('; ')
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
