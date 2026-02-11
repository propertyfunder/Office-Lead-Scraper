#!/usr/bin/env python3
import csv
import re
import os
import sys
import time
import signal
import argparse
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright

INPUT_FILE = 'unit8_leads_enriched.csv'
OUTPUT_FILE = 'unit8_leads_enriched.csv'
MAX_LEADS = 300
PAGE_TIMEOUT_MS = 15000
RENDER_WAIT_MS = 3000

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', re.IGNORECASE)

JUNK_EMAIL_LOCALS = {
    'example', 'test', 'noreply', 'no-reply', 'donotreply',
    'mailer-daemon', 'postmaster', 'webmaster', 'sentry',
    'wix', 'wordpress', 'squarespace',
}

GENERIC_PREFIXES = {
    'info', 'hello', 'enquiries', 'enquiry', 'contact', 'admin',
    'office', 'reception', 'support', 'team', 'bookings',
    'appointments', 'mail', 'general', 'sales',
}

BAD_EMAIL_PATTERNS = [
    'account.suspended@', 'shopping.cart@', 'business.software@',
    'subscribe.subscribed@', 'experience.friendlyand@', 'extended.hope@',
    'rapid.transformational@', 'best.body@', 'spiritual.coaching@',
    'routine.nail@', 'dock.no@', 'personalized.approach@',
    'vibrant.world@', 'learning.space@', 'let.chat@', 'who.we@',
    'medical.assurance@',
]


def clean_email(email):
    if not email:
        return None
    email = email.lower().strip()
    email = re.sub(r'[<>"\'\[\](){}]', '', email)
    m = re.search(r'[a-zA-Z][a-zA-Z0-9._%+-]*@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,7}\b', email, re.I)
    if not m:
        return None
    email = m.group(0)
    if '@' not in email:
        return None
    local, domain = email.split('@', 1)
    if len(local) > 64 or len(email) > 254:
        return None
    if local in JUNK_EMAIL_LOCALS:
        return None
    if any(bp in email for bp in BAD_EMAIL_PATTERNS):
        return None
    junk_domains = ['sentry', 'wixpress', 'godaddy', 'squarespace', 'wordpress',
                    'mailchimp', 'googleapis', 'gstatic', 'cloudflare']
    if any(j in domain for j in junk_domains):
        return None
    if email.endswith(('.png', '.jpg', '.gif', '.svg', '.css', '.js')):
        return None
    return email


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


def domain_matches(email, website):
    if not email or not website:
        return False
    try:
        email_domain = email.split('@')[1].lower()
        parsed = urlparse(website if '://' in website else f'https://{website}')
        site_host = (parsed.netloc or parsed.path).lower().replace('www.', '')
        email_domain_clean = email_domain.replace('www.', '')
        if email_domain_clean == site_host:
            return True
        site_parts = site_host.split('.')
        email_parts = email_domain_clean.split('.')
        if len(site_parts) >= 2 and len(email_parts) >= 2:
            if site_parts[-2:] == email_parts[-2:]:
                return True
    except Exception:
        pass
    return False


def extract_emails_from_rendered_page(page, site_domain, website):
    emails = set()

    try:
        body_text = page.evaluate('document.body ? document.body.innerText : ""')
        if body_text:
            for match in EMAIL_RE.findall(body_text):
                cleaned = clean_email(match)
                if cleaned:
                    emails.add(cleaned)
    except Exception:
        pass

    try:
        html_content = page.content()
        if html_content:
            for match in EMAIL_RE.findall(html_content):
                cleaned = clean_email(match)
                if cleaned:
                    emails.add(cleaned)
    except Exception:
        pass

    try:
        mailto_emails = page.evaluate("""
            () => {
                const results = [];
                document.querySelectorAll('a[href^="mailto:"]').forEach(a => {
                    const href = a.getAttribute('href');
                    const email = href.replace('mailto:', '').split('?')[0].trim();
                    if (email && email.includes('@')) results.push(email);
                });
                return results;
            }
        """)
        for email in mailto_emails:
            cleaned = clean_email(email)
            if cleaned:
                emails.add(cleaned)
    except Exception:
        pass

    try:
        cf_emails = page.evaluate("""
            () => {
                const results = [];
                document.querySelectorAll('[data-cfemail]').forEach(el => {
                    results.push(el.getAttribute('data-cfemail'));
                });
                document.querySelectorAll('a[href*="email-protection"]').forEach(a => {
                    const href = a.getAttribute('href');
                    const match = href.match(/email-protection#([a-f0-9]+)/);
                    if (match) results.push(match[1]);
                });
                return results;
            }
        """)
        for encoded in cf_emails:
            decoded = decode_cloudflare_email(encoded)
            if decoded:
                cleaned = clean_email(decoded)
                if cleaned:
                    emails.add(cleaned)
    except Exception:
        pass

    try:
        schema_emails = page.evaluate("""
            () => {
                const results = [];
                document.querySelectorAll('script[type="application/ld+json"]').forEach(s => {
                    try {
                        const data = JSON.parse(s.textContent);
                        const findEmails = (obj) => {
                            if (!obj || typeof obj !== 'object') return;
                            if (obj.email) results.push(obj.email);
                            Object.values(obj).forEach(v => {
                                if (Array.isArray(v)) v.forEach(findEmails);
                                else if (typeof v === 'object') findEmails(v);
                            });
                        };
                        findEmails(data);
                    } catch(e) {}
                });
                return results;
            }
        """)
        for email in schema_emails:
            cleaned = clean_email(email)
            if cleaned:
                emails.add(cleaned)
    except Exception:
        pass

    matched = [e for e in emails if domain_matches(e, website)]
    return matched


def find_contact_page_url(page, base_url):
    try:
        contact_url = page.evaluate("""
            (baseUrl) => {
                const links = document.querySelectorAll('a[href]');
                const keywords = ['contact', 'get-in-touch', 'enquir', 'email'];
                for (const link of links) {
                    const href = link.getAttribute('href').toLowerCase();
                    const text = link.textContent.toLowerCase().trim();
                    for (const kw of keywords) {
                        if (href.includes(kw) || text.includes(kw)) {
                            const url = new URL(link.getAttribute('href'), baseUrl);
                            if (url.hostname === new URL(baseUrl).hostname) {
                                return url.href;
                            }
                        }
                    }
                }
                return null;
            }
        """, base_url)
        return contact_url
    except Exception:
        return None


def scrape_lead_js(browser, website, lead_name):
    notes = []
    all_emails = set()

    parsed = urlparse(website if '://' in website else f'https://{website}')
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    context = browser.new_context(
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        viewport={'width': 1280, 'height': 720},
        locale='en-GB',
    )
    context.set_default_timeout(PAGE_TIMEOUT_MS)

    page = context.new_page()
    pages_checked = 0

    try:
        page.goto(website, wait_until='domcontentloaded', timeout=PAGE_TIMEOUT_MS)
        page.wait_for_timeout(RENDER_WAIT_MS)
        pages_checked += 1

        homepage_emails = extract_emails_from_rendered_page(page, parsed.netloc, website)
        all_emails.update(homepage_emails)

        personal = [e for e in all_emails if e.split('@')[0] not in GENERIC_PREFIXES]
        if personal and any(domain_matches(e, website) for e in personal):
            notes.append(f'js_homepage_personal:{len(personal)}')
            context.close()
            return list(all_emails), notes, pages_checked

        contact_url = find_contact_page_url(page, base_url)

        contact_paths = ['/contact', '/contact-us', '/get-in-touch']
        urls_to_try = []
        if contact_url:
            urls_to_try.append(contact_url)
        for path in contact_paths:
            url = base_url + path
            if url != contact_url:
                urls_to_try.append(url)

        for url in urls_to_try[:2]:
            try:
                page.goto(url, wait_until='domcontentloaded', timeout=PAGE_TIMEOUT_MS)
                page.wait_for_timeout(2000)
                pages_checked += 1

                page_emails = extract_emails_from_rendered_page(page, parsed.netloc, website)
                all_emails.update(page_emails)

                personal = [e for e in all_emails if e.split('@')[0] not in GENERIC_PREFIXES]
                if personal and any(domain_matches(e, website) for e in personal):
                    notes.append(f'js_contact_personal:{len(personal)}')
                    break
            except Exception:
                pass

    except Exception as e:
        err_msg = str(e)[:60]
        notes.append(f'js_error:{err_msg}')
    finally:
        try:
            context.close()
        except Exception:
            pass

    return list(all_emails), notes, pages_checked


def load_leads():
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    return rows, fieldnames


def save_leads(rows, fieldnames):
    tmp_file = OUTPUT_FILE + '.tmp'
    try:
        with open(tmp_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_file, OUTPUT_FILE)
    except Exception as e:
        print(f"    SAVE ERROR: {e}")
        if os.path.exists(tmp_file):
            os.remove(tmp_file)


def select_targets(rows, max_leads):
    targets = []

    for i, r in enumerate(rows):
        notes = (r.get('refinement_notes', '') or '')
        if 'js_scrape_v1' in notes:
            continue

        email = (r.get('email', '') or '').strip()
        website = (r.get('website', '') or '').strip()

        if not website or website == 'nan':
            continue

        for sd in ['facebook.com', 'instagram.com', 'twitter.com', 'linkedin.com', 'youtube.com', 'tiktok.com']:
            if sd in website.lower():
                website = None
                break
        if not website:
            continue

        is_missing = not email or email == 'nan'
        is_guessed = (
            str(r.get('email_guessed', '')).lower() == 'true'
            or str(r.get('email_type', '')).lower() == 'guessed'
        )
        is_generic = email and email != 'nan' and email.split('@')[0] in GENERIC_PREFIXES

        if is_missing:
            targets.append((i, 'missing'))
        elif is_guessed:
            targets.append((i, 'guessed'))
        elif is_generic:
            targets.append((i, 'generic'))

        if len(targets) >= max_leads:
            break

    return targets


def main():
    parser = argparse.ArgumentParser(description='JS-rendered email scrape')
    parser.add_argument('--run', action='store_true', help='Execute the scrape')
    parser.add_argument('--stats', action='store_true', help='Show stats only')
    parser.add_argument('--limit', type=int, default=MAX_LEADS, help=f'Max leads to process (default {MAX_LEADS})')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be scraped')
    args = parser.parse_args()

    rows, fieldnames = load_leads()
    targets = select_targets(rows, args.limit)

    if args.stats:
        total = len(rows)
        has_email = sum(1 for r in rows if (r.get('email', '') or '').strip() and r['email'].strip() != 'nan')
        js_done = sum(1 for r in rows if 'js_scrape_v1' in (r.get('refinement_notes', '') or ''))
        print(f"Total leads: {total}")
        print(f"Has email: {has_email} ({has_email*100/total:.1f}%)")
        print(f"JS-scrape candidates: {len(targets)}")
        print(f"Already JS-scraped: {js_done}")
        by_reason = {}
        for _, reason in targets:
            by_reason[reason] = by_reason.get(reason, 0) + 1
        for reason, count in sorted(by_reason.items()):
            print(f"  {reason}: {count}")
        return

    if args.dry_run:
        print(f"Would scrape {len(targets)} leads:")
        for idx, reason in targets[:20]:
            r = rows[idx]
            print(f"  [{reason}] {(r.get('company_name', '') or '')[:50]} -> {(r.get('website', '') or '')[:50]}")
        if len(targets) > 20:
            print(f"  ... and {len(targets) - 20} more")
        return

    if not args.run:
        print("Use --run to execute, --stats for stats, --dry-run to preview")
        return

    print("=" * 60)
    print("JS-RENDERED EMAIL SCRAPE")
    print("=" * 60)
    print(f"Targets: {len(targets)}")
    print()

    chromium_path = None
    for p in ['/nix/store/qa9cnw4v5xkxyip6mb9kxqfq1z4x2dx1-chromium-138.0.7204.100/bin/chromium']:
        if os.path.exists(p):
            chromium_path = p
            break
    if not chromium_path:
        import subprocess
        result = subprocess.run(['which', 'chromium'], capture_output=True, text=True)
        chromium_path = result.stdout.strip() if result.returncode == 0 else None

    if not chromium_path:
        print("ERROR: Chromium not found")
        return

    stats = {
        'processed': 0,
        'personal_found': 0,
        'generic_found': 0,
        'no_change': 0,
        'errors': 0,
        'timeouts': 0,
    }

    start_time = time.time()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            executable_path=chromium_path,
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu',
                  '--disable-extensions', '--disable-background-networking',
                  '--disable-sync', '--disable-translate',
                  '--no-first-run', '--disable-default-apps']
        )

        try:
            for count, (idx, reason) in enumerate(targets):
                lead = rows[idx]
                company = (lead.get('company_name', '') or '')[:50]
                website = (lead.get('website', '') or '').strip()
                old_email = (lead.get('email', '') or '').strip()
                if old_email == 'nan':
                    old_email = ''

                print(f"  [{count+1}/{len(targets)}] {company}")
                print(f"    Website: {website[:60]}")
                print(f"    Current: {old_email[:50] if old_email else '(none)'}  [{reason}]")

                lead_start = time.time()
                try:
                    found_emails, notes, pages = scrape_lead_js(browser, website, company)
                    elapsed = time.time() - lead_start

                    domain_emails = [e for e in found_emails if domain_matches(e, website)]
                    other_emails = [e for e in found_emails if not domain_matches(e, website)]

                    personal = [e for e in domain_emails if e.split('@')[0] not in GENERIC_PREFIXES]
                    generic = [e for e in domain_emails if e.split('@')[0] in GENERIC_PREFIXES]

                    new_email = None
                    email_type = None

                    if personal:
                        new_email = personal[0]
                        email_type = 'personal'
                    elif generic and (not old_email or old_email == 'nan' or
                                     str(lead.get('email_guessed', '')).lower() == 'true'):
                        new_email = generic[0]
                        email_type = 'generic'

                    existing_notes = lead.get('refinement_notes', '') or ''

                    if new_email and new_email != old_email:
                        if old_email and old_email != 'nan':
                            existing_guesses = lead.get('personal_email_guesses', '') or ''
                            if old_email not in str(existing_guesses):
                                lead['personal_email_guesses'] = f"{existing_guesses}; {old_email}".strip('; ')

                        lead['email'] = new_email
                        lead['email_guessed'] = 'false'
                        lead['email_type'] = 'verified'

                        note_parts = [f'js_scrape_v1']
                        note_parts.extend(notes)
                        note_parts.append(f'js_{email_type}:{new_email}')
                        lead['refinement_notes'] = f"{existing_notes}; {'; '.join(note_parts)}".strip('; ')

                        if email_type == 'personal':
                            stats['personal_found'] += 1
                            print(f"    FOUND ({email_type}): {new_email}  [{elapsed:.1f}s]")
                        else:
                            stats['generic_found'] += 1
                            print(f"    FOUND ({email_type}): {new_email}  [{elapsed:.1f}s]")
                    else:
                        note_parts = ['js_scrape_v1']
                        note_parts.extend(notes)
                        if domain_emails:
                            note_parts.append(f'js_emails_found:{len(domain_emails)}')
                        lead['refinement_notes'] = f"{existing_notes}; {'; '.join(note_parts)}".strip('; ')

                        stats['no_change'] += 1
                        print(f"    NO CHANGE  [{elapsed:.1f}s]")

                except Exception as e:
                    elapsed = time.time() - lead_start
                    err = str(e)[:60]
                    existing_notes = lead.get('refinement_notes', '') or ''
                    if 'timeout' in err.lower() or 'Timeout' in str(e):
                        lead['refinement_notes'] = f"{existing_notes}; js_scrape_v1; js_timeout".strip('; ')
                        stats['timeouts'] += 1
                        print(f"    TIMEOUT  [{elapsed:.1f}s]")
                    else:
                        lead['refinement_notes'] = f"{existing_notes}; js_scrape_v1; js_error:{err}".strip('; ')
                        stats['errors'] += 1
                        print(f"    ERROR: {err}  [{elapsed:.1f}s]")

                stats['processed'] += 1

                if stats['processed'] % 5 == 0:
                    save_leads(rows, fieldnames)
                    print(f"  --- Checkpoint saved ({stats['processed']}/{len(targets)}) ---")

        finally:
            browser.close()

    save_leads(rows, fieldnames)

    total_time = time.time() - start_time
    print()
    print("=" * 60)
    print("JS-RENDERED SCRAPE COMPLETE")
    print("=" * 60)
    print(f"  Duration:               {total_time/60:.1f} minutes ({int(total_time)}s)")
    print(f"  Leads processed:        {stats['processed']}")
    print(f"  Personal emails found:  {stats['personal_found']}")
    print(f"  Generic emails found:   {stats['generic_found']}")
    print(f"  No change:              {stats['no_change']}")
    print(f"  Timeouts:               {stats['timeouts']}")
    print(f"  Errors:                 {stats['errors']}")
    print(f"  TOTAL NEW EMAILS:       {stats['personal_found'] + stats['generic_found']}/{stats['processed']}")

    has_email = sum(1 for r in rows if (r.get('email', '') or '').strip() and r['email'].strip() != 'nan')
    print(f"\n  Email coverage: {has_email}/{len(rows)} ({has_email*100/len(rows):.1f}%)")


if __name__ == '__main__':
    main()
