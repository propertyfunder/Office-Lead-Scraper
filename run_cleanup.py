#!/usr/bin/env python3
"""
Lead quality cleanup pipeline — identifies and fixes suspect contact names
and guessed emails via targeted re-scraping and optional OpenAI validation.

Usage:
    python run_cleanup.py --stats                          # Show cleanup target counts
    python run_cleanup.py --dry-run                        # Show flagged records without changes
    python run_cleanup.py --run                            # Execute full cleanup
    python run_cleanup.py --run --limit 50                 # Process first 50 flagged leads
    python run_cleanup.py --run --skip-rescrape            # Skip website re-scrape (name/email fix only)
    python run_cleanup.py --run --skip-openai              # Skip OpenAI fallback
    python run_cleanup.py --run --openai-only              # Only run OpenAI on previously cleared names
"""

import sys
import os
import re
import argparse
import time
import threading
from datetime import date, datetime

sys.path.insert(0, '.')
from main import load_leads_from_csv
from src.enricher import LeadEnricher, _is_empty, OpenAICostTracker
from src.utils import (save_leads_to_csv, extract_domain, guess_email,
                       generate_email_guesses, normalize_name, clean_email,
                       make_request, get_all_fieldnames)
from src.models import BusinessLead

INPUT_FILE = 'unit8_leads_enriched.csv'
OUTPUT_FILE = 'unit8_leads_enriched.csv'
MISSING_NAME_FILE = 'unit8_leads_missing_name.csv'

MAX_PAGES_PER_LEAD = 5
MAX_HTTP_REQUESTS_PER_LEAD = 8
HARD_TIMEOUT_SECONDS = 45

UI_ARTIFACT_NAMES = {
    'book now', 'opening times', 'opening hours', 'gallery', 'photo gallery',
    'dermal fillers', 'get in touch', 'contact us', 'about us', 'read more',
    'learn more', 'view more', 'click here', 'submit', 'send', 'search',
    'our team', 'the team', 'our services', 'our story', 'services',
    'treatments', 'testimonials', 'testimonial', 'faq', 'blog', 'news',
    'events', 'careers', 'menu', 'close', 'home', 'welcome', 'hello',
    'privacy policy', 'terms', 'cookie policy', 'disclaimer',
    'join a class', 'room pricing', 'what we do', 'a message from',
    'my approach', 'grand opening', 'google reviews', 'real touch',
    'consultant neuropsychologist testimonial', 'grand opening spectacular',
}

UI_ARTIFACT_PATTERNS = [
    r'^(book|view|read|learn|click|find out|get|join|see|our|the|my|a)\b',
    r'\b(pricing|gallery|opening|times|hours|reviews|testimonial)s?\b',
    r'^(menu|home|close|submit|send|search|welcome|hello|hey)\b',
    r'\b(privacy|cookie|terms|disclaimer|policy)\b',
]

ROLE_ONLY_PATTERNS = [
    r'^(therapist|clinician|doctor|nurse|dentist|hygienist|podiatrist)\b',
    r'\b(in|at|for|of)\s+(motion|health|mind|life|beauty|touch)$',
    r'^(bacp|hcpc|nhs|gdc)\s+',
]


class CleanupEnricher(LeadEnricher):
    def __init__(self):
        self.google_api_key = ""
        self.companies_house_api_key = ""
        self.openai_api_key = os.environ.get("OPENAI_API_KEY", "")
        self.cost_tracker = OpenAICostTracker()
        self.session = None
        self._init_session()
        self._init_name_data()

    def _init_session(self):
        import requests
        self.session = requests.Session()
        try:
            from fake_useragent import UserAgent
            self.session.headers['User-Agent'] = UserAgent().random
        except Exception:
            self.session.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

    def _init_name_data(self):
        self.invalid_names = {
            'click here', 'read more', 'learn more', 'find out', 'view more',
            'book now', 'our team', 'the team', 'contact us', 'get in touch',
            'about us', 'home', 'menu', 'close', 'submit', 'send', 'search',
            'opening times', 'opening hours', 'privacy policy', 'terms',
            'cookie policy', 'disclaimer', 'gallery', 'services', 'treatments',
            'testimonials', 'faq', 'blog', 'news', 'events', 'careers',
            'our services', 'our story', 'welcome', 'hello', 'hey'
        }
        self.NOUN_PLACEHOLDERS = {
            'therapy', 'clinic', 'practice', 'studio', 'centre', 'center',
            'health', 'wellness', 'dental', 'medical', 'physio', 'osteo',
            'chiro', 'acupuncture', 'massage', 'beauty', 'skin', 'hair',
            'nails', 'spa', 'gym', 'fitness', 'yoga', 'pilates'
        }
        self.invalid_name_phrases = [
            'counselling', 'hypnotherapy', 'physiotherapy', 'osteopathy',
            'chiropractic', 'therapy', 'clinic', 'practice', 'studio',
            'dental', 'yoga', 'pilates', 'massage', 'acupuncture',
            'reflexology', 'wellness', 'fitness', 'gardens', 'oxfordshire',
            'surrey', 'hampshire', 'berkshire', 'sussex', 'london',
            'guildford', 'farnham', 'godalming', 'woking', 'stafford'
        ]
        self.SHORT_VALID_NAMES = {
            'ali', 'jo', 'al', 'ed', 'em', 'mo', 'bo', 'ty', 'di',
            'lu', 'vi', 'aj', 'jd', 'li', 'an'
        }
        self.COMMON_UK_FIRST_NAMES = set()
        try:
            names_file = os.path.join(os.path.dirname(__file__), 'src', 'uk_first_names.txt')
            if os.path.exists(names_file):
                with open(names_file) as f:
                    self.COMMON_UK_FIRST_NAMES = {n.strip().lower() for n in f if n.strip()}
        except Exception:
            pass

    def _get_director_from_companies_house(self, *args, **kwargs):
        return {}

    def _search_linkedin_for_contact(self, *args, **kwargs):
        return {}


def is_ui_artifact(name):
    if not name:
        return False
    name_lower = name.strip().lower()
    if name_lower in UI_ARTIFACT_NAMES:
        return True
    for pattern in UI_ARTIFACT_PATTERNS:
        if re.search(pattern, name_lower):
            if len(name_lower.split()) <= 3:
                return True
    return False


def is_role_only_name(name):
    if not name:
        return False
    name_lower = name.strip().lower()
    for pattern in ROLE_ONLY_PATTERNS:
        if re.search(pattern, name_lower):
            return True
    role_words = {'therapist', 'clinician', 'doctor', 'nurse', 'dentist',
                  'hygienist', 'podiatrist', 'osteopath', 'chiropractor',
                  'acupuncturist', 'physiotherapist', 'counsellor', 'psychologist'}
    words = set(name_lower.split())
    if words and words.issubset(role_words | {'the', 'our', 'lead', 'senior', 'head', 'chief', 'in', 'at', 'and'}):
        return True
    return False


def is_address_fragment(name, enricher=None):
    if not name:
        return False

    if enricher and enricher._is_valid_contact_name(name):
        first_name = name.strip().split()[0].lower().replace("'","").replace("-","")
        if hasattr(enricher, 'COMMON_UK_FIRST_NAMES') and first_name in enricher.COMMON_UK_FIRST_NAMES:
            return False

    name_lower = name.strip().lower()
    strong_address = {'street', 'road', 'avenue', 'drive', 'crescent', 'terrace',
                      'gardens', 'mews', 'square', 'row'}
    words = name_lower.split()
    if any(w in strong_address for w in words):
        return True

    ambiguous_address = {'lane', 'close', 'court', 'place', 'way', 'hill', 'park', 'green'}
    if any(w in ambiguous_address for w in words):
        if len(words) == 2 and words[0][0:1].isupper() if name.strip().split() else False:
            if enricher and enricher._is_valid_contact_name(name):
                return False
        return True

    if re.search(r'\b[A-Z]{1,2}\d{1,2}\b', name):
        return True
    return False


def is_company_name_echo(name, company_name, enricher=None):
    if not name or not company_name:
        return False
    cn = re.sub(r'[^a-z\s]', '', name.lower()).strip()
    co = re.sub(r'[^a-z\s]', '', company_name.lower()).strip()
    if not cn or not co:
        return False

    if enricher and enricher._is_valid_contact_name(name):
        return False

    stop = {'the', 'and', 'of', 'ltd', 'limited', 'llp', 'uk', 'plc', 'inc', 'in', 'at', 'for'}
    cn_words = set(cn.split()) - stop
    co_words = set(co.split()) - stop
    if cn_words and cn_words == co_words:
        return True
    if len(cn) > 3 and (cn in co or co in cn):
        return True
    return False


def is_concatenated_text(name):
    if not name:
        return False
    if re.search(r'[a-z][A-Z]', name) and len(name) > 15:
        return True
    if len(name.split()) >= 4 and not all(w[0].isupper() for w in name.split() if w):
        return True
    return False


def has_title_only(name):
    if not name:
        return False
    words = name.strip().lower().split()
    titles = {'dr', 'dr.', 'mr', 'mr.', 'mrs', 'mrs.', 'ms', 'ms.', 'miss', 'prof', 'prof.'}
    name_words = [w for w in words if w not in titles]
    if len(name_words) == 1:
        return True
    return False


def flag_suspect_name(lead, enricher):
    name = (lead.contact_name or '').strip()
    if not name:
        return None

    if is_ui_artifact(name):
        return f"ui_artifact:{name}"

    if is_address_fragment(name, enricher):
        return f"address_fragment:{name}"

    if is_role_only_name(name):
        return f"role_only_name:{name}"

    if is_company_name_echo(name, lead.company_name, enricher):
        return f"company_name_echo:{name}"

    if is_concatenated_text(name):
        return f"concatenated_text:{name}"

    notes = (lead.refinement_notes or '').lower()
    suspect_markers = ['possible_placeholder_name', 'rejected_garbage_name',
                       'vanity_name_match', 'suspicious_name_replaced']
    if any(m in notes for m in suspect_markers):
        return f"flagged_in_notes:{name}"

    try:
        conf = float(lead.confidence_score) if lead.confidence_score else None
        if conf is not None and conf <= 2:
            return f"low_confidence:{name}"
    except (ValueError, TypeError):
        pass

    if not enricher._is_valid_contact_name(name):
        if has_title_only(name):
            return f"title_only:{name}"
        return f"failed_validation:{name}"

    return None


def flag_guessed_email(lead):
    if lead.email_guessed != 'true':
        return None
    if not lead.email:
        return None
    bad_patterns = ['account.', 'shopping.', 'business.', 'experience.', 'spiritual.',
                    'subscribe.', 'rapid.', 'extended.', 'best.']
    email_local = lead.email.split('@')[0].lower()
    if any(email_local.startswith(p) for p in bad_patterns):
        return f"bad_guess_pattern:{lead.email}"
    has_website = bool(lead.website and lead.website.strip())
    has_generic = bool(lead.generic_email and '@' in lead.generic_email)
    if has_website or has_generic:
        return f"guessed_with_website:{lead.email}"
    return None


def identify_cleanup_targets(leads, enricher):
    suspect_names = []
    guessed_emails = []

    for idx, lead in enumerate(leads):
        name_flag = flag_suspect_name(lead, enricher)
        if name_flag:
            suspect_names.append((idx, name_flag))

        email_flag = flag_guessed_email(lead)
        if email_flag:
            guessed_emails.append((idx, email_flag))

    return suspect_names, guessed_emails


def rescrape_for_contact(enricher, lead):
    notes = []
    if not lead.website:
        notes.append('cleanup_skipped:no_website')
        return lead, notes, {'email': '', 'contact': '', 'contacts': [], 'generic_email': ''}

    is_social = any(d in lead.website.lower() for d in
        ['facebook.com', 'fb.com', 'instagram.com', 'twitter.com'])
    if is_social:
        notes.append('cleanup_skipped:social_only')
        return lead, notes, {'email': '', 'contact': '', 'contacts': [], 'generic_email': ''}

    try:
        web_result = enricher._enrich_from_website(lead)
    except Exception as e:
        notes.append(f'cleanup_scrape_error:{str(e)[:60]}')
        return lead, notes, {'email': '', 'contact': '', 'contacts': [], 'generic_email': ''}

    found_email = web_result.get('email', '')
    found_contact = web_result.get('contact', '')
    web_contacts = web_result.get('contacts', [])
    web_generic = web_result.get('generic_email', '')
    web_notes = web_result.get('_notes', [])

    if web_notes:
        notes.extend(web_notes)

    if web_generic and (not lead.generic_email or not lead.generic_email.strip()):
        lead.generic_email = clean_email(web_generic)
        if lead.generic_email:
            notes.append(f'cleanup_found_generic:{lead.generic_email}')

    result = {
        'email': found_email,
        'contact': found_contact,
        'contacts': web_contacts,
        'generic_email': web_generic,
    }

    return lead, notes, result


def resolve_contact_name(lead, scrape_result, enricher, notes, flag_reason=""):
    old_name = (lead.contact_name or '').strip()
    found_contact = scrape_result.get('contact', '')
    web_contacts = scrape_result.get('contacts', [])

    safe_categories = {'low_confidence', 'flagged_in_notes'}
    flag_category = flag_reason.split(':')[0] if flag_reason else ''
    is_definitely_fake = flag_category in {
        'ui_artifact', 'address_fragment', 'role_only_name',
        'concatenated_text', 'company_name_echo'
    }

    valid_candidates = []

    if found_contact and enricher._is_valid_contact_name(found_contact):
        if not enricher._is_domain_name(found_contact, lead.website or ''):
            if not enricher._is_vanity_name(found_contact, lead.company_name):
                if not enricher._is_suspicious_name(found_contact):
                    valid_candidates.append({
                        'name': normalize_name(found_contact),
                        'source': 'website_primary',
                        'verified': True
                    })

    for c in (web_contacts or []):
        cname = c.get('name', '')
        if not cname or not enricher._is_valid_contact_name(cname):
            continue
        if enricher._is_domain_name(cname, lead.website or ''):
            continue
        if enricher._is_vanity_name(cname, lead.company_name):
            continue
        valid_candidates.append({
            'name': normalize_name(cname),
            'source': 'website_team',
            'verified': True,
            'role': c.get('role', '')
        })

    role_priority = {
        'owner': 1, 'founder': 1, 'director': 2, 'managing': 2,
        'principal': 3, 'lead': 3, 'senior': 4, 'head': 4,
        'doctor': 5, 'dr': 5, 'surgeon': 5, 'consultant': 5,
        'therapist': 6, 'practitioner': 6, 'clinician': 6,
    }

    def candidate_priority(c):
        role = (c.get('role', '') or '').lower()
        best = 99
        for keyword, pri in role_priority.items():
            if keyword in role:
                best = min(best, pri)
        name_lower = c['name'].lower()
        if name_lower.startswith('dr ') or name_lower.startswith('dr.'):
            best = min(best, 5)
        return best

    if valid_candidates:
        valid_candidates.sort(key=candidate_priority)
        best = valid_candidates[0]
        lead.contact_name = best['name']
        lead.contact_source = "website_verified"
        lead.contact_verified = "true"
        notes.append(f'cleanup_name_replaced:{old_name}->{best["name"]}')

        if len(valid_candidates) > 1:
            all_names = [c['name'] for c in valid_candidates]
            lead.contact_names = "; ".join(all_names)
            lead.multiple_contacts = "true"
    elif is_definitely_fake:
        previously_verified = (lead.contact_source or '') in ('website_verified', 'companies_house')
        if old_name and not previously_verified:
            lead.contact_name = ""
            lead.contact_source = ""
            lead.contact_verified = "false"
            notes.append(f'cleanup_fake_removed:{old_name}')
            notes.append('no_named_individual_found')
        elif old_name and previously_verified:
            notes.append(f'cleanup_kept_verified_source:{old_name}')
    else:
        notes.append(f'cleanup_name_kept_no_replacement:{old_name}')

    return lead, notes


def resolve_email(lead, scrape_result, enricher, notes):
    old_email = (lead.email or '').strip()
    old_guessed = lead.email_guessed
    found_email = scrape_result.get('email', '')

    if found_email and '@' in found_email:
        cleaned = clean_email(found_email)
        if cleaned:
            if old_guessed == 'true' and old_email:
                existing_guesses = lead.personal_email_guesses or ''
                if old_email not in existing_guesses:
                    lead.personal_email_guesses = f"{existing_guesses}; {old_email}".strip('; ')
                notes.append(f'cleanup_email_replaced:{old_email}->{cleaned}')

            lead.email = cleaned
            lead.email_guessed = "false"
            notes.append(f'cleanup_real_email_found:{cleaned}')
            return lead, notes

    if lead.contact_name and lead.website:
        domain = extract_domain(lead.website)
        if domain and lead.email_guessed == 'true':
            notes.append('email_guessed_only_no_website_email')

    if not found_email and not lead.email:
        web_generic = scrape_result.get('generic_email', '')
        if web_generic:
            cleaned = clean_email(web_generic)
            if cleaned:
                lead.generic_email = cleaned
                notes.append(f'cleanup_generic_email_found:{cleaned}')

    return lead, notes


def openai_validate_contact(enricher, lead, notes):
    if not enricher.openai_api_key:
        actual_key = os.environ.get("OPENAI_API_KEY", "")
        if actual_key:
            enricher.openai_api_key = actual_key

    if not enricher.openai_api_key:
        notes.append('cleanup_openai_skipped:no_key')
        return lead, notes

    if not enricher.cost_tracker.can_make_call():
        notes.append('cleanup_openai_skipped:budget_exhausted')
        return lead, notes

    if not lead.website:
        notes.append('cleanup_openai_skipped:no_website')
        return lead, notes

    website_text = ""
    try:
        website_text = enricher._get_minimal_text(lead)
    except Exception:
        pass

    if not website_text or len(website_text.strip()) < 50:
        notes.append('cleanup_openai_skipped:insufficient_website_text')
        return lead, notes

    prompt = f"""Analyze this UK wellness/health business website text to identify the owner, founder, or lead practitioner.

Company: {lead.company_name}
Sector: {lead.sector or 'wellness/health'}
Website: {lead.website}

Website text:
{website_text[:2000]}

Task: Identify if a specific person appears to be the owner, founder, or lead practitioner of this business.
Rules:
- Only return a name if you are confident it is a real person
- Do not guess or invent names
- Role titles alone (e.g. "The Therapist") do not count
- Marketing text or UI elements do not count
- If the business appears to have multiple practitioners with no clear owner, return NONE

Reply with EXACTLY one line:
FOUND: [Full Name] | [Role if apparent]
or
NONE: [brief reason]"""

    try:
        from openai import OpenAI
        client = OpenAI(api_key=enricher.openai_api_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You extract real person names from business website text. Be strict — only return names you are confident are real people, not business names or roles."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=100,
            temperature=0
        )
        enricher.cost_tracker.record_call(response.usage.total_tokens if response.usage else 200)
        answer = response.choices[0].message.content.strip()
        notes.append(f'cleanup_openai:{answer[:80]}')

        if answer.upper().startswith('FOUND:'):
            parts = answer[6:].strip().split('|')
            found_name = parts[0].strip()
            found_role = parts[1].strip() if len(parts) > 1 else ''

            if found_name and enricher._is_valid_contact_name(found_name):
                if not enricher._is_vanity_name(found_name, lead.company_name):
                    lead.contact_name = normalize_name(found_name)
                    lead.contact_source = "openai_cleanup"
                    lead.contact_verified = "false"
                    if found_role:
                        lead.contact_titles = found_role
                    notes.append(f'cleanup_openai_found:{found_name}')
                    print(f"    [OpenAI] Found: {found_name} ({found_role})")
                else:
                    notes.append(f'cleanup_openai_vanity_rejected:{found_name}')
            else:
                notes.append(f'cleanup_openai_invalid_name:{found_name}')
        else:
            notes.append('cleanup_openai_no_contact')
            print(f"    [OpenAI] No contact found: {answer[:60]}")

    except Exception as e:
        notes.append(f'cleanup_openai_error:{str(e)[:60]}')

    return lead, notes


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
        return None, ['cleanup_timeout_exceeded']

    if container['error'] is not None:
        return None, [f'cleanup_error:{str(container["error"])[:60]}']

    return container['result'], []


def print_cleanup_stats(leads, enricher):
    suspect_names, guessed_emails = identify_cleanup_targets(leads, enricher)

    name_categories = {}
    for idx, flag in suspect_names:
        cat = flag.split(':')[0]
        name_categories[cat] = name_categories.get(cat, 0) + 1

    print()
    print("=" * 60)
    print("CLEANUP TARGET ANALYSIS")
    print("=" * 60)
    print(f"  Total leads:            {len(leads)}")
    print(f"  With contact_name:      {sum(1 for l in leads if l.contact_name and l.contact_name.strip())}")
    print()
    print(f"  SUSPECT NAMES:          {len(suspect_names)}")
    for cat, count in sorted(name_categories.items(), key=lambda x: -x[1]):
        print(f"    {cat}: {count}")
    print()
    print(f"  GUESSED EMAILS:         {len(guessed_emails)}")
    print()

    all_flagged = set(idx for idx, _ in suspect_names) | set(idx for idx, _ in guessed_emails)
    print(f"  TOTAL UNIQUE FLAGGED:   {len(all_flagged)}")
    print(f"  With website:           {sum(1 for idx in all_flagged if leads[idx].website and leads[idx].website.strip())}")
    print()

    print("  Sample suspect names:")
    for idx, flag in suspect_names[:15]:
        lead = leads[idx]
        print(f"    [{flag.split(':')[0]}] '{lead.contact_name}' — {lead.company_name[:40]}")
    if len(suspect_names) > 15:
        print(f"    ... and {len(suspect_names) - 15} more")

    print()
    print("  Sample guessed emails:")
    for idx, flag in guessed_emails[:10]:
        lead = leads[idx]
        print(f"    '{lead.email}' — {lead.company_name[:40]}")
    if len(guessed_emails) > 10:
        print(f"    ... and {len(guessed_emails) - 10} more")
    print()


def main():
    parser = argparse.ArgumentParser(description='Lead quality cleanup pipeline')
    parser.add_argument('--stats', action='store_true', help='Show cleanup targets and exit')
    parser.add_argument('--dry-run', action='store_true', help='Show flagged records without processing')
    parser.add_argument('--run', action='store_true', help='Execute cleanup')
    parser.add_argument('--limit', type=int, default=0, help='Max leads to process (0=all)')
    parser.add_argument('--skip-rescrape', action='store_true', help='Skip website re-scraping')
    parser.add_argument('--skip-openai', action='store_true', help='Skip OpenAI fallback')
    parser.add_argument('--openai-only', action='store_true', help='Only run OpenAI on leads missing name after prior cleanup')
    parser.add_argument('--input', default=INPUT_FILE, help='Input CSV file')
    parser.add_argument('--output', default=OUTPUT_FILE, help='Output CSV file')
    args = parser.parse_args()

    leads = load_leads_from_csv(args.input)
    print(f"Loaded {len(leads)} leads from {args.input}")

    enricher = CleanupEnricher()

    if args.stats:
        print_cleanup_stats(leads, enricher)
        return

    suspect_names, guessed_emails = identify_cleanup_targets(leads, enricher)
    all_flagged_indices = list(dict.fromkeys(
        [idx for idx, _ in suspect_names] + [idx for idx, _ in guessed_emails]
    ))

    suspect_idx_set = set(idx for idx, _ in suspect_names)
    email_idx_set = set(idx for idx, _ in guessed_emails)

    suspect_flags = {idx: flag for idx, flag in suspect_names}
    email_flags = {idx: flag for idx, flag in guessed_emails}

    if args.dry_run:
        print()
        print("=" * 60)
        print("DRY RUN — Flagged Records")
        print("=" * 60)
        print(f"\n  Suspect names ({len(suspect_names)}):")
        for idx, flag in suspect_names:
            lead = leads[idx]
            print(f"    {idx:4d}. [{flag.split(':')[0]:20s}] '{lead.contact_name}' — {lead.company_name[:40]}")
        print(f"\n  Guessed emails ({len(guessed_emails)}):")
        for idx, flag in guessed_emails:
            lead = leads[idx]
            print(f"    {idx:4d}. '{lead.email}' — {lead.company_name[:40]} (generic={lead.generic_email or 'none'})")
        print(f"\n  Total unique leads to process: {len(all_flagged_indices)}")
        return

    if not args.run:
        print("Use --stats, --dry-run, or --run")
        parser.print_help()
        return

    if args.openai_only:
        openai_targets = []
        for idx in all_flagged_indices:
            lead = leads[idx]
            notes = (lead.refinement_notes or '').lower()
            if ('cleanup_fake_removed' in notes or 'no_named_individual_found' in notes) and not lead.contact_name:
                if lead.website and lead.website.strip():
                    openai_targets.append(idx)
        all_flagged_indices = openai_targets
        print(f"\n[OpenAI-only mode] {len(openai_targets)} leads with removed names and websites")

    targets = all_flagged_indices
    if args.limit > 0:
        targets = targets[:args.limit]

    print()
    print("=" * 60)
    print("LEAD QUALITY CLEANUP")
    print("=" * 60)
    print(f"  Targets:         {len(targets)} leads")
    print(f"    - Suspect names:  {sum(1 for t in targets if t in suspect_idx_set)}")
    print(f"    - Guessed emails: {sum(1 for t in targets if t in email_idx_set)}")
    print(f"  Re-scrape:       {'SKIP' if args.skip_rescrape else 'YES'}")
    print(f"  OpenAI fallback: {'SKIP' if args.skip_openai else 'YES'}")
    print(f"  Started:         {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    if len(targets) == 0:
        print("\nNo leads to clean up. Dataset looks good!")
        return

    stats = {
        'processed': 0,
        'names_removed': 0,
        'names_replaced': 0,
        'names_kept': 0,
        'emails_replaced': 0,
        'emails_kept': 0,
        'openai_found': 0,
        'openai_none': 0,
        'errors': 0,
        'timeouts': 0,
    }

    start_time = time.time()

    for count, idx in enumerate(targets):
        lead = leads[idx]
        all_notes = []
        old_name = (lead.contact_name or '').strip()
        old_email = (lead.email or '').strip()
        old_contact_source = (lead.contact_source or '').strip()
        old_was_verified = old_contact_source in ('website_verified', 'companies_house')
        has_suspect_name = idx in suspect_idx_set
        has_guessed_email = idx in email_idx_set

        print(f"\n[{count+1}/{len(targets)}] {lead.company_name}")
        if has_suspect_name:
            print(f"  Flag: {suspect_flags.get(idx, 'unknown')}")
        if has_guessed_email:
            print(f"  Email flag: {email_flags.get(idx, 'unknown')}")

        scrape_result = {'email': '', 'contact': '', 'contacts': [], 'generic_email': ''}

        if not args.skip_rescrape and not args.openai_only:
            result_tuple, timeout_notes = run_with_timeout(
                rescrape_for_contact, (enricher, lead), HARD_TIMEOUT_SECONDS
            )
            if timeout_notes:
                all_notes.extend(timeout_notes)
                stats['timeouts'] += 1
            elif result_tuple is not None:
                lead, scrape_notes, scrape_result = result_tuple
                all_notes.extend(scrape_notes)
            else:
                stats['errors'] += 1

        if has_suspect_name and not args.openai_only:
            flag_reason = suspect_flags.get(idx, '')
            lead, name_notes = resolve_contact_name(
                lead, scrape_result, enricher, all_notes, flag_reason
            )
            all_notes = name_notes

        if has_guessed_email and not args.openai_only:
            lead, email_notes = resolve_email(lead, scrape_result, enricher, all_notes)
            all_notes = email_notes

        name_was_removed = (
            old_name and
            not lead.contact_name and
            any('cleanup_fake_removed' in n for n in all_notes)
        )

        need_openai = (
            name_was_removed and
            not old_was_verified and
            not args.skip_openai and
            lead.website
        )
        if args.openai_only and not lead.contact_name and lead.website:
            need_openai = True

        if need_openai:
            print(f"  [OpenAI fallback] Removed name '{old_name}', trying AI...")
            lead, openai_notes = openai_validate_contact(enricher, lead, all_notes)
            all_notes = openai_notes
            if lead.contact_name:
                stats['openai_found'] += 1
            else:
                stats['openai_none'] += 1

        lead.enrichment_status = enricher._determine_status(lead)
        lead.email_type = enricher._classify_email_type(lead)
        lead.confidence_score = enricher._calculate_confidence_score(lead)
        lead.mailshot_category = enricher._classify_mailshot(lead)

        if all_notes:
            existing = lead.refinement_notes or ""
            new_notes = "; ".join(all_notes)
            lead.refinement_notes = f"{existing}; {new_notes}".strip("; ") if existing else new_notes

        lead.last_enriched_date = str(date.today())

        stats['processed'] += 1

        current_name = (lead.contact_name or '').strip()
        if has_suspect_name:
            if current_name and current_name != old_name:
                stats['names_replaced'] += 1
            elif not current_name and old_name:
                stats['names_removed'] += 1
            else:
                stats['names_kept'] += 1

        if has_guessed_email:
            current_email = (lead.email or '').strip()
            if current_email != old_email and lead.email_guessed != 'true':
                stats['emails_replaced'] += 1
            else:
                stats['emails_kept'] += 1

        save_leads_to_csv(leads, args.output, mode='w')

        if (count + 1) % 10 == 0 or count == len(targets) - 1:
            elapsed = time.time() - start_time
            rate = stats['processed'] / elapsed * 60 if elapsed > 0 else 0
            print(f"\n  --- Progress: {stats['processed']}/{len(targets)} ({rate:.0f}/min) "
                  f"| replaced: {stats['names_replaced']} | removed: {stats['names_removed']} "
                  f"| emails fixed: {stats['emails_replaced']} | errors: {stats['errors']} ---")

    elapsed = time.time() - start_time
    print()
    print("=" * 60)
    print("CLEANUP COMPLETE")
    print("=" * 60)
    print(f"  Duration:         {elapsed/60:.1f} minutes ({elapsed:.0f}s)")
    print(f"  Processed:        {stats['processed']}")
    print(f"  Names replaced:   {stats['names_replaced']}")
    print(f"  Names removed:    {stats['names_removed']} (fake/artifact cleared)")
    print(f"  Names kept:       {stats['names_kept']}")
    print(f"  Emails replaced:  {stats['emails_replaced']} (guessed -> real)")
    print(f"  Emails kept:      {stats['emails_kept']}")
    if not args.skip_openai:
        print(f"  OpenAI found:     {stats['openai_found']}")
        print(f"  OpenAI no result: {stats['openai_none']}")
    print(f"  Timeouts:         {stats['timeouts']}")
    print(f"  Errors:           {stats['errors']}")
    print(f"  Saved to:         {args.output}")

    missing_name_leads = [l for l in leads if not (l.contact_name or '').strip()]
    if missing_name_leads:
        save_leads_to_csv(missing_name_leads, MISSING_NAME_FILE, mode='w')
        print(f"  Missing names:    {len(missing_name_leads)} (saved to {MISSING_NAME_FILE})")

    log_path = 'cleanup_log.txt'
    with open(log_path, 'a') as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Duration: {elapsed/60:.1f}min | Processed: {stats['processed']}\n")
        for k, v in stats.items():
            f.write(f"  {k}: {v}\n")
    print(f"  Log appended to:  {log_path}")

    total_with_name = sum(1 for l in leads if (l.contact_name or '').strip())
    total = len(leads)
    print()
    print(f"  Final contact coverage: {total_with_name}/{total} ({total_with_name/total*100:.1f}%)")


if __name__ == '__main__':
    main()
