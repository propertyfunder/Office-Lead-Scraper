#!/usr/bin/env python3
"""
Final data sanitisation pipeline — removes fake contacts, resolves sole-trader
businesses, re-validates guessed emails, and validates low-confidence names.

Usage:
    python run_final_sanitise.py --stats
    python run_final_sanitise.py --dry-run
    python run_final_sanitise.py --run [--limit N] [--skip-openai] [--skip-rescrape]
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
from src.enricher import LeadEnricher, _is_empty, OpenAICostTracker
from src.utils import (save_leads_to_csv, extract_domain, clean_email,
                       normalize_name, make_request, get_all_fieldnames)
from src.models import BusinessLead

INPUT_FILE = 'unit8_leads_enriched.csv'
OUTPUT_FILE = 'unit8_leads_enriched.csv'
MISSING_NAME_FILE = 'unit8_leads_missing_name.csv'
HARD_TIMEOUT_SECONDS = 30


class SanitiseEnricher(LeadEnricher):
    def __init__(self):
        self.google_api_key = ""
        self.companies_house_api_key = ""
        self.openai_api_key = os.environ.get("OPENAI_API_KEY", "")
        self.cost_tracker = OpenAICostTracker()
        self.session = None
        self.generic_email_prefixes = [
            'info', 'contact', 'enquiries', 'hello', 'admin', 'reception',
            'office', 'mail', 'enquiry', 'general', 'support', 'help', 'sales'
        ]
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
            'guildford', 'woking', 'farnham', 'godalming', 'haslemere',
            'nutrition', 'aesthetics', 'osteopaths', 'podiatry',
        ]
        self.SHORT_VALID_NAMES = {'al', 'bo', 'ed', 'jo', 'ty', 'di', 'lu', 'ng', 'yu', 'li', 'yi', 'qi'}
        self.COMMON_UK_FIRST_NAMES = self.COMMON_UK_FIRST_NAMES | {
            'suzanna', 'andy', 'carolyn', 'joanna', 'annemarie', 'andrea',
            'bridget', 'rachael', 'yvette', 'sheila', 'keeley', 'christiane',
            'henriette', 'katrina', 'lesley', 'lindsey', 'beverley', 'gillian',
            'denise', 'elaine', 'lorraine', 'dianne', 'sheena', 'mavis',
            'daphne', 'beryl', 'mabel', 'muriel', 'brenda', 'cynthia',
            'moira', 'vivienne', 'iris', 'hilda', 'marjorie', 'constance',
            'hilary', 'felicity', 'vanessa', 'tamsin', 'philippa', 'annabel',
            'harriet', 'imogen', 'phoebe', 'lydia', 'chloe', 'naomi',
            'abigail', 'madeline', 'eleanor', 'rosie', 'rosa', 'clara',
            'stella', 'lara', 'nina', 'vera', 'gail', 'leah', 'rhian',
            'sian', 'cerys', 'nia', 'angharad', 'catrin', 'bethan',
            'sioned', 'ffion', 'carys', 'siobhan', 'maeve', 'sinead',
            'niamh', 'roisin', 'aisling', 'ciara', 'aoife', 'orla',
            'derek', 'clive', 'barry', 'trevor', 'cyril', 'cedric',
            'reginald', 'ernest', 'bertie', 'archie', 'freddie', 'alfie',
            'alfredo', 'matteo', 'luca', 'marco', 'fabio', 'carlo',
            'alison', 'kirsten', 'megan', 'rhys', 'lloyd', 'owen',
            'dylan', 'iwan', 'dafydd', 'emyr', 'gwyn', 'huw',
            'annette', 'yvonne', 'colette', 'danielle', 'jacqueline',
            'suzanne', 'renee', 'dominique', 'monique', 'simone',
            'arnaud', 'pascal', 'thierry', 'remy', 'alain',
        }


GARBAGE_WORDS = {
    'suspended', 'subscribed', 'subscribe', 'cart', 'shopping', 'account',
    'pricing', 'treatments', 'treatment', 'gallery', 'reviews', 'review',
    'opening', 'approach', 'discover', 'welcome', 'routine', 'salon',
    'hospital', 'university', 'college', 'assurance', 'insurance',
    'pilates', 'podiatry', 'dental', 'healthcare', 'hygienist',
    'yoga', 'physio', 'chiropractic', 'clinic', 'surgery', 'medical',
    'limited', 'osteopaths', 'osteopath', 'holistic', 'aesthetics',
    'nutrition', 'counselling', 'hypnotherapy', 'wellness', 'fitness',
    'beauty', 'house', 'space', 'world', 'village', 'community',
    'care', 'touch', 'massage', 'acupuncture', 'remedies', 'specialist',
}

GARBAGE_EXACT = {
    'let\'s chat', 'who we', 'dock no', 'ear care', 'real touch',
    'my ashram', 'my planet', 'my healthy', 'account suspended',
    'shopping cart', 'subscribe subscribed', 'experience friendlyand',
    'routine nail care', 'vibrant world', 'learning space',
    'beauty salon', 'medical assurance', 'crofton healthcare',
    'joe pilates', 'elizabeth pilates', 'wilson podiatry',
    'wickersley podiatry', 'east west college', 'thomas hospitals',
    'victoria hospital', 'hillcroft house', 'personalized approach',
}

VERB_PREFIXES = {
    'join', 'book', 'discover', 'explore', 'find', 'view', 'read',
    'learn', 'subscribe', 'get', 'see', 'meet', 'welcome', 'visit',
    'experience', 'try', 'start', 'begin', 'browse', 'contact',
}

MULTI_PRACTITIONER_WORDS = {
    'clinic', 'centre', 'center', 'group', 'practice', 'hospital',
    'surgery', 'partnership', 'associates', 'trust', 'nhs', 'team',
    'dental', 'medical', 'leisure', 'gym', 'class', 'village',
    'studio', 'studios', 'spa', 'house', 'lodge', 'collection',
}

SERVICE_SUFFIXES = {
    'counselling', 'counsellor', 'therapy', 'therapist', 'hypnotherapy',
    'hypnotherapist', 'physiotherapy', 'physiotherapist', 'osteopathy',
    'osteopath', 'chiropractic', 'chiropractor', 'acupuncture',
    'acupuncturist', 'nutrition', 'nutritionist', 'massage', 'yoga',
    'pilates', 'podiatry', 'podiatrist', 'reflexology', 'reflexologist',
    'psychotherapy', 'psychotherapist', 'coaching', 'coach', 'aesthetics',
    'beauty', 'holistic', 'wellness', 'fitness', 'meditation',
    'healing', 'healer', 'midwifery', 'midwife', 'doula',
}


def is_garbage_name(name, enricher):
    if not name or name == 'nan':
        return True, 'empty'
    name_lower = name.strip().lower()

    if name_lower in GARBAGE_EXACT:
        return True, f'exact_garbage:{name}'

    words = name_lower.split()

    if words[0] in VERB_PREFIXES and len(words) >= 2:
        first_word_title = name.strip().split()[0].lower().replace("'","").replace("-","")
        if first_word_title not in enricher.COMMON_UK_FIRST_NAMES:
            return True, f'verb_prefix:{name}'

    for w in words:
        if w in GARBAGE_WORDS:
            first_word = words[0].replace("'","").replace("-","")
            if first_word in enricher.COMMON_UK_FIRST_NAMES:
                if len(words) == 2 and w == words[1]:
                    return True, f'name_plus_garbage:{name}'
                continue
            return True, f'garbage_word:{w}:{name}'

    if words[0] == words[-1] and len(words) == 2:
        return True, f'duplicate_word:{name}'

    alpha_only = re.sub(r'[^a-z]', '', name_lower)
    vowels = sum(1 for c in alpha_only if c in 'aeiouy')
    if len(alpha_only) >= 4 and vowels / len(alpha_only) < 0.15:
        return True, f'gibberish:{name}'

    if re.search(r"'s\s", name) and len(words) > 2:
        return True, f'possessive_phrase:{name}'

    if name.startswith("I'm ") or name.startswith("I am "):
        return True, f'first_person:{name}'

    if not enricher._is_valid_contact_name(name):
        first_word = words[0].replace("'","").replace("-","").lower()
        title_prefixes = {'dr', 'dr.', 'mr', 'mrs', 'ms', 'miss', 'prof'}
        if first_word in title_prefixes and len(words) == 2:
            return False, ''
        if first_word not in enricher.COMMON_UK_FIRST_NAMES:
            return True, f'failed_validation:{name}'
        if len(words) == 2 and words[1] in SERVICE_SUFFIXES:
            return True, f'name_plus_service:{name}'

    return False, ''


PLACE_WORDS = {
    'cottage', 'barn', 'manor', 'hall', 'lodge', 'court', 'villa',
    'tower', 'gate', 'bridge', 'field', 'mead', 'grange', 'heath',
    'abbey', 'priory', 'mill', 'forge', 'garden', 'park', 'wood',
}


def extract_sole_trader_name(company_name, enricher):
    if not company_name:
        return None

    cn = company_name.strip()
    cn_lower = cn.lower()

    for mw in MULTI_PRACTITIONER_WORDS:
        cn_words = cn_lower.split()
        if mw in cn_words:
            return None

    cn_clean = re.sub(r'\s*[-–—]\s*.+$', '', cn)
    cn_clean = re.sub(r'\s*\(.+\)$', '', cn_clean)
    cn_clean = re.sub(r',\s*.+$', '', cn_clean)

    qualification_suffixes = {'do', 'bsc', 'msc', 'phd', 'frcs', 'mbbs', 'mrcgp',
                               'mcsp', 'hcpc', 'mbacp', 'pgdip', 'ba', 'ma', 'dosth'}

    words = cn_clean.split()
    name_words = []
    for w in words:
        w_lower = w.lower().rstrip('.,')
        if w_lower in SERVICE_SUFFIXES:
            break
        if w_lower in qualification_suffixes:
            break
        if w_lower in {'the', 'and', 'of', 'with', 'at', 'in', 'for', 'by'}:
            break
        if w_lower in {'ltd', 'limited', 'llp', 'uk', 'plc', 'inc'}:
            break
        if w_lower in PLACE_WORDS:
            return None
        if w_lower in MULTI_PRACTITIONER_WORDS:
            return None
        non_name_words = {
            'specialist', 'senior', 'advanced', 'registered', 'private',
            'professional', 'qualified', 'certified', 'independent',
            'clinical', 'speech', 'language', 'occupational', 'mental',
            'natural', 'alternative', 'traditional', 'chinese', 'thai',
            'sports', 'remedial', 'deep', 'tissue', 'hot', 'stone',
        }
        if w_lower in non_name_words:
            break
        title_prefixes = {'dr', 'dr.', 'mr', 'mrs', 'ms', 'miss', 'prof'}
        if w_lower in title_prefixes:
            name_words.append(w)
            continue
        if not w[0].isupper() and w_lower not in {'de', 'la', 'le', 'von', 'van', 'mc', 'mac'}:
            break
        name_words.append(w)

    if len(name_words) < 2 or len(name_words) > 4:
        return None

    first = name_words[0].lower().replace("'","").replace("-","")
    if first in title_prefixes:
        if len(name_words) >= 2:
            candidate = ' '.join(name_words)
            return candidate
        return None

    check_name = first
    if check_name not in enricher.COMMON_UK_FIRST_NAMES:
        return None

    last_word = name_words[-1].lower()
    if last_word in PLACE_WORDS or last_word in GARBAGE_WORDS:
        return None

    candidate = ' '.join(name_words)
    if enricher._is_valid_contact_name(candidate):
        return candidate

    core_words = [w for w in name_words if w.lower() not in {'de', 'la', 'le', 'von', 'van', 'do'}]
    if len(core_words) >= 2 and all(len(w) >= 2 for w in core_words):
        if check_name in enricher.COMMON_UK_FIRST_NAMES:
            return candidate

    if len(name_words) >= 2:
        non_initial = [w for w in name_words if len(w) > 1]
        initials = [w for w in name_words if len(w) == 1 and w.isupper()]
        if len(non_initial) >= 2 and len(initials) <= 1:
            if check_name in enricher.COMMON_UK_FIRST_NAMES:
                return candidate

    return None


def scrape_for_email(enricher, lead):
    notes = []
    if not lead.website or not lead.website.strip():
        return lead, notes

    is_social = any(d in lead.website.lower() for d in
        ['facebook.com', 'instagram.com', 'twitter.com'])
    if is_social:
        notes.append('email_rescrape_skipped:social_only')
        return lead, notes

    try:
        web_result = enricher._enrich_from_website(lead)
    except Exception as e:
        notes.append(f'email_rescrape_error:{str(e)[:60]}')
        return lead, notes

    found_email = web_result.get('email', '')
    web_generic = web_result.get('generic_email', '')

    if found_email and '@' in found_email:
        cleaned = clean_email(found_email)
        if cleaned and cleaned != lead.email:
            old_email = lead.email
            if lead.email and lead.email_guessed == 'true':
                existing = lead.personal_email_guesses or ''
                if lead.email not in existing:
                    lead.personal_email_guesses = f"{existing}; {lead.email}".strip('; ')
            lead.email = cleaned
            lead.email_guessed = "false"
            lead.email_source = "website_verified"
            notes.append(f'email_replaced:{old_email}->{cleaned}')
            return lead, notes

    if web_generic and '@' in web_generic:
        cleaned = clean_email(web_generic)
        if cleaned:
            lead.generic_email = cleaned
            notes.append(f'generic_email_found:{cleaned}')

    notes.append('email_guess_retained_no_real_found')
    return lead, notes


def openai_validate_name(enricher, lead, notes):
    if not enricher.openai_api_key:
        actual_key = os.environ.get("OPENAI_API_KEY", "")
        if actual_key:
            enricher.openai_api_key = actual_key

    if not enricher.openai_api_key:
        notes.append('openai_validation_skipped:no_key')
        return lead, notes

    if not enricher.cost_tracker.can_make_call():
        notes.append('openai_validation_skipped:budget_exhausted')
        return lead, notes

    name = (lead.contact_name or '').strip()
    if not name:
        return lead, notes

    prompt = f"""Is "{name}" a real human person's name?

Context:
- Company: {lead.company_name}
- Sector: {lead.sector or 'wellness/health'}
- This name was extracted from a UK business listing/website

Rules:
- A real person name has a first name and surname (e.g. "Sarah Jones", "Dr James Wilson")
- NOT real: UI text, page sections, service names, place names, business names
- NOT real: role titles alone (e.g. "The Therapist", "Head Clinician")
- NOT real: gibberish or random text
- Borderline foreign names that look plausible should be CONFIRMED

Reply with EXACTLY one word:
CONFIRMED
REJECTED
UNCERTAIN"""

    try:
        from openai import OpenAI
        client = OpenAI(api_key=enricher.openai_api_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You validate whether text is a real human name. Reply with exactly one word: CONFIRMED, REJECTED, or UNCERTAIN."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=10,
            temperature=0
        )
        enricher.cost_tracker.record_usage(response.usage.total_tokens if response.usage else 100)
        answer = response.choices[0].message.content.strip().upper()

        if 'CONFIRMED' in answer:
            lead.contact_verified = "true"
            notes.append(f'openai_name_validation:confirmed:{name}')
        elif 'REJECTED' in answer:
            notes.append(f'openai_name_validation:rejected:{name}')
            lead.contact_name = ""
            lead.contact_source = ""
            lead.contact_verified = "false"
        else:
            try:
                cs = float(lead.confidence_score) if lead.confidence_score else 2
                lead.confidence_score = max(1, cs - 1)
            except (ValueError, TypeError):
                lead.confidence_score = 1
            notes.append(f'openai_name_validation:uncertain:{name}')

    except Exception as e:
        notes.append(f'openai_validation_error:{str(e)[:60]}')

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
        return None, ['timeout_exceeded']
    if container['error'] is not None:
        return None, [f'error:{str(container["error"])[:60]}']
    return container['result'], []


def analyse_dataset(leads, enricher):
    garbage_names = []
    sole_trader_candidates = []
    guessed_emails = []
    openai_validation_candidates = []

    for idx, lead in enumerate(leads):
        name = (lead.contact_name or '').strip()

        if name and name != 'nan':
            is_garbage, reason = is_garbage_name(name, enricher)
            if is_garbage and reason != 'empty':
                garbage_names.append((idx, reason))

        if not name or name == 'nan':
            notes = (lead.refinement_notes or '').lower()
            already_rejected = ('final_human_filter' in notes or 'openai_name_validation:rejected' in notes)
            if not already_rejected:
                inferred = extract_sole_trader_name(lead.company_name, enricher)
                if inferred:
                    sole_trader_candidates.append((idx, inferred))

        if lead.email_guessed == 'true' and lead.email and lead.website:
            guessed_emails.append((idx, lead.email))

        if name and name != 'nan':
            try:
                conf = float(lead.confidence_score) if lead.confidence_score else None
            except (ValueError, TypeError):
                conf = None
            contact_src = (getattr(lead, 'contact_source', '') or '').lower()
            is_garbage, _ = is_garbage_name(name, enricher)
            if not is_garbage:
                needs_validation = False
                if conf is not None and conf <= 3 and 'companies_house' not in contact_src:
                    needs_validation = True
                if not enricher._is_valid_contact_name(name) and not is_garbage:
                    needs_validation = True
                if needs_validation:
                    openai_validation_candidates.append((idx, name, conf or 2))

    return garbage_names, sole_trader_candidates, guessed_emails, openai_validation_candidates


def main():
    parser = argparse.ArgumentParser(description='Final data sanitisation')
    parser.add_argument('--stats', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--run', action='store_true')
    parser.add_argument('--limit', type=int, default=0)
    parser.add_argument('--skip-openai', action='store_true')
    parser.add_argument('--skip-rescrape', action='store_true')
    args = parser.parse_args()

    if not any([args.stats, args.dry_run, args.run]):
        parser.print_help()
        return

    leads = load_leads_from_csv(INPUT_FILE)
    print(f"Loaded {len(leads)} leads from {INPUT_FILE}")

    enricher = SanitiseEnricher()

    garbage, sole_traders, guessed, openai_candidates = analyse_dataset(leads, enricher)

    total_with_name = sum(1 for l in leads
                         if (l.contact_name or '').strip() and l.contact_name != 'nan')

    if args.stats or args.dry_run:
        print(f"\n{'='*60}")
        print("FINAL SANITISATION ANALYSIS")
        print(f"{'='*60}")
        print(f"  Total leads:            {len(leads)}")
        print(f"  With contact_name:      {total_with_name}")
        print()
        print(f"  STEP 1 — Garbage names to clear:   {len(garbage)}")
        by_type = {}
        for _, reason in garbage:
            rtype = reason.split(':')[0]
            by_type[rtype] = by_type.get(rtype, 0) + 1
        for k, v in sorted(by_type.items(), key=lambda x: -x[1]):
            print(f"    {k}: {v}")
        print()
        print(f"  STEP 2 — Sole trader inferences:    {len(sole_traders)}")
        print()
        print(f"  STEP 3 — Guessed emails to check:   {len(guessed)}")
        print()
        print(f"  STEP 4 — OpenAI validation targets:  {len(openai_candidates)}")
        print()

        projected_remaining = total_with_name - len(garbage) + len(sole_traders)
        pct = projected_remaining / len(leads) * 100 if leads else 0
        print(f"  Projected coverage after cleanup:  {projected_remaining}/{len(leads)} ({pct:.1f}%)")

    if args.dry_run:
        print(f"\n{'='*60}")
        print("DRY RUN — Details")
        print(f"{'='*60}")

        print(f"\n  Garbage names ({len(garbage)}):")
        for idx, reason in garbage:
            lead = leads[idx]
            print(f"    {idx:>5}. [{reason.split(':')[0]:<20}] '{lead.contact_name}' — {lead.company_name[:50]}")

        print(f"\n  Sole trader inferences ({len(sole_traders)}):")
        for idx, inferred_name in sole_traders:
            lead = leads[idx]
            print(f"    {idx:>5}. '{inferred_name}' <- {lead.company_name[:50]}")

        print(f"\n  Guessed emails ({len(guessed)}):")
        for idx, email in guessed:
            lead = leads[idx]
            print(f"    {idx:>5}. '{email}' — {lead.company_name[:50]}")

        print(f"\n  OpenAI validation targets ({len(openai_candidates)}):")
        for idx, name, conf in openai_candidates:
            lead = leads[idx]
            print(f"    {idx:>5}. [{conf}] '{name}' — {lead.company_name[:50]}")

        return

    if not args.run:
        return

    print(f"\n{'='*60}")
    print("FINAL SANITISATION — EXECUTING")
    print(f"{'='*60}")

    stats = {
        'garbage_cleared': 0,
        'sole_trader_inferred': 0,
        'emails_replaced': 0,
        'emails_retained': 0,
        'openai_confirmed': 0,
        'openai_rejected': 0,
        'openai_uncertain': 0,
        'errors': 0,
        'timeouts': 0,
    }
    start_time = time.time()

    print(f"\n--- STEP 1: Final Human Name Filter ({len(garbage)} targets) ---")
    for idx, reason in garbage:
        lead = leads[idx]
        old_name = lead.contact_name
        lead.contact_name = ""
        lead.contact_source = ""
        lead.contact_verified = "false"
        existing_notes = lead.refinement_notes or ''
        lead.refinement_notes = f"{existing_notes}; cleanup:final_human_filter:{reason}".strip('; ')
        stats['garbage_cleared'] += 1
        print(f"  Cleared: '{old_name}' — {lead.company_name[:50]}")

    save_leads_to_csv(leads, OUTPUT_FILE, 'w')
    print(f"  [checkpoint saved after step 1]")

    print(f"\n--- STEP 2: Sole Trader Name Inference ({len(sole_traders)} targets) ---")
    for idx, inferred_name in sole_traders:
        lead = leads[idx]
        lead.contact_name = normalize_name(inferred_name)
        lead.contact_source = "sole_trader_inference"
        lead.contact_verified = "false"
        lead.confidence_score = 3
        existing_notes = lead.refinement_notes or ''
        lead.refinement_notes = f"{existing_notes}; contact_inferred_from_company_name:{inferred_name}".strip('; ')
        stats['sole_trader_inferred'] += 1
        print(f"  Inferred: '{inferred_name}' <- {lead.company_name[:50]}")

    save_leads_to_csv(leads, OUTPUT_FILE, 'w')
    print(f"  [checkpoint saved after step 2]")

    if not args.skip_rescrape:
        email_targets = guessed
        if args.limit and args.limit < len(email_targets):
            email_targets = email_targets[:args.limit]

        print(f"\n--- STEP 3: Email Re-validation ({len(email_targets)} targets) ---")
        for count, (idx, old_email) in enumerate(email_targets):
            lead = leads[idx]
            print(f"  [{count+1}/{len(email_targets)}] {lead.company_name[:50]}")

            result_tuple, timeout_notes = run_with_timeout(
                scrape_for_email, (enricher, lead), HARD_TIMEOUT_SECONDS
            )
            if timeout_notes:
                stats['timeouts'] += 1
                existing_notes = lead.refinement_notes or ''
                lead.refinement_notes = f"{existing_notes}; email_rescrape_timeout".strip('; ')
            elif result_tuple is not None:
                lead, scrape_notes = result_tuple
                if any('email_replaced' in n for n in scrape_notes):
                    stats['emails_replaced'] += 1
                    print(f"    Email replaced!")
                else:
                    stats['emails_retained'] += 1
                existing_notes = lead.refinement_notes or ''
                lead.refinement_notes = f"{existing_notes}; {'; '.join(scrape_notes)}".strip('; ')
            else:
                stats['errors'] += 1
    else:
        print(f"\n--- STEP 3: Email Re-validation SKIPPED ---")
        stats['emails_retained'] = len(guessed)

    save_leads_to_csv(leads, OUTPUT_FILE, 'w')
    print(f"  [checkpoint saved after step 3]")

    if not args.skip_openai:
        ov_targets = openai_candidates
        if args.limit and args.limit < len(ov_targets):
            ov_targets = ov_targets[:args.limit]

        print(f"\n--- STEP 4: OpenAI Name Validation ({len(ov_targets)} targets) ---")
        for count, (idx, name, conf) in enumerate(ov_targets):
            lead = leads[idx]
            print(f"  [{count+1}/{len(ov_targets)}] '{name}' — {lead.company_name[:50]}")

            notes = []
            lead, notes = openai_validate_name(enricher, lead, notes)

            if any('confirmed' in n for n in notes):
                stats['openai_confirmed'] += 1
                print(f"    CONFIRMED")
            elif any('rejected' in n for n in notes):
                stats['openai_rejected'] += 1
                print(f"    REJECTED -> cleared")
            elif any('uncertain' in n for n in notes):
                stats['openai_uncertain'] += 1
                print(f"    UNCERTAIN -> kept, lower confidence")
            else:
                stats['errors'] += 1

            existing_notes = lead.refinement_notes or ''
            lead.refinement_notes = f"{existing_notes}; {'; '.join(notes)}".strip('; ')

            if (count + 1) % 10 == 0:
                print(f"  --- Progress: {count+1}/{len(ov_targets)} ---")
    else:
        print(f"\n--- STEP 4: OpenAI Name Validation SKIPPED ---")

    for lead in leads:
        if lead.contact_name and lead.contact_name.strip() and lead.contact_name != 'nan':
            try:
                lead.email_type = enricher._classify_email_type(lead)
            except Exception:
                pass

    save_leads_to_csv(leads, OUTPUT_FILE, 'w')
    print(f"\nSaved {len(leads)} leads to {OUTPUT_FILE}")

    missing = [l for l in leads if not l.contact_name or not l.contact_name.strip() or l.contact_name == 'nan']
    save_leads_to_csv(missing, MISSING_NAME_FILE, 'w')
    print(f"Saved {len(missing)} missing-name leads to {MISSING_NAME_FILE}")

    final_with_name = sum(1 for l in leads
                         if (l.contact_name or '').strip() and l.contact_name != 'nan')
    verified = sum(1 for l in leads
                   if (l.contact_verified or '') == 'true'
                   and (l.contact_name or '').strip() and l.contact_name != 'nan')
    inferred = sum(1 for l in leads
                   if (l.contact_source or '') == 'sole_trader_inference')
    duration = time.time() - start_time

    print(f"\n{'='*60}")
    print("SANITISATION COMPLETE")
    print(f"{'='*60}")
    print(f"  Duration:               {duration/60:.1f} minutes ({duration:.0f}s)")
    print(f"  Garbage names cleared:  {stats['garbage_cleared']}")
    print(f"  Sole traders inferred:  {stats['sole_trader_inferred']}")
    print(f"  Emails replaced:        {stats['emails_replaced']}")
    print(f"  Emails retained:        {stats['emails_retained']}")
    print(f"  OpenAI confirmed:       {stats['openai_confirmed']}")
    print(f"  OpenAI rejected:        {stats['openai_rejected']}")
    print(f"  OpenAI uncertain:       {stats['openai_uncertain']}")
    print(f"  Timeouts:               {stats['timeouts']}")
    print(f"  Errors:                 {stats['errors']}")
    print()
    print(f"  FINAL METRICS:")
    print(f"    Contacts:   {final_with_name}/{len(leads)} ({final_with_name/len(leads)*100:.1f}%)")
    print(f"    Verified:   {verified}")
    print(f"    Inferred:   {inferred}")
    print(f"    Missing:    {len(missing)}")


if __name__ == '__main__':
    main()
