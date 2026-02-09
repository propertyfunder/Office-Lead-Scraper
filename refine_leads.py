#!/usr/bin/env python3
"""
Lead Data Refinement Script v2
- Exclusion: only if no website AND no Facebook page
- Flag-based system: name_review_needed, missing_email instead of excluding
- Multi-contact team_email_guesses
- Deduplication when contact_name matches principal_name
- Two output files: enriched (all usable/flagged) and excluded (no web presence)
"""
import csv
import re
import sys
import os
from urllib.parse import urlparse
from src.utils import generate_email_guesses, extract_domain

INPUT_FILE = "leads.csv"
ENRICHED_OUTPUT = "unit8_leads_enriched.csv"
EXCLUDED_OUTPUT = "unit8_leads_excluded.csv"
CH_ENRICHED_FILE = "unit8_leads_enriched.csv"

GENERIC_PREFIXES = {
    'info', 'admin', 'contact', 'hello', 'reception', 'enquiries',
    'enquiry', 'office', 'mail', 'help', 'support', 'team',
    'bookings', 'booking', 'appointments', 'clinic', 'practice',
    'surgery', 'studio', 'therapy', 'treatments', 'service',
    'services', 'general', 'sales', 'care', 'dental', 'physio',
    'health', 'wellness', 'fitness', 'clientcare'
}

ROLE_EMAIL_WORDS = {
    'clinic', 'assistant', 'manager', 'reception', 'admin', 'secretary',
    'accounts', 'billing', 'podiatry', 'physio', 'osteo', 'dental',
    'surgery', 'practice', 'studio', 'spa', 'salon', 'therapy',
    'treatment', 'client', 'patient', 'care', 'nurse', 'doctor',
    'head', 'lead', 'senior', 'director', 'coordinator',
    'customer', 'relations', 'reservations', 'service', 'cservice',
    'northdene', 'londonroad', 'camberley', 'godalming', 'guildford',
    'farnham', 'woking', 'haslemere', 'cranleigh', 'bramley',
    'elstead', 'milford', 'witley', 'churt', 'hindhead',
}

PERSONAL_EMAIL_DOMAINS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com',
    'me.com', 'live.com', 'btinternet.com', 'sky.com', 'virginmedia.com',
    'talktalk.net', 'aol.com', 'protonmail.com', 'mail.com', 'msn.com',
    'googlemail.com'
}

BUSINESS_WORDS = {
    'clinic', 'clinics', 'surgery', 'surgeries', 'medical', 'dental',
    'practice', 'practices', 'centre', 'centers', 'center', 'centres',
    'house', 'links', 'suite', 'treatment', 'treatments', 'therapy',
    'therapist', 'therapists', 'hygienist', 'university', 'care',
    'hospital', 'hospitals', 'pharmacy', 'assurance', 'holistic',
    'directory', 'services', 'service', 'village', 'community',
    'street', 'road', 'admin', 'podiatry', 'physiotherapist',
    'osteopath', 'chiropractic', 'acupuncture', 'hypnotherapy',
    'counselling', 'nutrition', 'pilates', 'yoga', 'massage',
    'dentistry', 'aesthetics', 'beauty', 'limited', 'ltd',
    'group', 'associates', 'solutions', 'consulting', 'consultancy',
    'foundation', 'trust', 'partnership', 'potential', 'limitless',
    'wellness', 'fitness', 'studio', 'studios', 'academy', 'institute',
    'school', 'college', 'nursery'
}

PLACEHOLDER_NAMES = {
    'first last', 'test test', 'john doe', 'jane doe', 'new title',
    'quick links', 'delivery suite', 'regional care', 'let\'s chat',
    'my healthy', 'not available', 'no name', 'unknown unknown'
}

SOCIAL_DOMAINS = ['facebook.com', 'fb.com', 'instagram.com', 'twitter.com',
                  'tiktok.com', 'linkedin.com', 'youtube.com']

SHORT_VALID_NAMES = {'ali', 'jo', 'al', 'ed', 'em', 'mo', 'bo', 'ty', 'di', 'lu', 'vi', 'aj', 'jd'}

TITLE_PREFIXES = {'dr', 'mr', 'mrs', 'ms', 'miss', 'prof', 'professor', 'rev', 'sir', 'dame', 'lord', 'lady'}

QUALIFICATION_SUFFIXES = {
    'bsc', 'msc', 'phd', 'dphil', 'frcs', 'mbbs', 'mrcgp',
    'mcsp', 'hcpc', 'mbacp', 'ukcp', 'babcp', 'pgdip',
    'diphe', 'ba', 'ma', 'hons', 'fhea', 'pgcert',
    'mphil', 'mchiro', 'dosth', 'dip', 'cert', 'accred',
    'registered', 'chartered', 'fellow'
}

JOB_TITLE_WORDS = {
    'director', 'manager', 'ceo', 'cto', 'cfo', 'coo', 'founder',
    'partner', 'associate', 'senior', 'junior', 'head', 'lead',
    'consultant', 'specialist', 'coordinator', 'officer', 'executive',
    'president', 'vice', 'chairman', 'secretary', 'treasurer',
    'supervisor', 'administrator', 'analyst', 'engineer', 'developer',
    'therapist', 'practitioner', 'clinician', 'nurse', 'doctor',
    'surgeon', 'dentist', 'hygienist', 'receptionist', 'assistant',
    'intern', 'trainee', 'apprentice', 'volunteer'
}

NAME_PREFIXES = {'mc', 'mac', 'van', 'von', 'de', "o'", 'al', 'el', 'ben', 'le', 'la', 'di'}

UNUSUAL_BIGRAMS = {'wl', 'wt', 'xz', 'zp', 'gf', 'gj',
                   'kp', 'jp', 'jf', 'jm', 'mq', 'dk', 'dx', 'hk', 'pn',
                   'rl', 'rq', 'sq', 'tp', 'vg', 'wk', 'xf', 'zf',
                   'qk', 'qx', 'qz', 'vx', 'wx', 'zx', 'bx', 'cx',
                   'fx', 'hx', 'jx', 'kx', 'mx', 'px'}

UNUSUAL_TRIGRAMS = {'spj', 'spk', 'spn', 'zpk', 'etx', 'xzp', 'qkr',
                    'bvd', 'gkl', 'jkl', 'kzp', 'pzk', 'vkd', 'wqr'}


def is_valid_name(name: str) -> str:
    """Returns 'valid', 'review', or 'invalid'."""
    if not name or len(name.strip()) < 2:
        return 'invalid'

    name_clean = name.strip()
    if name_clean.lower() in PLACEHOLDER_NAMES:
        return 'invalid'

    words = name_clean.split()
    if len(words) < 2:
        return 'invalid'

    name_words = [w for w in words
                  if w.lower().rstrip('.') not in TITLE_PREFIXES
                  and w.lower().strip('().,') not in QUALIFICATION_SUFFIXES]

    if any(c.isdigit() for c in ' '.join(name_words)):
        return 'invalid'

    if len(name_words) < 1:
        return 'invalid'

    if len(name_words) > 4:
        return 'review'

    for word in name_words:
        word_lower = word.lower().rstrip('.').rstrip("'s")
        if word_lower in BUSINESS_WORDS:
            return 'invalid'

    role_suffixes = JOB_TITLE_WORDS
    last_word = words[-1].lower()
    if last_word in role_suffixes:
        return 'review'

    if re.match(r'^(Spire|NHS|Private|Victoria|Aberdeen|Durham|Hillcroft|Lisle)\b', name_clean):
        return 'invalid'

    for word in name_words:
        alpha = re.sub(r'[^a-zA-Z]', '', word.replace("'", "").replace("-", ""))
        if len(alpha) < 2:
            if alpha.lower() not in SHORT_VALID_NAMES:
                continue
        if len(alpha) >= 2:
            vowel_count = sum(1 for c in alpha.lower() if c in 'aeiouy')
            if vowel_count == 0:
                return 'invalid'
            vowel_ratio = vowel_count / len(alpha)
            if len(alpha) >= 4 and vowel_ratio < 0.15:
                return 'review'

    alpha_only = re.sub(r'[^a-zA-Z]', '', name_clean.replace("'", "").replace("-", ""))
    if len(alpha_only) < 4:
        if not any(w.lower() in SHORT_VALID_NAMES for w in name_words):
            return 'invalid'

    if re.search(r'[^aeiouyAEIOUY\s]{6,}', alpha_only):
        return 'review'

    for w in name_words:
        alpha = re.sub(r'[^a-zA-Z]', '', w).lower()
        if len(alpha) < 2:
            continue

        check_alpha = alpha
        for prefix in NAME_PREFIXES:
            clean_prefix = prefix.replace("'", "")
            if alpha.startswith(clean_prefix) and len(alpha) > len(clean_prefix) + 1:
                check_alpha = alpha[len(clean_prefix):]
                break

        leading_consonants = 0
        for c in check_alpha:
            if c in 'aeiouy':
                break
            leading_consonants += 1
        if leading_consonants >= 4:
            return 'review'

    for w in name_words:
        alpha = re.sub(r'[^a-zA-Z]', '', w).lower()
        if len(alpha) >= 2:
            for i in range(len(alpha) - 1):
                if alpha[i:i+2] in UNUSUAL_BIGRAMS:
                    return 'review'
        if len(alpha) >= 3:
            for i in range(len(alpha) - 2):
                if alpha[i:i+3] in UNUSUAL_TRIGRAMS:
                    return 'review'

    if re.search(r'(.)\1{2,}', alpha_only.lower()):
        return 'review'

    return 'valid'


def strip_job_titles(name: str) -> str:
    if not name:
        return name
    words = name.strip().split()
    cleaned = []
    for w in words:
        w_lower = w.lower().rstrip('.,;:')
        w_stripped = w_lower.strip('()')
        if w_stripped in TITLE_PREFIXES:
            continue
        if w_stripped in JOB_TITLE_WORDS:
            continue
        if w_stripped in QUALIFICATION_SUFFIXES:
            continue
        if w_lower in {'–', '-', '|', '/'}:
            break
        cleaned.append(w)
    result = ' '.join(cleaned).strip()
    result = re.sub(r'\s*[-–|/].*$', '', result).strip()
    result = re.sub(r'\s*\(.*?\)\s*$', '', result).strip()
    return result if len(result.split()) >= 2 else name.strip()


def is_facebook_url(url: str) -> bool:
    if not url:
        return False
    return any(d in url.lower() for d in ['facebook.com', 'fb.com'])


def is_social_media_url(url: str) -> bool:
    if not url:
        return False
    return any(d in url.lower() for d in SOCIAL_DOMAINS)


def extract_facebook_url(row: dict) -> str:
    website = row.get('website', '').strip()
    if is_facebook_url(website):
        return website

    for field in ['linkedin', 'source', 'enrichment_source']:
        val = row.get(field, '').strip()
        if val and is_facebook_url(val):
            return val

    return ''


def classify_email(email: str) -> str:
    if not email or '@' not in email:
        return ''
    local = email.split('@')[0].lower()
    if local in GENERIC_PREFIXES:
        return 'generic'
    return 'personal'


def is_email_name_gibberish(email: str) -> bool:
    if not email or '@' not in email:
        return False
    local = email.split('@')[0].lower()
    if local in GENERIC_PREFIXES:
        return False
    parts = re.split(r'[._\-]', local)
    for part in parts:
        alpha = re.sub(r'[^a-z]', '', part)
        if not alpha or len(alpha) < 2:
            continue
        vowels = sum(1 for c in alpha if c in 'aeiouy')
        if vowels == 0 and len(alpha) >= 3:
            return True
        if len(alpha) >= 3 and vowels / len(alpha) < 0.2:
            return True
        if len(alpha) >= 2 and alpha[:2] in UNUSUAL_BIGRAMS:
            return True
        if len(alpha) >= 3 and alpha[:3] in UNUSUAL_TRIGRAMS:
            return True
    return False


def _local_looks_like_person_name(local: str) -> bool:
    segments = re.split(r'[._\-]', local)
    alpha_segments = [s for s in segments if re.match(r'^[a-z]{2,}$', s)]
    skip_words = GENERIC_PREFIXES | ROLE_EMAIL_WORDS | BUSINESS_WORDS
    alpha_segments = [s for s in alpha_segments if s not in skip_words]
    if len(alpha_segments) >= 2:
        return True
    return False


def email_matches_contact(email: str, contact_name: str) -> bool:
    if not email or not contact_name or '@' not in email:
        return True
    domain = email.split('@')[-1].lower()
    if domain in PERSONAL_EMAIL_DOMAINS:
        return True
    local = email.split('@')[0].lower()
    if not _local_looks_like_person_name(local):
        return True
    name_parts = [re.sub(r'[^a-z]', '', w.lower()) for w in contact_name.split() if len(w) >= 2]
    name_parts = [p for p in name_parts if p and p not in {t for t in TITLE_PREFIXES}]
    if not name_parts:
        return True
    for part in name_parts:
        if len(part) >= 3 and part in local:
            return True
    if len(name_parts) >= 2:
        first = name_parts[0]
        last = name_parts[-1]
        if first and last and first[0] == local[0:1] and last in local:
            return True
    return False


def generate_team_email_guesses(contact_names_str: str, domain: str, exclude_name: str = '') -> str:
    if not contact_names_str or not domain:
        return ''

    names = [n.strip() for n in contact_names_str.replace('|', ';').split(';') if n.strip()]
    exclude_lower = exclude_name.strip().lower() if exclude_name else ''

    all_guesses = []
    for name in names:
        if name.lower() == exclude_lower:
            continue
        validity = is_valid_name(name)
        if validity == 'invalid':
            continue
        name_clean = strip_job_titles(name)
        guesses = generate_email_guesses(name_clean, domain)
        if guesses:
            best_guess = guesses[0]
            all_guesses.append(f"{name_clean}: {best_guess}")

    return ' | '.join(all_guesses)


def deduplicate_email_guesses(contact_name: str, principal_name: str,
                               personal_guesses: str, principal_guess: str) -> tuple:
    if not contact_name or not principal_name:
        return personal_guesses, principal_guess

    if contact_name.strip().lower() == principal_name.strip().lower():
        if personal_guesses and principal_guess:
            p_emails = [e.strip() for e in personal_guesses.split('|')]
            if principal_guess.strip() in p_emails:
                return personal_guesses, ''
            return personal_guesses, ''
        elif principal_guess and not personal_guesses:
            return principal_guess, ''

    return personal_guesses, principal_guess


def compute_data_score(row: dict) -> str:
    score = 0
    if row.get('website_verified') == 'yes':
        score += 2
    elif row.get('website_verified') == 'facebook':
        score += 1

    name_validity = is_valid_name(row.get('contact_name', ''))
    if name_validity == 'valid':
        score += 2
    elif name_validity == 'review':
        score += 1

    if row.get('principal_name', '').strip():
        score += 1

    email = row.get('contact_email', '') or row.get('email', '')
    if email and classify_email(email) == 'personal':
        score += 2
    elif row.get('generic_email'):
        score += 1

    if row.get('personal_email_guesses'):
        score += 1
    if row.get('phone'):
        score += 1
    if row.get('ai_score'):
        try:
            if int(row['ai_score']) >= 7:
                score += 1
        except:
            pass

    if score >= 7:
        return 'high'
    elif score >= 4:
        return 'medium'
    return 'low'


def load_ch_data() -> dict:
    ch_data = {}
    if os.path.exists(CH_ENRICHED_FILE):
        try:
            with open(CH_ENRICHED_FILE, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = row.get('company_name', '').strip().lower()
                    pname = row.get('principal_name', '').strip()
                    pguess = row.get('principal_email_guess', '').strip()
                    if key and (pname or pguess):
                        ch_data[key] = {
                            'principal_name': pname,
                            'principal_email_guess': pguess
                        }
        except Exception as e:
            print(f"  Warning: Could not load CH data from {CH_ENRICHED_FILE}: {e}")
    return ch_data


def deduplicate_leads(leads: list) -> list:
    seen_keys = {}
    deduped = []

    for lead in leads:
        name_key = lead.get('company_name', '').strip().lower()
        website = lead.get('website', '').strip()
        domain_key = extract_domain(website).lower() if website else ''

        key_parts = [name_key]
        if domain_key:
            key_parts.append(domain_key)
        dedup_key = '|'.join(key_parts)

        if dedup_key in seen_keys:
            existing = seen_keys[dedup_key]
            existing_score = compute_data_score(existing)
            new_score = compute_data_score(lead)
            score_order = {'high': 3, 'medium': 2, 'low': 1}
            if score_order.get(new_score, 0) > score_order.get(existing_score, 0):
                idx = deduped.index(existing)
                deduped[idx] = lead
                seen_keys[dedup_key] = lead
        else:
            seen_keys[dedup_key] = lead
            deduped.append(lead)

    return deduped


def process_lead(lead: dict, ch_data: dict) -> dict:
    website = lead.get('website', '').strip()
    has_real_website = bool(website) and not is_social_media_url(website)
    has_facebook = is_facebook_url(website)

    fb_url = extract_facebook_url(lead)
    lead['facebook_url'] = fb_url

    if has_real_website:
        lead['website_verified'] = 'yes'
    elif has_facebook or fb_url:
        lead['website_verified'] = 'facebook'
    else:
        lead['website_verified'] = 'no'

    company_key = lead.get('company_name', '').strip().lower()
    if company_key in ch_data:
        ch_info = ch_data[company_key]
        if not lead.get('principal_name', '').strip():
            lead['principal_name'] = ch_info.get('principal_name', '')
        if not lead.get('principal_email_guess', '').strip():
            lead['principal_email_guess'] = ch_info.get('principal_email_guess', '')

    principal = lead.get('principal_name', '').strip()
    if principal:
        p_validity = is_valid_name(principal)
        if p_validity == 'invalid':
            lead['principal_name'] = ''
            lead['principal_email_guess'] = ''

    contact_name = lead.get('contact_name', '').strip()
    if contact_name:
        contact_name = strip_job_titles(contact_name)
        lead['contact_name'] = contact_name

    contact_validity = is_valid_name(contact_name) if contact_name else 'invalid'

    if contact_validity != 'valid':
        contact_names = lead.get('contact_names', '').strip()
        if contact_names:
            for cn in contact_names.replace('|', ';').split(';'):
                cn = cn.strip()
                if cn:
                    cn_clean = strip_job_titles(cn)
                    if is_valid_name(cn_clean) == 'valid':
                        lead['contact_name'] = cn_clean
                        contact_validity = 'valid'
                        break

    lead['name_review_needed'] = ''
    if contact_validity == 'review':
        lead['name_review_needed'] = 'True'
    elif contact_validity == 'invalid' and contact_name:
        lead['name_review_needed'] = 'True'

    domain = extract_domain(website) if has_real_website else ''

    existing_email = lead.get('email', '').strip()
    existing_generic = lead.get('generic_email', '').strip()

    if existing_email:
        email_cls = classify_email(existing_email)
        if email_cls == 'generic':
            if not existing_generic:
                lead['generic_email'] = existing_email
            lead['email'] = ''
            existing_email = ''

    if existing_email and classify_email(existing_email) == 'personal':
        contact = lead.get('contact_name', '').strip()
        if is_email_name_gibberish(existing_email):
            notes = lead.get('refinement_notes', '') or ''
            notes_list = [n.strip() for n in notes.split(';') if n.strip()]
            notes_list.append(f"gibberish_email_cleared:{existing_email}")
            lead['refinement_notes'] = '; '.join(notes_list)
            existing_email = ''
            lead['email'] = ''
        elif contact and not email_matches_contact(existing_email, contact):
            notes = lead.get('refinement_notes', '') or ''
            notes_list = [n.strip() for n in notes.split(';') if n.strip()]
            notes_list.append(f"email_contact_mismatch:{existing_email}")
            lead['refinement_notes'] = '; '.join(notes_list)
            lead['name_review_needed'] = 'True'

    lead['contact_email'] = existing_email

    if not existing_generic and domain:
        lead['generic_email'] = f"info@{domain}"

    best_contact = lead.get('contact_name', '').strip()
    if contact_validity in ('valid', 'review') and best_contact and domain:
        guesses = generate_email_guesses(best_contact, domain)
        if guesses:
            lead['personal_email_guesses'] = ' | '.join(guesses)
    elif not lead.get('personal_email_guesses', '').strip():
        if lead.get('guessed_personal_emails', '').strip():
            lead['personal_email_guesses'] = lead['guessed_personal_emails'].replace('; ', ' | ')

    contact_names_str = lead.get('contact_names', '').strip()
    if contact_names_str and domain:
        team_guesses = generate_team_email_guesses(contact_names_str, domain, exclude_name=best_contact)
        lead['team_email_guesses'] = team_guesses

    personal_guesses, principal_guess = deduplicate_email_guesses(
        lead.get('contact_name', ''),
        lead.get('principal_name', ''),
        lead.get('personal_email_guesses', ''),
        lead.get('principal_email_guess', '')
    )
    lead['personal_email_guesses'] = personal_guesses
    lead['principal_email_guess'] = principal_guess

    final_contact_email = lead.get('contact_email', '').strip()
    final_generic = lead.get('generic_email', '').strip()
    final_personal = lead.get('personal_email_guesses', '').strip()

    if final_contact_email and classify_email(final_contact_email) == 'personal':
        lead['email_type'] = 'both' if final_generic else 'personal'
    elif final_generic:
        lead['email_type'] = 'both' if final_personal else 'generic'
    elif final_personal:
        lead['email_type'] = 'personal'
    else:
        lead['email_type'] = ''

    has_any_email = bool(final_contact_email or final_generic or final_personal or
                         lead.get('principal_email_guess', '').strip() or
                         lead.get('team_email_guesses', '').strip())
    lead['missing_email'] = '' if has_any_email else 'True'

    lead['data_score'] = compute_data_score(lead)

    return lead


def refine_leads(skip_re_enrich=False, re_enrich_limit=50):
    print("=" * 60)
    print("LEAD DATA REFINEMENT v2")
    print("  Exclusion: no website AND no Facebook only")
    print("  Flags: name_review_needed, missing_email")
    print("=" * 60)

    with open(INPUT_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        all_leads = list(reader)

    unit8_leads = [r for r in all_leads if r.get('category') == 'unit8']
    other_leads = [r for r in all_leads if r.get('category') != 'unit8']
    print(f"Total leads in file: {len(all_leads)}")
    print(f"Unit 8 leads to process: {len(unit8_leads)}")
    print(f"Other leads (kept as-is): {len(other_leads)}")

    print(f"\nStep 1: Loading existing CH director data...")
    ch_data = load_ch_data()
    print(f"  Loaded {len(ch_data)} CH director records")

    print(f"\nStep 2: Deduplication...")
    before_dedup = len(unit8_leads)
    unit8_leads = deduplicate_leads(unit8_leads)
    print(f"  {before_dedup} -> {len(unit8_leads)} ({before_dedup - len(unit8_leads)} duplicates removed)")

    print(f"\nStep 3: Processing leads (validation, emails, flags)...")
    enriched = []
    excluded = []

    for i, lead in enumerate(unit8_leads):
        if (i + 1) % 200 == 0:
            print(f"  Processing {i + 1}/{len(unit8_leads)}...")

        process_lead(lead, ch_data)

        website = lead.get('website', '').strip()
        has_website = bool(website) and not is_social_media_url(website)
        has_facebook = is_facebook_url(website) or bool(lead.get('facebook_url', '').strip())

        if not has_website and not has_facebook:
            lead['excluded_reason'] = 'no website and no Facebook page'
            lead['archived'] = 'TRUE'
            existing_notes = lead.get('refinement_notes', '').strip()
            excl_tag = 'excluded:no_web_no_fb'
            if excl_tag not in existing_notes:
                lead['refinement_notes'] = f"{existing_notes}; {excl_tag}".strip('; ') if existing_notes else excl_tag
            lead['confidence_score'] = '1'
            lead['mailshot_category'] = 'do_not_email'
            excluded.append(lead)
        else:
            lead['excluded_reason'] = ''
            lead['archived'] = 'FALSE'
            enriched.append(lead)

    print(f"  Enriched: {len(enriched)}")
    print(f"  Excluded (no web presence at all): {len(excluded)}")

    print(f"\nStep 4: Summary statistics...")
    review_needed = sum(1 for r in enriched if r.get('name_review_needed') == 'True')
    missing_email = sum(1 for r in enriched if r.get('missing_email') == 'True')
    has_principal = sum(1 for r in enriched if r.get('principal_name', '').strip())
    has_contact = sum(1 for r in enriched if is_valid_name(r.get('contact_name', '')) == 'valid')
    has_team_guesses = sum(1 for r in enriched if r.get('team_email_guesses', '').strip())
    has_personal = sum(1 for r in enriched if r.get('personal_email_guesses', '').strip())
    has_generic = sum(1 for r in enriched if r.get('generic_email', '').strip())
    high = sum(1 for r in enriched if r.get('data_score') == 'high')
    medium = sum(1 for r in enriched if r.get('data_score') == 'medium')
    low = sum(1 for r in enriched if r.get('data_score') == 'low')

    print(f"  Flags: {review_needed} name_review_needed, {missing_email} missing_email")
    print(f"  Contacts: {has_contact} valid contacts, {has_principal} CH directors")
    print(f"  Emails: {has_personal} personal guesses, {has_generic} generic, {has_team_guesses} team guesses")
    print(f"  Scores: {high} high, {medium} medium, {low} low")

    enriched_fields = [
        'company_name', 'website', 'website_verified', 'facebook_url',
        'contact_name', 'contact_names', 'contact_source',
        'contact_email', 'personal_email_guesses', 'team_email_guesses',
        'principal_name', 'principal_email_guess',
        'generic_email', 'email_type', 'mailshot_category',
        'name_review_needed', 'missing_email',
        'data_score', 'confidence_score',
        'sector', 'location', 'phone', 'linkedin',
        'ai_score', 'ai_reason', 'tag', 'google_rating',
        'category', 'place_id', 'search_town',
        'enrichment_source', 'enrichment_status', 'enrichment_attempts',
        'email_guessed', 'contact_verified', 'multiple_contacts',
        'contact_titles', 'archived', 'refinement_notes', 'last_enriched_date'
    ]

    excluded_fields = enriched_fields + ['excluded_reason']

    with open(ENRICHED_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=enriched_fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(enriched)

    with open(EXCLUDED_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=excluded_fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(excluded)

    print("\n" + "=" * 60)
    print("REFINEMENT COMPLETE")
    print("=" * 60)
    print(f"\nEnriched leads: {len(enriched)} -> {ENRICHED_OUTPUT}")
    print(f"Excluded leads: {len(excluded)} -> {EXCLUDED_OUTPUT}")
    print(f"\nNext step: Run ch_enrich.py to populate principal_name/principal_email_guess")


if __name__ == "__main__":
    skip = '--skip-re-enrich' in sys.argv
    limit = 50
    for arg in sys.argv:
        if arg.startswith('--re-enrich-limit='):
            try:
                limit = int(arg.split('=')[1])
            except:
                pass
    refine_leads(skip_re_enrich=skip, re_enrich_limit=limit)
