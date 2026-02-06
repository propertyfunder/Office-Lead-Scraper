#!/usr/bin/env python3
"""
Lead Data Refinement Script
Validates, cleans, and splits unit8 leads into enriched and excluded CSVs.
"""
import csv
import re
import sys
import requests
from urllib.parse import urlparse
from src.utils import generate_email_guesses, extract_domain

INPUT_FILE = "leads.csv"
ENRICHED_OUTPUT = "unit8_leads_enriched.csv"
EXCLUDED_OUTPUT = "unit8_leads_excluded.csv"

GENERIC_PREFIXES = {
    'info', 'admin', 'contact', 'hello', 'reception', 'enquiries',
    'enquiry', 'office', 'mail', 'help', 'support', 'team',
    'bookings', 'booking', 'appointments', 'clinic', 'practice',
    'surgery', 'studio', 'therapy', 'treatments', 'service',
    'services', 'general', 'sales', 'care', 'dental', 'physio',
    'health', 'wellness', 'fitness'
}

BUSINESS_WORDS = {
    'clinic', 'surgery', 'medical', 'dental', 'practice', 'centre',
    'center', 'house', 'links', 'suite', 'treatment', 'therapy',
    'therapist', 'hygienist', 'university', 'care', 'hospital',
    'pharmacy', 'assurance', 'holistic', 'therapists', 'directory',
    'services', 'service', 'village', 'community', 'street', 'road'
}

PLACEHOLDER_NAMES = {
    'first last', 'test test', 'john doe', 'jane doe', 'new title',
    'quick links', 'delivery suite', 'regional care', 'let\'s chat',
    'my healthy'
}

SOCIAL_DOMAINS = ['facebook.com', 'fb.com', 'instagram.com', 'twitter.com',
                  'tiktok.com', 'linkedin.com', 'youtube.com']


def is_valid_name(name: str) -> bool:
    if not name or len(name.strip()) < 3:
        return False

    name_clean = name.strip()
    if name_clean.lower() in PLACEHOLDER_NAMES:
        return False

    words = name_clean.split()
    if len(words) < 2:
        return False

    for word in words:
        word_lower = word.lower().rstrip('.')
        if word_lower in BUSINESS_WORDS:
            return False

    title_prefixes = {'dr', 'mr', 'mrs', 'ms', 'miss', 'prof', 'professor'}
    name_words = []
    for w in words:
        if w.lower().rstrip('.') not in title_prefixes:
            name_words.append(w)

    if len(name_words) < 1:
        return False

    for word in name_words:
        alpha = re.sub(r'[^a-zA-Z]', '', word)
        if len(alpha) < 2:
            continue
        word_vowels = sum(1 for c in alpha.lower() if c in 'aeiouy')
        if word_vowels == 0:
            return False

    alpha_only = re.sub(r'[^a-zA-Z]', '', name_clean)
    if len(alpha_only) < 4:
        return False

    if re.search(r'[^aeiouyAEIOUY\s]{5,}', alpha_only):
        return False

    if re.match(r'^(Spire|NHS|Private|Victoria|Aberdeen|Durham|Hillcroft|Lisle)\b', name_clean):
        return False

    if name_clean.lower() == 'first last':
        return False

    role_suffixes = {'physiotherapist', 'podiatry', 'osteopath', 'chiropractic',
                     'acupuncture', 'hypnotherapy', 'counselling', 'counseling',
                     'nutrition', 'pilates', 'yoga', 'massage', 'dentistry',
                     'aesthetics', 'beauty'}
    last_word = words[-1].lower()
    if last_word in role_suffixes:
        return False

    name_prefixes = {'mc', 'mac', 'van', 'von', 'de'}
    for w in name_words:
        alpha = re.sub(r'[^a-zA-Z]', '', w).lower()
        if len(alpha) < 2:
            continue
        check_alpha = alpha
        for prefix in name_prefixes:
            if alpha.startswith(prefix) and len(alpha) > len(prefix) + 1:
                check_alpha = alpha[len(prefix):]
                break
        leading_consonants = 0
        for c in check_alpha:
            if c in 'aeiouy':
                break
            leading_consonants += 1
        if leading_consonants >= 4:
            return False

    unusual_bigrams = {'wl', 'wt', 'xz', 'zp', 'gf', 'gj',
                       'kp', 'jp', 'jf', 'jm', 'mq', 'dk', 'dx', 'hk', 'pn',
                       'rl', 'rq', 'sq', 'tp', 'vg', 'wk', 'xf', 'zf'}
    unusual_trigrams = {'spj', 'spk', 'spn', 'zpk', 'etx'}
    for w in name_words:
        alpha = re.sub(r'[^a-zA-Z]', '', w).lower()
        if len(alpha) >= 2 and alpha[:2] in unusual_bigrams:
            return False
        if len(alpha) >= 3 and alpha[:3] in unusual_trigrams:
            return False

    return True


def is_facebook_url(url: str) -> bool:
    if not url:
        return False
    return any(d in url.lower() for d in ['facebook.com', 'fb.com'])


def is_social_media_url(url: str) -> bool:
    if not url:
        return False
    return any(d in url.lower() for d in SOCIAL_DOMAINS)


def check_website_works(url: str, timeout: int = 10) -> bool:
    if not url:
        return False
    try:
        resp = requests.head(url, timeout=timeout, allow_redirects=True,
                             headers={'User-Agent': 'Mozilla/5.0'})
        return resp.status_code < 400
    except:
        try:
            resp = requests.get(url, timeout=timeout, allow_redirects=True,
                                headers={'User-Agent': 'Mozilla/5.0'})
            return resp.status_code < 400
        except:
            return False


def classify_email(email: str) -> str:
    if not email or '@' not in email:
        return ''
    local = email.split('@')[0].lower()
    if local in GENERIC_PREFIXES:
        return 'generic'
    return 'personal'


def compute_data_score(row: dict) -> str:
    score = 0
    if row.get('website_verified') == 'yes':
        score += 2
    if is_valid_name(row.get('principal_name', '')):
        score += 2
    if row.get('email') and classify_email(row['email']) == 'personal':
        score += 2
    elif row.get('generic_email'):
        score += 1
    if row.get('guessed_personal_emails'):
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


def refine_leads():
    print("=" * 60)
    print("LEAD DATA REFINEMENT")
    print("=" * 60)

    with open(INPUT_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        all_leads = list(reader)

    unit8_leads = [r for r in all_leads if r.get('category') == 'unit8']
    print(f"Total leads in file: {len(all_leads)}")
    print(f"Unit 8 leads to process: {len(unit8_leads)}")

    enriched = []
    excluded = []

    for i, lead in enumerate(unit8_leads):
        if (i + 1) % 50 == 0:
            print(f"  Processing {i + 1}/{len(unit8_leads)}...")

        exclude_reasons = []

        website = lead.get('website', '').strip()
        has_website = bool(website) and not is_social_media_url(website)
        has_facebook = is_facebook_url(website)

        if has_website:
            lead['website_verified'] = 'yes'
        elif has_facebook:
            lead['website_verified'] = 'facebook'
        else:
            lead['website_verified'] = 'no'

        if not has_website and not has_facebook:
            exclude_reasons.append('no web presence')

        contact_name = lead.get('contact_name', '').strip()
        if contact_name and is_valid_name(contact_name):
            lead['principal_name'] = contact_name
        else:
            contact_names = lead.get('contact_names', '').strip()
            found_valid = False
            if contact_names:
                for cn in contact_names.split(';'):
                    cn = cn.strip()
                    if cn and is_valid_name(cn):
                        lead['principal_name'] = cn
                        found_valid = True
                        break
            if not found_valid:
                lead['principal_name'] = ''
                if not contact_name:
                    exclude_reasons.append('no contact')
                else:
                    exclude_reasons.append('fake name')

        existing_email = lead.get('email', '').strip()
        existing_generic = lead.get('generic_email', '').strip()
        existing_guesses = lead.get('personal_email_guesses', '').strip()

        if existing_email:
            email_type = classify_email(existing_email)
            if email_type == 'generic':
                if not existing_generic:
                    lead['generic_email'] = existing_email
                lead['email'] = ''
                existing_email = ''

        if not existing_generic and existing_email:
            email_type_check = classify_email(existing_email)
            if email_type_check == 'generic':
                lead['generic_email'] = existing_email
                lead['email'] = ''
                existing_email = ''

        principal = lead.get('principal_name', '').strip()
        domain = extract_domain(website) if has_website else ''

        if principal and domain and not existing_guesses:
            guesses = generate_email_guesses(principal, domain)
            if guesses:
                lead['guessed_personal_emails'] = '; '.join(guesses)
                lead['personal_email_guesses'] = '; '.join(guesses)
        elif existing_guesses:
            lead['guessed_personal_emails'] = existing_guesses

        final_email = lead.get('email', '').strip()
        final_generic = lead.get('generic_email', '').strip()
        final_guesses = lead.get('guessed_personal_emails', '').strip()

        if final_email and classify_email(final_email) == 'personal':
            if final_generic:
                lead['email_type'] = 'both'
            else:
                lead['email_type'] = 'personal'
        elif final_generic:
            if final_guesses:
                lead['email_type'] = 'both'
            else:
                lead['email_type'] = 'generic'
        elif final_guesses:
            lead['email_type'] = 'personal'
        else:
            lead['email_type'] = ''

        has_any_email = bool(final_email or final_generic or final_guesses)
        if not has_any_email:
            exclude_reasons.append('no email')

        lead['data_score'] = compute_data_score(lead)

        if exclude_reasons:
            lead['excluded_reason'] = ' | '.join(exclude_reasons)
            lead['included_in_export'] = 'no'
            excluded.append(lead)
        else:
            lead['excluded_reason'] = ''
            lead['included_in_export'] = 'yes'
            enriched.append(lead)

    enriched_fields = [
        'company_name', 'sector', 'location', 'website', 'website_verified',
        'principal_name', 'contact_name', 'contact_names', 'contact_titles',
        'generic_email', 'email', 'guessed_personal_emails', 'email_type',
        'phone', 'linkedin', 'ai_score', 'ai_reason', 'tag', 'google_rating',
        'data_score', 'category', 'place_id', 'search_town',
        'enrichment_source', 'enrichment_status', 'email_guessed',
        'contact_verified', 'multiple_contacts'
    ]

    excluded_fields = enriched_fields + ['excluded_reason', 'included_in_export']

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

    high = sum(1 for r in enriched if r.get('data_score') == 'high')
    medium = sum(1 for r in enriched if r.get('data_score') == 'medium')
    low = sum(1 for r in enriched if r.get('data_score') == 'low')
    print(f"\nData quality: {high} high, {medium} medium, {low} low")

    with_principal = sum(1 for r in enriched if r.get('principal_name'))
    with_personal = sum(1 for r in enriched if r.get('email') and classify_email(r['email']) == 'personal')
    with_generic = sum(1 for r in enriched if r.get('generic_email'))
    with_guesses = sum(1 for r in enriched if r.get('guessed_personal_emails'))
    verified_web = sum(1 for r in enriched if r.get('website_verified') == 'yes')

    print(f"\nEnriched breakdown:")
    print(f"  With principal name:    {with_principal}")
    print(f"  With personal email:    {with_personal}")
    print(f"  With generic email:     {with_generic}")
    print(f"  With email guesses:     {with_guesses}")
    print(f"  Website verified:       {verified_web}")

    reason_counts = {}
    for r in excluded:
        for reason in r.get('excluded_reason', '').split(' | '):
            reason = reason.strip()
            if reason:
                reason_counts[reason] = reason_counts.get(reason, 0) + 1

    print(f"\nExclusion reasons:")
    for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
        print(f"  {reason}: {count}")


if __name__ == "__main__":
    refine_leads()
