#!/usr/bin/env python3
"""
Lead Data Refinement Script (Production-Ready)
Validates, cleans, re-enriches weak records, and splits unit8 leads
into enriched and excluded CSVs with full validation and archiving.
"""
import csv
import re
import sys
import os
import requests
from urllib.parse import urlparse
from src.utils import generate_email_guesses, extract_domain
from src.models import BusinessLead

INPUT_FILE = "leads.csv"
ENRICHED_OUTPUT = "unit8_leads_enriched.csv"
EXCLUDED_OUTPUT = "unit8_leads_excluded.csv"

GENERIC_PREFIXES = {
    'info', 'admin', 'contact', 'hello', 'reception', 'enquiries',
    'enquiry', 'office', 'mail', 'help', 'support', 'team',
    'bookings', 'booking', 'appointments', 'clinic', 'practice',
    'surgery', 'studio', 'therapy', 'treatments', 'service',
    'services', 'general', 'sales', 'care', 'dental', 'physio',
    'health', 'wellness', 'fitness', 'clientcare'
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


def is_valid_name(name: str) -> str:
    """Returns 'valid', 'suspicious', or 'missing'."""
    if not name or len(name.strip()) < 2:
        return 'missing'

    name_clean = name.strip()
    if name_clean.lower() in PLACEHOLDER_NAMES:
        return 'missing'

    if any(c.isdigit() for c in name_clean):
        return 'missing'

    words = name_clean.split()
    if len(words) < 2:
        return 'missing'

    if len(words) > 3:
        return 'suspicious'

    for word in words:
        word_lower = word.lower().rstrip('.').rstrip("'s")
        if word_lower in BUSINESS_WORDS:
            return 'missing'

    if re.search(r"'s\s+\w", name_clean):
        return 'suspicious'

    title_prefixes = {'dr', 'mr', 'mrs', 'ms', 'miss', 'prof', 'professor'}
    name_words = [w for w in words if w.lower().rstrip('.') not in title_prefixes]

    if len(name_words) < 1:
        return 'missing'

    for word in name_words:
        alpha = re.sub(r'[^a-zA-Z]', '', word)
        if len(alpha) < 2:
            if alpha.lower() not in SHORT_VALID_NAMES:
                continue
        if len(alpha) >= 2:
            word_vowels = sum(1 for c in alpha.lower() if c in 'aeiouy')
            if word_vowels == 0:
                return 'missing'

    alpha_only = re.sub(r'[^a-zA-Z]', '', name_clean)
    if len(alpha_only) < 4:
        if not any(w.lower() in SHORT_VALID_NAMES for w in name_words):
            return 'missing'

    if re.search(r'[^aeiouyAEIOUY\s]{5,}', alpha_only):
        return 'missing'

    if re.match(r'^(Spire|NHS|Private|Victoria|Aberdeen|Durham|Hillcroft|Lisle)\b', name_clean):
        return 'missing'

    role_suffixes = {'physiotherapist', 'podiatry', 'osteopath', 'chiropractic',
                     'acupuncture', 'hypnotherapy', 'counselling', 'counseling',
                     'nutrition', 'pilates', 'yoga', 'massage', 'dentistry',
                     'aesthetics', 'beauty'}
    last_word = words[-1].lower()
    if last_word in role_suffixes:
        return 'missing'

    name_prefixes = {'mc', 'mac', 'van', 'von', 'de', "o'"}
    for w in name_words:
        alpha = re.sub(r'[^a-zA-Z]', '', w).lower()
        if len(alpha) < 2:
            continue
        check_alpha = alpha
        for prefix in name_prefixes:
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
            return 'missing'

    unusual_bigrams = {'wl', 'wt', 'xz', 'zp', 'gf', 'gj',
                       'kp', 'jp', 'jf', 'jm', 'mq', 'dk', 'dx', 'hk', 'pn',
                       'rl', 'rq', 'sq', 'tp', 'vg', 'wk', 'xf', 'zf'}
    unusual_trigrams = {'spj', 'spk', 'spn', 'zpk', 'etx'}
    for w in name_words:
        alpha = re.sub(r'[^a-zA-Z]', '', w).lower()
        if len(alpha) >= 2 and alpha[:2] in unusual_bigrams:
            return 'missing'
        if len(alpha) >= 3 and alpha[:3] in unusual_trigrams:
            return 'missing'

    return 'valid'


def is_facebook_url(url: str) -> bool:
    if not url:
        return False
    return any(d in url.lower() for d in ['facebook.com', 'fb.com'])


def is_social_media_url(url: str) -> bool:
    if not url:
        return False
    return any(d in url.lower() for d in SOCIAL_DOMAINS)


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
    elif row.get('website_verified') == 'facebook':
        score += 1
    if row.get('contact_name_validity') == 'valid':
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


def row_to_lead(row: dict) -> BusinessLead:
    return BusinessLead(
        company_name=row.get('company_name', ''),
        website=row.get('website', ''),
        sector=row.get('sector', ''),
        contact_name=row.get('contact_name', ''),
        email=row.get('email', ''),
        linkedin=row.get('linkedin', ''),
        location=row.get('location', ''),
        employee_count=row.get('employee_count', ''),
        source=row.get('source', ''),
        ai_score=row.get('ai_score', ''),
        ai_reason=row.get('ai_reason', ''),
        tag=row.get('tag', ''),
        phone=row.get('phone', ''),
        google_rating=row.get('google_rating', ''),
        place_id=row.get('place_id', ''),
        search_town=row.get('search_town', ''),
        category=row.get('category', ''),
        enrichment_source=row.get('enrichment_source', ''),
        enrichment_status=row.get('enrichment_status', ''),
        ai_enriched=row.get('ai_enriched', ''),
        email_guessed=row.get('email_guessed', ''),
        contact_verified=row.get('contact_verified', ''),
        generic_email=row.get('generic_email', ''),
        contact_names=row.get('contact_names', ''),
        personal_email_guesses=row.get('personal_email_guesses', ''),
        contact_titles=row.get('contact_titles', ''),
        multiple_contacts=row.get('multiple_contacts', ''),
    )


def lead_to_row(lead: BusinessLead) -> dict:
    return lead.to_dict()


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


def attempt_re_enrichment(weak_leads: list, max_records: int = 100) -> list:
    """Re-enrich weak/suspicious records using the enricher pipeline.
    
    Prioritizes records with fake names (fixable via website/CH) over
    records missing contact entirely. Caps at max_records to avoid timeout.
    """
    if not weak_leads:
        return weak_leads

    fake_name_leads = []
    other_weak = []
    for lead in weak_leads:
        contact = lead.get('contact_name', '').strip()
        if contact and is_valid_name(contact) != 'valid':
            fake_name_leads.append(lead)
        else:
            other_weak.append(lead)

    to_enrich = fake_name_leads[:max_records]
    remaining_slots = max(0, max_records - len(to_enrich))
    to_enrich.extend(other_weak[:remaining_slots])
    skip_leads = fake_name_leads[max_records:] + other_weak[remaining_slots:]

    print(f"  [Re-enrich] {len(to_enrich)} to re-enrich ({len(fake_name_leads)} fake names, {len(other_weak)} missing data)")
    if skip_leads:
        print(f"  [Re-enrich] {len(skip_leads)} skipped (over limit of {max_records})")

    try:
        from src.enricher import LeadEnricher
        enricher = LeadEnricher()
    except Exception as e:
        print(f"  [Re-enrich] Could not initialize enricher: {e}")
        return weak_leads

    re_enriched = []
    for i, row in enumerate(to_enrich):
        if (i + 1) % 20 == 0:
            print(f"  [Re-enrich] Processing {i + 1}/{len(to_enrich)}...")

        website = row.get('website', '').strip()
        has_website = bool(website) and not is_social_media_url(website)
        has_facebook = is_facebook_url(website)

        if not has_website and not has_facebook:
            re_enriched.append(row)
            continue

        lead = row_to_lead(row)
        if lead.contact_name and is_valid_name(lead.contact_name) != 'valid':
            lead.contact_name = ''
            lead.contact_verified = ''

        try:
            enriched_lead = enricher.enrich(lead, skip_if_complete=False)
            updated_row = lead_to_row(enriched_lead)
            protected_fields = {'ai_score', 'ai_reason', 'google_rating', 'place_id',
                                'search_town', 'category', 'tag', 'location', 'sector',
                                'company_name', 'source'}
            for key, orig_val in row.items():
                if key in protected_fields and orig_val and not updated_row.get(key):
                    updated_row[key] = orig_val
                elif key not in updated_row:
                    updated_row[key] = orig_val
            re_enriched.append(updated_row)
        except Exception as e:
            print(f"  [Re-enrich] Error for {row.get('company_name', '?')}: {e}")
            re_enriched.append(row)

    re_enriched.extend(skip_leads)
    return re_enriched


def process_lead(lead: dict) -> dict:
    """Process a single lead: validate name, classify emails, generate guesses."""

    website = lead.get('website', '').strip()
    has_website = bool(website) and not is_social_media_url(website)
    has_facebook = is_facebook_url(website)

    if has_website:
        lead['website_verified'] = 'yes'
    elif has_facebook:
        lead['website_verified'] = 'facebook'
    else:
        lead['website_verified'] = 'no'

    contact_name = lead.get('contact_name', '').strip()
    validity = is_valid_name(contact_name)

    if validity == 'valid':
        lead['principal_name'] = contact_name
        lead['contact_name_validity'] = 'valid'
    else:
        contact_names = lead.get('contact_names', '').strip()
        found_valid = False
        if contact_names:
            for cn in contact_names.replace('|', ';').split(';'):
                cn = cn.strip()
                if cn and is_valid_name(cn) == 'valid':
                    lead['principal_name'] = cn
                    lead['contact_name_validity'] = 'valid'
                    found_valid = True
                    break
        if not found_valid:
            lead['principal_name'] = ''
            if not contact_name:
                lead['contact_name_validity'] = 'missing'
            else:
                lead['contact_name_validity'] = 'suspicious'

    existing_email = lead.get('email', '').strip()
    existing_generic = lead.get('generic_email', '').strip()

    if existing_email:
        email_cls = classify_email(existing_email)
        if email_cls == 'generic':
            if not existing_generic:
                lead['generic_email'] = existing_email
            lead['email'] = ''
            existing_email = ''

    principal = lead.get('principal_name', '').strip()
    domain = extract_domain(website) if has_website else ''

    if principal and domain:
        guesses = generate_email_guesses(principal, domain)
        if guesses:
            lead['guessed_personal_emails'] = ' | '.join(guesses)
            lead['personal_email_guesses'] = ' | '.join(guesses)
    else:
        existing_guesses = lead.get('personal_email_guesses', '').strip()
        if existing_guesses:
            lead['guessed_personal_emails'] = existing_guesses.replace('; ', ' | ')

    final_email = lead.get('email', '').strip()
    final_generic = lead.get('generic_email', '').strip()
    final_guesses = lead.get('guessed_personal_emails', '').strip()

    if final_email and classify_email(final_email) == 'personal':
        lead['email_type'] = 'both' if final_generic else 'personal'
    elif final_generic:
        lead['email_type'] = 'both' if final_guesses else 'generic'
    elif final_guesses:
        lead['email_type'] = 'personal'
    else:
        lead['email_type'] = ''

    lead['data_score'] = compute_data_score(lead)

    return lead


def classify_lead(lead: dict) -> tuple:
    """Returns (is_enriched: bool, exclude_reasons: list)."""
    exclude_reasons = []

    if lead.get('website_verified') == 'no':
        exclude_reasons.append('no web presence')

    validity = lead.get('contact_name_validity', 'missing')
    if validity == 'missing':
        if lead.get('contact_name', '').strip():
            exclude_reasons.append('fake name')
        else:
            exclude_reasons.append('no contact')
    elif validity == 'suspicious':
        exclude_reasons.append('fake name')

    has_any_email = bool(
        lead.get('email', '').strip() or
        lead.get('generic_email', '').strip() or
        lead.get('guessed_personal_emails', '').strip()
    )
    if not has_any_email:
        exclude_reasons.append('no email')

    return (len(exclude_reasons) == 0, exclude_reasons)


def refine_leads(skip_re_enrich=False, re_enrich_limit=50):
    print("=" * 60)
    print("LEAD DATA REFINEMENT (Production-Ready)")
    print("=" * 60)

    with open(INPUT_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        all_leads = list(reader)

    unit8_leads = [r for r in all_leads if r.get('category') == 'unit8']
    other_leads = [r for r in all_leads if r.get('category') != 'unit8']
    print(f"Total leads in file: {len(all_leads)}")
    print(f"Unit 8 leads to process: {len(unit8_leads)}")
    print(f"Other leads (kept as-is): {len(other_leads)}")

    print(f"\nStep 1: Initial validation pass...")
    for i, lead in enumerate(unit8_leads):
        if (i + 1) % 100 == 0:
            print(f"  Processing {i + 1}/{len(unit8_leads)}...")
        process_lead(lead)

    print(f"\nStep 2: Deduplication (after validation)...")
    before_dedup = len(unit8_leads)
    unit8_leads = deduplicate_leads(unit8_leads)
    print(f"  {before_dedup} -> {len(unit8_leads)} ({before_dedup - len(unit8_leads)} duplicates removed)")

    enriched = []
    weak_leads = []
    excluded = []

    for lead in unit8_leads:
        is_good, reasons = classify_lead(lead)
        if is_good:
            enriched.append(lead)
        else:
            has_website = lead.get('website_verified') in ('yes', 'facebook')
            only_missing_contact_or_email = all(r in ('no contact', 'fake name', 'no email') for r in reasons)
            if has_website and only_missing_contact_or_email:
                weak_leads.append(lead)
            else:
                lead['excluded_reason'] = ' | '.join(reasons)
                lead['archived'] = 'TRUE'
                lead['included_in_export'] = 'no'
                excluded.append(lead)

    print(f"\n  Initial pass: {len(enriched)} enriched, {len(weak_leads)} weak (re-enrichable), {len(excluded)} excluded")

    if weak_leads and not skip_re_enrich:
        print(f"\nStep 3: Re-enrichment of {len(weak_leads)} weak records (limit: {re_enrich_limit})...")
        re_enriched = attempt_re_enrichment(weak_leads, max_records=re_enrich_limit)

        for lead in re_enriched:
            process_lead(lead)
            is_good, reasons = classify_lead(lead)
            if is_good:
                enriched.append(lead)
            else:
                lead['excluded_reason'] = ' | '.join(reasons)
                lead['archived'] = 'TRUE'
                lead['included_in_export'] = 'no'
                excluded.append(lead)

        print(f"  After re-enrichment: {len(enriched)} enriched, {len(excluded)} excluded")
    elif weak_leads and skip_re_enrich:
        print(f"\nStep 3: Skipping re-enrichment ({len(weak_leads)} weak records)")
        for lead in weak_leads:
            is_good, reasons = classify_lead(lead)
            if is_good:
                enriched.append(lead)
            else:
                lead['excluded_reason'] = ' | '.join(reasons)
                lead['archived'] = 'TRUE'
                lead['included_in_export'] = 'no'
                excluded.append(lead)
    else:
        print(f"\nStep 3: No weak records to re-enrich")

    print(f"\nStep 4: Final validation gate...")
    final_enriched = []
    for lead in enriched:
        has_name = lead.get('contact_name_validity') == 'valid'
        has_web = lead.get('website_verified') != 'no'
        has_email = bool(
            lead.get('email', '').strip() or
            lead.get('generic_email', '').strip() or
            lead.get('guessed_personal_emails', '').strip()
        )

        if has_name and has_web and has_email:
            lead['excluded_reason'] = ''
            lead['archived'] = 'FALSE'
            lead['included_in_export'] = 'yes'
            if lead['data_score'] == 'low':
                lead['data_score'] = 'medium'
            final_enriched.append(lead)
        else:
            fail_reasons = []
            if not has_name:
                fail_reasons.append('no contact')
            if not has_web:
                fail_reasons.append('no web presence')
            if not has_email:
                fail_reasons.append('no email')
            lead['excluded_reason'] = ' | '.join(fail_reasons)
            lead['archived'] = 'TRUE'
            lead['included_in_export'] = 'no'
            excluded.append(lead)

    moved = len(enriched) - len(final_enriched)
    if moved > 0:
        print(f"  Moved {moved} leads from enriched to excluded (failed final gate)")
    enriched = final_enriched

    enriched_fields = [
        'company_name', 'sector', 'location', 'website', 'website_verified',
        'principal_name', 'contact_name', 'contact_name_validity',
        'contact_names', 'contact_titles',
        'generic_email', 'email', 'guessed_personal_emails', 'email_type',
        'phone', 'linkedin', 'ai_score', 'ai_reason', 'tag', 'google_rating',
        'data_score', 'archived', 'category', 'place_id', 'search_town',
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
    validity_valid = sum(1 for r in enriched if r.get('contact_name_validity') == 'valid')

    print(f"\nEnriched breakdown:")
    print(f"  With principal name:    {with_principal}")
    print(f"  Name validity (valid):  {validity_valid}")
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
    import argparse
    parser = argparse.ArgumentParser(description='Refine lead data')
    parser.add_argument('--skip-re-enrich', action='store_true',
                        help='Skip re-enrichment of weak records (faster)')
    parser.add_argument('--re-enrich-limit', type=int, default=50,
                        help='Max records to re-enrich (default: 50)')
    args = parser.parse_args()
    refine_leads(skip_re_enrich=args.skip_re_enrich,
                 re_enrich_limit=args.re_enrich_limit)
