#!/usr/bin/env python3
import csv
import os
import re
import sys
import time
from datetime import datetime

from src.models import BusinessLead
from src.enricher import LeadEnricher
from src.utils import (
    extract_domain, guess_email, generate_email_guesses,
    clean_email, make_request, rate_limit
)

INPUT_FILE = "office_leads.csv"
OUTPUT_FILE = "office_leads.csv"
PLACES_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "")

from config import PLACES_API_DAILY_LIMIT

TEAM_PAGE_PATTERNS = [
    "/team", "/about-us", "/about", "/our-team", "/people",
    "/staff", "/who-we-are", "/meet-the-team", "/contact",
]

def load_leads():
    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: {INPUT_FILE} not found")
        sys.exit(1)
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    leads = []
    for row in rows:
        lead = BusinessLead(company_name=row.get('company_name', ''))
        for k, v in row.items():
            if hasattr(lead, k):
                setattr(lead, k, v or '')
        leads.append(lead)
    return leads, fieldnames


def save_leads(leads, fieldnames):
    from src.models import BusinessLead
    all_fields = [f.name for f in BusinessLead.__dataclass_fields__.values()]
    for f in all_fields:
        if f not in fieldnames:
            fieldnames.append(f)
    tmp = OUTPUT_FILE + '.tmp'
    with open(tmp, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for lead in leads:
            writer.writerow(lead.to_dict())
    os.replace(tmp, OUTPUT_FILE)


def has_any_email(lead):
    return bool(
        (lead.email and lead.email.strip())
        or (lead.contact_email and lead.contact_email.strip())
        or (lead.generic_email and lead.generic_email.strip())
    )


def phase2_website_discovery(leads):
    import requests as req
    if not PLACES_API_KEY:
        print("  SKIP: GOOGLE_MAPS_API_KEY not set — cannot do Places lookup")
        return 0, 0

    targets = [l for l in leads if not l.website or not l.website.strip()]
    print(f"\n{'='*60}")
    print("PHASE 2: Website Discovery (Google Places)")
    print(f"{'='*60}")
    print(f"  Records missing website: {len(targets)}")
    print(f"  Daily Places limit: {PLACES_API_DAILY_LIMIT}")

    found = 0
    places_calls = 0
    tier_counts = {1: 0, 2: 0, 3: 0}
    for i, lead in enumerate(targets, 1):
        if not lead.website or not lead.website.strip():
            pass
        else:
            continue

        if places_calls >= PLACES_API_DAILY_LIMIT:
            remaining = len(targets) - i + 1
            print(f"  [CAP] Reached {PLACES_API_DAILY_LIMIT} Places calls — skipping {remaining} remaining")
            for skip_lead in targets[i-1:]:
                skip_lead.missing_email = "true"
            break

        query = f"{lead.company_name}"
        if lead.location:
            loc_part = lead.location.split(',')[0].strip()
            query += f" {loc_part}"

        try:
            rate_limit(0.3, 0.5)
            places_calls += 1
            r = req.post(
                "https://places.googleapis.com/v1/places:searchText",
                headers={
                    "X-Goog-Api-Key": PLACES_API_KEY,
                    "X-Goog-FieldMask": "places.displayName,places.websiteUri,places.nationalPhoneNumber,places.rating,places.userRatingCount,places.formattedAddress",
                },
                json={"textQuery": query, "maxResultCount": 3},
                timeout=15,
            )
            if r.status_code != 200:
                print(f"  [{i}/{len(targets)}] {lead.company_name} -> Places API error {r.status_code}")
                continue

            data = r.json()
            place_results = data.get("places", [])

            best, tier = _find_best_place(
                lead.company_name, place_results, lead.location, lead.contact_name
            )
            if best:
                tier_counts[tier] += 1
                existing_notes = lead.refinement_notes or ""
                note = f"places_match:tier{tier}"
                lead.refinement_notes = f"{existing_notes}; {note}".strip("; ") if existing_notes else note

                website = best.get("websiteUri", "")
                if website:
                    lead.website = website
                    lead.enrichment_source = (lead.enrichment_source or "") + ";places_lookup"
                    found += 1
                    print(f"  [{i}/{len(targets)}] {lead.company_name} -> {website} [tier{tier}]")

                    phone = best.get("nationalPhoneNumber", "")
                    if phone and not lead.phone:
                        lead.phone = phone

                    rating = best.get("rating")
                    if rating:
                        rating_str = f"{rating}/5"
                        review_count = best.get("userRatingCount")
                        if review_count:
                            rating_str += f" ({review_count} reviews)"
                        lead.google_rating = rating_str
                else:
                    print(f"  [{i}/{len(targets)}] {lead.company_name} -> matched [tier{tier}] but no website")
                    lead.missing_email = "true"
            else:
                print(f"  [{i}/{len(targets)}] {lead.company_name} -> no match")
                lead.missing_email = "true"

        except Exception as e:
            print(f"  [{i}/{len(targets)}] {lead.company_name} -> ERROR: {e}")

    no_match = len(targets) - sum(tier_counts.values())
    print(f"  Website discovery: found {found} out of {len(targets)}")
    print(f"  Match breakdown:  tier1={tier_counts[1]}  tier2={tier_counts[2]}  tier3={tier_counts[3]}  no_match={no_match}")
    print(f"  Places API calls this run: {places_calls}")
    print(f"  Estimated cost: \u00a3{places_calls * 0.019:.2f} (@ \u00a30.019/call)")
    return found, places_calls


_PLACES_STOP_WORDS = {"ltd", "limited", "plc", "llp", "uk", "the", "and", "co", "group"}
_GU_RE = re.compile(r'\bGU\d{1,2}\b', re.I)


def _clean_name_words(name):
    return set(re.sub(r'[^a-z0-9\s]', '', name.lower().strip()).split()) - _PLACES_STOP_WORDS


def _address_plausible(place, lead_location):
    addr = (place.get("formattedAddress", "") or "").lower()
    if _GU_RE.search(addr):
        return True
    if lead_location:
        first_word = lead_location.split(',')[0].strip().split()
        if first_word and len(first_word[0]) > 2 and first_word[0].lower() in addr:
            return True
    return False


def _find_best_place(company_name, places, lead_location="", contact_name=""):
    """
    Three-tier Places matching. Returns (best_match_dict, tier_int) or (None, None).

    Tier 1 — word overlap ≥50% (primary, no address guard needed).
    Tier 2 — single meaningful token (≥4 chars) shared between names + plausible address.
    Tier 3 — director surname in Places display name + plausible address.
    """
    cn_words = _clean_name_words(company_name)

    # Tier 1: word overlap ≥50%
    for place in places:
        display = (place.get("displayName", {}).get("text", "") or "").lower()
        display_words = _clean_name_words(display)
        if not cn_words or not display_words:
            continue
        overlap = cn_words & display_words
        score = len(overlap) / max(len(cn_words), 1)
        if score >= 0.5:
            return place, 1

    # Tier 2: single meaningful token match + plausible address
    cn_meaningful = {w for w in cn_words if len(w) >= 4}
    if cn_meaningful:
        for place in places:
            display = (place.get("displayName", {}).get("text", "") or "").lower()
            display_words = _clean_name_words(display)
            display_meaningful = {w for w in display_words if len(w) >= 4}
            if cn_meaningful & display_meaningful and _address_plausible(place, lead_location):
                return place, 2

    # Tier 3: director surname in Places display name + plausible address
    if contact_name and contact_name.strip():
        parts = contact_name.strip().split()
        if parts:
            surname = parts[-1].lower()
            if len(surname) >= 4:
                for place in places:
                    display = (place.get("displayName", {}).get("text", "") or "").lower()
                    if surname in display and _address_plausible(place, lead_location):
                        return place, 3

    return None, None


def phase3_email_enrichment(leads):
    from bs4 import BeautifulSoup

    targets = [l for l in leads if not has_any_email(l)]
    print(f"\n{'='*60}")
    print("PHASE 3: Email Enrichment")
    print(f"{'='*60}")
    print(f"  Records needing email: {len(targets)}")

    stats = {"real": 0, "guessed": 0, "still_missing": 0, "errors": 0}

    for i, lead in enumerate(targets, 1):
        try:
            _classify_size(lead)
            domain = extract_domain(lead.website) if lead.website else ""
            director = (lead.contact_name or lead.principal_name or "").strip()

            print(f"  [{i}/{len(targets)}] {lead.company_name} ({lead.size_signal}) ", end="")

            if lead.size_signal == "larger" and lead.website:
                email_found = _enrich_larger(lead, domain, director)
            elif lead.size_signal == "established_small" and lead.website:
                email_found = _enrich_established_small(lead, domain, director)
            else:
                email_found = _enrich_small_new(lead, domain, director)

            if not has_any_email(lead) and director and domain:
                guesses = generate_email_guesses(director, domain)
                if guesses:
                    lead.email = clean_email(guesses[0])
                    lead.email_guessed = "true"
                    lead.email_type = "personal_guess"
                    lead.personal_email_guesses = "; ".join(guesses)
                    email_found = "guessed"

            if has_any_email(lead):
                if lead.email_guessed == "true":
                    stats["guessed"] += 1
                    print(f"-> guessed: {lead.email}")
                else:
                    stats["real"] += 1
                    print(f"-> found: {lead.email or lead.generic_email}")
            else:
                stats["still_missing"] += 1
                lead.missing_email = "true"
                print(f"-> no email")

            _update_enrichment_status(lead)

        except Exception as e:
            stats["errors"] += 1
            print(f"-> ERROR: {e}")

    print(f"\n  Email enrichment results:")
    print(f"    Real emails found:  {stats['real']}")
    print(f"    Emails guessed:     {stats['guessed']}")
    print(f"    Still missing:      {stats['still_missing']}")
    print(f"    Errors:             {stats['errors']}")
    return stats


def _classify_size(lead):
    if lead.size_signal and lead.website:
        return

    if not lead.website or not lead.website.strip():
        lead.size_signal = "small_new"
        return

    is_social = any(d in lead.website.lower() for d in [
        "facebook.com", "linkedin.com", "twitter.com", "instagram.com"
    ])
    if is_social:
        lead.size_signal = "small_new"
        return

    has_team = False
    try:
        response = make_request(lead.website, timeout=10)
        if response and response.status_code < 400:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, 'lxml')
            links = [a.get('href', '') for a in soup.find_all('a', href=True)]
            for link in links:
                link_lower = link.lower()
                if any(p in link_lower for p in ['/team', '/people', '/our-team', '/about-us/team',
                                                  '/meet-the-team', '/staff', '/about/team']):
                    has_team = True
                    break
            if not has_team:
                text = soup.get_text(separator=' ').lower()
                if re.search(r'(our team|meet the team|our people)', text):
                    has_team = True
    except Exception:
        pass

    if has_team:
        lead.size_signal = "larger"
        return

    doc = (lead.date_of_creation or "").strip()
    if doc:
        try:
            created = datetime.strptime(doc, "%Y-%m-%d")
            cutoff = datetime(2021, 3, 1)
            if created < cutoff:
                lead.size_signal = "established_small"
                return
        except (ValueError, TypeError):
            pass

    lead.size_signal = "small_new"


def _enrich_larger(lead, domain, director):
    from bs4 import BeautifulSoup

    try:
        response = make_request(lead.website, timeout=10)
        if not response or response.status_code >= 400:
            return None
        soup = BeautifulSoup(response.text, 'lxml')

        found_email = _extract_emails_from_soup(soup, domain)
        if found_email:
            _assign_email(lead, found_email, domain)
            return "found"

        links = [a.get('href', '') for a in soup.find_all('a', href=True)]
        team_urls = []
        base = lead.website.rstrip('/')
        for link in links:
            link_lower = link.lower()
            for pattern in TEAM_PAGE_PATTERNS:
                if pattern in link_lower:
                    if link.startswith('http'):
                        team_urls.append(link)
                    elif link.startswith('/'):
                        team_urls.append(base + link)
                    break

        for url in team_urls[:3]:
            try:
                rate_limit(0.3, 0.5)
                resp = make_request(url, timeout=10)
                if resp and resp.status_code < 400:
                    team_soup = BeautifulSoup(resp.text, 'lxml')
                    found = _extract_emails_from_soup(team_soup, domain)
                    if found:
                        _assign_email(lead, found, domain)
                        return "found"
            except Exception:
                pass

    except Exception:
        pass

    if director and domain:
        guesses = generate_email_guesses(director, domain)
        if guesses:
            lead.email = clean_email(guesses[0])
            lead.email_guessed = "true"
            lead.email_type = "personal_guess"
            lead.personal_email_guesses = "; ".join(guesses)
            return "guessed"

    return None


def _enrich_established_small(lead, domain, director):
    from bs4 import BeautifulSoup

    try:
        response = make_request(lead.website, timeout=10)
        if not response or response.status_code >= 400:
            if director and domain:
                return _guess_from_director(lead, director, domain)
            return None

        soup = BeautifulSoup(response.text, 'lxml')

        found_email = _extract_emails_from_soup(soup, domain)
        if found_email:
            _assign_email(lead, found_email, domain)
            return "found"

        for tag in soup.find_all('a', href=True):
            href = tag.get('href', '')
            if href.startswith('mailto:'):
                addr = href.replace('mailto:', '').split('?')[0].strip().lower()
                if '@' in addr and '.' in addr:
                    lead.generic_email = clean_email(addr)
                    lead.email = clean_email(addr)
                    lead.email_type = "generic"
                    return "found"

    except Exception:
        pass

    if director and domain:
        return _guess_from_director(lead, director, domain)
    return None


def _enrich_small_new(lead, domain, director):
    if director and domain:
        return _guess_from_director(lead, director, domain)
    return None


def _guess_from_director(lead, director, domain):
    guesses = generate_email_guesses(director, domain)
    if guesses:
        lead.email = clean_email(guesses[0])
        lead.email_guessed = "true"
        lead.email_type = "personal_guess"
        lead.personal_email_guesses = "; ".join(guesses)

        if not lead.generic_email:
            pass

        return "guessed"
    return None


def _extract_emails_from_soup(soup, domain):
    import re
    text = soup.get_text(separator=' ')
    email_re = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,7}')
    emails = email_re.findall(text)

    for tag in soup.find_all('a', href=True):
        href = tag.get('href', '')
        if href.startswith('mailto:'):
            addr = href.replace('mailto:', '').split('?')[0].strip()
            if '@' in addr:
                emails.append(addr)

    skip_patterns = ['example.com', 'sentry.io', 'wixpress.com', 'placeholder',
                     'email.com', 'domain.com', 'company.com', 'test.com']

    valid = []
    for e in emails:
        e = e.lower().strip().rstrip('.,;:!?')
        if any(p in e for p in skip_patterns):
            continue
        if domain and domain in e:
            valid.append(e)
        elif not domain:
            valid.append(e)

    return valid[0] if valid else None


def _assign_email(lead, email, domain):
    email = clean_email(email)
    generic_prefixes = ['info@', 'hello@', 'contact@', 'enquiries@',
                        'admin@', 'office@', 'mail@', 'support@',
                        'sales@', 'reception@', 'accounts@']

    is_generic = any(email.lower().startswith(p) for p in generic_prefixes)
    if is_generic:
        lead.generic_email = email
        if not lead.email:
            lead.email = email
        lead.email_type = "generic"
    else:
        lead.email = email
        lead.email_type = "personal"


def _update_enrichment_status(lead):
    has_real = (
        (lead.email and lead.email_guessed != "true")
        or lead.contact_email
        or lead.generic_email
    )
    has_any = bool(lead.email or lead.contact_email or lead.generic_email)
    has_name = bool(lead.contact_name and lead.contact_name != lead.company_name)

    if has_real and has_name:
        lead.enrichment_status = "complete"
    elif has_any and has_name:
        lead.enrichment_status = "guessed_email"
    elif has_any and not has_name:
        lead.enrichment_status = "missing_name"
    elif has_name and not has_any:
        lead.enrichment_status = "missing_email"
    else:
        lead.enrichment_status = "incomplete"


def phase4_geo_classify(leads):
    print(f"\n{'='*60}")
    print("PHASE 4: Geo Classification")
    print(f"{'='*60}")

    from src.geo_classifier import classify_from_website

    targets = [l for l in leads if
               not l.geo_relevance or not l.geo_relevance.strip()
               or (l.website and l.website.strip() and 'places_lookup' in (l.enrichment_source or ''))]
    print(f"  Records needing geo classification: {len(targets)}")

    classified = {"local": 0, "review": 0, "exclude": 0}
    for i, lead in enumerate(targets, 1):
        try:
            geo, geo_reason = classify_from_website(
                website=lead.website,
                location=lead.location,
                sector=lead.sector,
                generic_email=lead.generic_email,
                company_name=lead.company_name,
            )
            lead.geo_relevance = geo
            if geo_reason:
                existing = lead.refinement_notes or ""
                note = f"geo:{geo_reason}"
                if "geo:" in existing:
                    import re
                    existing = re.sub(r'geo:[^;]*', '', existing).strip('; ')
                lead.refinement_notes = f"{existing}; {note}".strip("; ") if existing else note
            classified[geo] = classified.get(geo, 0) + 1
            if i % 10 == 0:
                print(f"    Classified {i}/{len(targets)}...")
        except Exception as e:
            print(f"    [{i}] {lead.company_name} -> geo error: {e}")

    print(f"  Geo results: {classified.get('local', 0)} local, "
          f"{classified.get('review', 0)} review, "
          f"{classified.get('exclude', 0)} exclude")
    return classified


def print_summary(leads, email_stats, geo_stats, places_found, places_calls=0):
    print(f"\n{'='*60}")
    print("OFFICE ENRICHMENT COMPLETE")
    print(f"{'='*60}")

    total = len(leads)
    with_email = sum(1 for l in leads if has_any_email(l))
    with_guessed = sum(1 for l in leads if l.email_guessed == "true")
    with_real = with_email - with_guessed
    still_missing = total - with_email
    with_website = sum(1 for l in leads if l.website and l.website.strip())
    with_name = sum(1 for l in leads if l.contact_name and l.contact_name.strip() and l.contact_name != l.company_name)

    geo_local = sum(1 for l in leads if l.geo_relevance == "local")
    geo_review = sum(1 for l in leads if l.geo_relevance == "review")
    geo_exclude = sum(1 for l in leads if l.geo_relevance == "exclude")

    print(f"  CH Sweep:                   {total} records")
    print(f"  With website:               {with_website}")
    print(f"  With named contact:         {with_name}")
    print(f"  Email enrichment attempted: {email_stats.get('real', 0) + email_stats.get('guessed', 0) + email_stats.get('still_missing', 0)}")
    print(f"  Emails found (real):        {with_real}")
    print(f"  Emails guessed:             {with_guessed}")
    print(f"  Still missing email:        {still_missing}")
    print(f"  Geo classified:             {geo_local} local / {geo_review} review / {geo_exclude} exclude")
    print(f"  Places API calls this run:  {places_calls}")
    print(f"  Estimated cost:             \u00a3{places_calls * 0.019:.2f} (@ \u00a30.019/call)")
    print(f"  {OUTPUT_FILE} saved.")

    status_counts = {}
    for l in leads:
        s = l.enrichment_status or "unknown"
        status_counts[s] = status_counts.get(s, 0) + 1
    print(f"\n  Enrichment status breakdown:")
    for status, count in sorted(status_counts.items()):
        print(f"    {status}: {count}")

    size_counts = {}
    for l in leads:
        s = l.size_signal or "unclassified"
        size_counts[s] = size_counts.get(s, 0) + 1
    print(f"\n  Size signal breakdown:")
    for signal, count in sorted(size_counts.items()):
        print(f"    {signal}: {count}")


def main():
    start_time = time.time()
    print(f"{'='*60}")
    print("OFFICE POST-CH EMAIL ENRICHMENT PIPELINE")
    print(f"{'='*60}")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    leads, fieldnames = load_leads()
    print(f"  Loaded {len(leads)} leads from {INPUT_FILE}")

    already_have_email = sum(1 for l in leads if has_any_email(l))
    missing_website = sum(1 for l in leads if not l.website or not l.website.strip())
    print(f"  Already have email: {already_have_email}")
    print(f"  Missing website: {missing_website}")

    places_found, places_calls = phase2_website_discovery(leads)
    save_leads(leads, fieldnames)
    print(f"  [Checkpoint] Saved after Phase 2")

    email_stats = phase3_email_enrichment(leads)
    save_leads(leads, fieldnames)
    print(f"  [Checkpoint] Saved after Phase 3")

    geo_stats = phase4_geo_classify(leads)
    save_leads(leads, fieldnames)
    print(f"  [Checkpoint] Saved after Phase 4")

    elapsed = int(time.time() - start_time)
    mins = elapsed // 60
    secs = elapsed % 60
    print(f"\n  Duration: {mins}m {secs}s")

    print_summary(leads, email_stats, geo_stats, places_found, places_calls)


if __name__ == "__main__":
    main()
