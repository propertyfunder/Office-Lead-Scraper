#!/usr/bin/env python3
"""
Enhanced Layer 1 domain discovery with content validation — 100-lead test.
Works exclusively from test_website_discovery_v2.csv.
Never touches office_leads.csv.

Layer 1: 7 slug patterns, HEAD check, mandatory GET content validation,
         confidence scoring (high/medium/low). Only high/medium set website.
Layer 2: Google Places fallback, hard cap 30 calls.
"""
import csv
import os
import re
import sys
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, ".")
from run_office_enrichment import (
    _find_best_place,
    _load_places_daily_count,
    _save_places_daily_count,
)
from config import PLACES_API_DAILY_LIMIT

PLACES_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "")

SOURCE_FILE  = "office_leads.csv"
V1_FILE      = "test_website_discovery.csv"
TEST_FILE    = "test_website_discovery_v2.csv"
LAYER2_CAP   = 30

# ─── slug helpers ────────────────────────────────────────────────────────────

_LEGAL_RE = re.compile(
    r'\b(ltd|limited|llp|plc|& co\.?|and co\.?|group|uk|\(uk\))\b\.?',
    re.IGNORECASE,
)
_PUNC_RE  = re.compile(r"[^a-z0-9\s]")
_STOP3    = {"of", "at", "in", "the", "and", "a", "an", "or", "by", "to",
             "for", "co", "de", "la", "le"}


def _clean(name: str) -> str:
    s = _LEGAL_RE.sub("", name)
    s = _PUNC_RE.sub("", s.lower())
    return re.sub(r"\s+", " ", s).strip()


def _sig_words(cleaned: str) -> list:
    """Return significant words: len > 2 and not in stop list."""
    return [w for w in cleaned.split() if len(w) > 2 and w not in _STOP3]


def _slug_candidates(company_name: str) -> list:
    """
    Return ordered list of (pattern_num, slug, tld, full_url) tuples.
    Tries www. then non-www for each candidate.
    """
    cleaned = _clean(company_name)
    if not cleaned:
        return []

    all_words = cleaned.split()
    sig       = _sig_words(cleaned)

    candidates = []

    def _add(pattern_num, slug, tlds):
        if not slug or len(slug) < 2:
            return
        for tld in tlds:
            for prefix in (f"https://www.{slug}.{tld}", f"https://{slug}.{tld}"):
                candidates.append((pattern_num, slug, tld, prefix))

    # 1. Full name hyphenated
    slug1 = "-".join(all_words)
    _add(1, slug1, ["co.uk", "com", "uk"])

    # 2. Full name no separator
    slug2 = "".join(all_words)
    if slug2 != slug1:
        _add(2, slug2, ["co.uk", "com"])

    # 3. Significant words hyphenated
    if sig:
        slug3 = "-".join(sig)
        if slug3 not in (slug1, slug2):
            _add(3, slug3, ["co.uk", "com"])

    # 4. Significant words no separator
    if sig:
        slug4 = "".join(sig)
        if slug4 not in (slug1, slug2) and slug4 != "".join(sig[:1]):
            _add(4, slug4, ["co.uk", "com"])

    # 5. First two significant words
    if len(sig) >= 2:
        slug5 = "-".join(sig[:2])
        if slug5 not in (slug1, slug3):
            _add(5, slug5, ["co.uk", "com"])

    # 6. Initials (only if 3+ chars)
    initials = "".join(w[0] for w in sig)
    if len(initials) >= 3:
        _add(6, initials, ["co.uk", "com"])

    # 7. First significant word only (only if 6+ chars)
    if sig and len(sig[0]) >= 6:
        slug7 = sig[0]
        if slug7 not in (slug1, slug2, slug3, slug4):
            _add(7, slug7, ["co.uk", "com"])

    return candidates


# ─── HTTP helpers ─────────────────────────────────────────────────────────────

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

_PARKED_SIGNALS = [
    "domain for sale", "buy this domain", "parked by", "coming soon",
    "under construction", "godaddy", "sedoparking", "hugedomains",
    "this domain", "domain is for sale", "register this domain",
]

_GEO_SIGNALS = re.compile(
    r'\bGU\d|Surrey|Godalming|Guildford|Farnham|Woking|Haslemere\b',
    re.IGNORECASE,
)


def _head_ok(url: str) -> bool:
    """Quick HEAD check — accept status < 400."""
    try:
        r = requests.head(url, allow_redirects=False, timeout=3,
                          headers=_HEADERS)
        return r.status_code < 400
    except Exception:
        return False


def _fetch_page(url: str):
    """GET with 8s timeout. Returns (html_text, final_url) or (None, None)."""
    try:
        r = requests.get(url, allow_redirects=True, timeout=8,
                         headers=_HEADERS)
        if r.status_code >= 400:
            return None, None
        return r.text, r.url
    except Exception:
        return None, None


def _validate_content(html: str, company_name: str) -> dict:
    """
    Run all content checks. Returns dict with keys:
      name_match, parked, geo_signal, thin_content, word_count, rejection_reason
    """
    result = {
        "name_match": False,
        "parked": False,
        "geo_signal": "none",
        "thin_content": False,
        "word_count": 0,
        "rejection_reason": None,
    }

    soup = BeautifulSoup(html, "lxml")

    # Remove script/style noise
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    title   = (soup.title.string or "") if soup.title else ""
    h1_tags = " ".join(t.get_text(" ", strip=True) for t in soup.find_all("h1"))
    body    = soup.get_text(" ", strip=True)

    full_text = f"{title} {h1_tags} {body}".lower()
    word_count = len(body.split())

    result["word_count"] = word_count
    result["thin_content"] = word_count < 100

    # Parked domain check
    for sig in _PARKED_SIGNALS:
        if sig in full_text:
            result["parked"] = True
            result["rejection_reason"] = f"parked ({sig!r})"
            return result

    # Name match — any significant word in page
    sig = _sig_words(_clean(company_name))
    result["name_match"] = any(w in full_text for w in sig)
    if not result["name_match"]:
        result["rejection_reason"] = "no_name_match"
        return result

    # Location signal
    if _GEO_SIGNALS.search(full_text):
        result["geo_signal"] = "confirmed"

    return result


def _confidence(pattern_num: int, validation: dict) -> str:
    """Map pattern + validation result to high/medium/low confidence."""
    if not validation["name_match"] or validation["parked"]:
        return "reject"
    if validation["thin_content"]:
        return "low"
    if pattern_num in (6, 7):
        return "low"
    if validation["geo_signal"] == "confirmed":
        return "high"
    return "medium"


# ─── Places helper ────────────────────────────────────────────────────────────

def _places_query(query: str, company_name: str, lead_location: str = "",
                  contact_name: str = ""):
    import requests as req
    try:
        time.sleep(0.5)
        r = req.post(
            "https://places.googleapis.com/v1/places:searchText",
            headers={
                "X-Goog-Api-Key": PLACES_API_KEY,
                "X-Goog-FieldMask": (
                    "places.displayName,places.websiteUri,"
                    "places.nationalPhoneNumber,places.rating,"
                    "places.userRatingCount,places.formattedAddress"
                ),
            },
            json={"textQuery": query, "maxResultCount": 3},
            timeout=15,
        )
        if r.status_code != 200:
            return None, None
        places = r.json().get("places", [])
        return _find_best_place(company_name, places, lead_location, contact_name)
    except Exception as e:
        print(f"    [Places error] {e}")
        return None, None


# ─── CSV helpers ──────────────────────────────────────────────────────────────

def _load_csv(path: str):
    if not os.path.exists(path):
        return [], []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)
        fields = list(reader.fieldnames or [])
    return rows, fields


def _save_csv(rows: list, fieldnames: list, path: str):
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    os.replace(tmp, path)


def _src_add(src: str, tag: str) -> str:
    parts = [p for p in (src or "").split(";") if p]
    if tag not in parts:
        parts.append(tag)
    return ";".join(parts)


# ─── setup ────────────────────────────────────────────────────────────────────

def setup_test_file():
    # Names already used in v1 test
    v1_names = set()
    if os.path.exists(V1_FILE):
        with open(V1_FILE, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                v1_names.add(row.get("company_name", ""))

    # Names already in this v2 file (for resume)
    v2_rows, v2_fields = _load_csv(TEST_FILE)
    v2_names = {r["company_name"] for r in v2_rows}

    exclude_src = {"places_exhausted", "layer1_domain_inference",
                   "layer2_places", "low_confidence_domain"}

    source_fields = []
    candidates = []
    with open(SOURCE_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        source_fields = list(reader.fieldnames or [])
        for row in reader:
            name    = (row.get("company_name") or "").strip()
            website = (row.get("website") or "").strip()
            src     = row.get("enrichment_source") or ""
            if not name or website:
                continue
            if any(t in src for t in exclude_src):
                continue
            if name in v1_names or name in v2_names:
                continue
            candidates.append(row)

    total_eligible = len(candidates) + len(v2_rows)

    # Ensure domain_candidate field is in fieldnames
    base_fields = v2_fields if v2_fields else source_fields
    if "domain_candidate" not in base_fields:
        base_fields = base_fields + ["domain_candidate"]

    to_add = candidates[:max(0, 100 - len(v2_rows))]
    # Give new rows an empty domain_candidate field
    for r in to_add:
        r.setdefault("domain_candidate", "")

    all_rows = v2_rows + to_add
    _save_csv(all_rows, base_fields, TEST_FILE)
    print(f"  Eligible fresh candidates: {len(candidates) + len(v2_rows)}")
    print(f"  Test file: {len(v2_rows)} existing + {len(to_add)} new = {len(all_rows)} total")
    return base_fields, total_eligible


# ─── main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("ENHANCED LAYER 1 DOMAIN DISCOVERY TEST v2")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    if not PLACES_API_KEY:
        print("ERROR: GOOGLE_MAPS_API_KEY not set"); sys.exit(1)

    print("\n[Setup] Building test file...")
    fieldnames, total_eligible = setup_test_file()

    rows, fieldnames = _load_csv(TEST_FILE)
    rows = rows[:100]

    daily_count       = _load_places_daily_count()
    remaining_global  = max(0, PLACES_API_DAILY_LIMIT - daily_count)
    print(f"\n[Places] Daily: {daily_count}/{PLACES_API_DAILY_LIMIT} ({remaining_global} remaining)")
    print(f"[Layer 2 cap] {LAYER2_CAP} calls for this test\n")

    # ── stat counters ────────────────────────────────────────────────────────
    l1 = {
        "attempted": 0,
        "head_resolved": 0,
        "passed_validation": 0,
        "high": 0, "medium": 0, "low": 0,
        "rejected_no_name": 0,
        "rejected_parked": 0,
        "rejected_get_fail": 0,
        "pattern_hits": {i: 0 for i in range(1, 8)},
        "hits_detail": [],      # (company, slug, confidence, geo_signal)
        "low_detail": [],       # (company, domain_candidate, reason)
        "reject_detail": [],    # (company, domain, reason)
    }
    l2 = {"attempted": 0, "hits": 0, "cap_skipped": 0, "calls": 0, "hits_detail": []}

    for i, row in enumerate(rows, 1):
        company  = (row.get("company_name") or "").strip()
        location = (row.get("location") or "").strip()
        contact  = (row.get("contact_name") or "").strip()
        website  = (row.get("website") or "").strip()
        src      = row.get("enrichment_source") or ""

        # ── resume: already processed ────────────────────────────────────────
        already_done = (
            website
            or "layer1_domain_inference" in src
            or "layer2_places" in src
            or "low_confidence_domain" in src
            or "places_exhausted" in src
        )
        if already_done:
            # re-count into stats
            if "layer1_domain_inference" in src:
                l1["attempted"] += 1; l1["passed_validation"] += 1; l1["head_resolved"] += 1
                conf = row.get("domain_confidence", "medium")
                if conf in ("high", "medium", "low"):
                    l1[conf] += 1
                    pnum = int(row.get("slug_pattern", "1") or "1")
                    l1["pattern_hits"][pnum] = l1["pattern_hits"].get(pnum, 0) + 1
                geo = row.get("geo_signal", "none")
                l1["hits_detail"].append((company, website, conf, geo))
            elif "low_confidence_domain" in src:
                l1["attempted"] += 1; l1["low"] += 1; l1["passed_validation"] += 1; l1["head_resolved"] += 1
                l1["low_detail"].append((company, row.get("domain_candidate",""), "low_confidence"))
            elif "layer2_places" in src:
                l1["attempted"] += 1
                l2["attempted"] += 1; l2["hits"] += 1
                l2["hits_detail"].append((company, website))
            elif "places_exhausted" in src:
                l1["attempted"] += 1; l2["attempted"] += 1
            if i % 10 == 0:
                print(f"[{i}/100] RESUME — {company}")
            continue

        resolved   = False
        layer_tag  = ""
        conf_label = ""

        # ── LAYER 1: enhanced domain inference ───────────────────────────────
        l1["attempted"] += 1
        candidates = _slug_candidates(company)
        seen_slugs = set()  # avoid retrying same base slug twice (www/non-www)

        for (pnum, slug, tld, url) in candidates:
            slug_key = f"{slug}.{tld}"
            if slug_key in seen_slugs:
                continue

            time.sleep(0.5)  # rate limit between requests

            # HEAD check
            if not _head_ok(url):
                continue

            l1["head_resolved"] += 1
            seen_slugs.add(slug_key)  # mark as resolved so we don't double-count

            # GET for content validation
            time.sleep(0.5)
            html, final_url = _fetch_page(url)

            if html is None:
                # GET failed — reject, do not accept on HEAD alone
                if len(l1["reject_detail"]) < 20:
                    l1["reject_detail"].append((company, url, "get_request_failed"))
                l1["rejected_get_fail"] = l1.get("rejected_get_fail", 0) + 1
                continue

            val  = _validate_content(html, company)
            conf = _confidence(pnum, val)

            if conf == "reject":
                reason = val["rejection_reason"] or "unknown"
                if "no_name_match" in reason:
                    l1["rejected_no_name"] += 1
                elif "parked" in reason:
                    l1["rejected_parked"] += 1
                if len(l1["reject_detail"]) < 20:
                    l1["reject_detail"].append((company, url, reason))
                continue

            # Accepted (high/medium) or low-confidence flagged
            l1["passed_validation"] += 1
            l1["pattern_hits"][pnum] = l1["pattern_hits"].get(pnum, 0) + 1

            if conf in ("high", "medium"):
                l1[conf] += 1
                row["website"]           = final_url or url
                row["enrichment_source"] = _src_add(src, "layer1_domain_inference")
                row["domain_confidence"] = conf
                row["geo_signal"]        = val["geo_signal"]
                row["slug_pattern"]      = str(pnum)
                resolved    = True
                conf_label  = conf
                layer_tag   = f"Layer 1 {conf.upper()} [p{pnum}|{tld}|{val['geo_signal']}]"
                l1["hits_detail"].append((company, final_url or url, conf, val["geo_signal"]))

            else:  # low confidence — flag only
                l1["low"] += 1
                row["domain_candidate"]  = final_url or url
                row["enrichment_source"] = _src_add(src, "low_confidence_domain")
                row["domain_confidence"] = "low"
                row["geo_signal"]        = val["geo_signal"]
                row["slug_pattern"]      = str(pnum)
                conf_label = "low"
                layer_tag  = f"Layer 1 LOW [p{pnum}|{tld}] → domain_candidate only"
                l1["low_detail"].append((
                    company, final_url or url,
                    f"p{pnum}/{tld}/geo:{val['geo_signal']}/words:{val['word_count']}"
                ))

            break  # stop at first content-validated result regardless of confidence

        # ── LAYER 2: Places fallback ──────────────────────────────────────────
        if not resolved and "low_confidence_domain" not in (row.get("enrichment_source") or ""):
            l2["attempted"] += 1

            if l2["calls"] >= LAYER2_CAP or remaining_global <= 0:
                l2["cap_skipped"] += 1
                layer_tag = "Layer 2 CAPPED"
            else:
                loc_part = location.split(",")[0].strip() if location else ""
                query    = f"{company} {loc_part}".strip()
                best, tier = _places_query(query, company, location, contact)

                daily_count     += 1
                l2["calls"]     += 1
                remaining_global -= 1
                _save_places_daily_count(daily_count)

                if best and best.get("websiteUri"):
                    l2["hits"] += 1
                    l2["hits_detail"].append((company, best["websiteUri"]))
                    row["website"]           = best["websiteUri"]
                    row["enrichment_source"] = _src_add(src, "layer2_places")
                    resolved  = True
                    layer_tag = f"Layer 2 HIT [tier{tier}]"
                else:
                    row["enrichment_source"] = _src_add(src, "places_exhausted")
                    layer_tag = "ALL MISS → exhausted"

        # ── progress every 10 ────────────────────────────────────────────────
        if i % 10 == 0:
            conf_str = f" ({conf_label})" if conf_label else ""
            print(f"[{i}/100] {layer_tag}{conf_str} — {company}")

        # ── atomic save ──────────────────────────────────────────────────────
        _save_csv(rows, fieldnames, TEST_FILE)

    # ─── REPORT ──────────────────────────────────────────────────────────────
    l1_accepted  = l1["high"] + l1["medium"]
    l1_total_val = l1["passed_validation"]
    head_total   = l1["head_resolved"]
    rejected_total = (l1["rejected_no_name"] + l1["rejected_parked"]
                      + l1.get("rejected_get_fail", 0))
    false_pos_rate = (rejected_total / max(head_total, 1)) * 100
    total_resolved = l1_accepted + l2["hits"]

    l2_tried = l2["attempted"] - l2["cap_skipped"]
    proj_remaining  = total_eligible - 100
    hit_rate        = total_resolved / 100
    proj_total      = int(proj_remaining * hit_rate)
    proj_l1_free    = int(proj_remaining * (l1_accepted / 100))
    proj_l2_targets = proj_remaining - int(proj_remaining * (l1["high"] + l1["medium"] + l1["low"]) / 100)
    proj_l2_calls   = proj_l2_targets
    proj_l2_cost    = proj_l2_calls * 0.019
    proj_total_cost = proj_l2_cost

    sep = "=" * 60
    print(f"\n\n{sep}")
    print("=== ENHANCED DOMAIN DISCOVERY TEST REPORT (100 leads) ===")
    print(sep)

    print(f"""
LAYER 1 — Enhanced domain inference
  Leads attempted:              {l1['attempted']}
  Domains that resolved (HEAD): {head_total}
  Passed content validation:    {l1_total_val}
    — High confidence:          {l1['high']}
    — Medium confidence:        {l1['medium']}
    — Low confidence (flagged): {l1['low']}
  Rejected at content validation:{rejected_total}
    — No name match:            {l1['rejected_no_name']}
    — Parked domain:            {l1['rejected_parked']}
    — GET request failed:       {l1.get('rejected_get_fail', 0)}

  Slug pattern breakdown (of validated hits):
    Pattern 1 (full hyphenated):      {l1['pattern_hits'][1]}
    Pattern 2 (full no separator):    {l1['pattern_hits'][2]}
    Pattern 3 (sig words hyphenated): {l1['pattern_hits'][3]}
    Pattern 4 (sig words no sep):     {l1['pattern_hits'][4]}
    Pattern 5 (first two words):      {l1['pattern_hits'][5]}
    Pattern 6 (initials):             {l1['pattern_hits'][6]}
    Pattern 7 (first word only):      {l1['pattern_hits'][7]}

  False positive rate (resolved but failed validation): {rejected_total}/{head_total} ({false_pos_rate:.0f}%)

  Sample validated hits (up to 5):
    [company | slug used | confidence | geo_signal]""")
    for co, dom, conf, geo in l1["hits_detail"][:5]:
        print(f"    {co}  |  {dom}  |  {conf}  |  {geo}")

    print("  Sample rejections (up to 5):")
    for co, dom, reason in l1["reject_detail"][:5]:
        print(f"    {co}  |  {dom}  |  {reason}")

    print("  Sample low-confidence (up to 5):")
    for co, dom, reason in l1["low_detail"][:5]:
        print(f"    {co}  |  {dom}  |  {reason}")

    print(f"""
LAYER 2 — Places fallback
  Attempted:   {l2['attempted']} (of {l1['attempted'] - l1_accepted - l1['low']} Layer 1 misses)
  Cap hits:    {l2['cap_skipped']}
  Hits:        {l2['hits']} ({l2['hits'] * 100 // max(l2_tried, 1)}% of attempted)
  Misses:      {l2_tried - l2['hits']}
  API calls:   {l2['calls']}
  Cost:        £{l2['calls'] * 0.019:.2f} ({l2['calls']} calls @ £0.019)""")

    print(f"""
OVERALL
  High/medium confidence websites found: {l1_accepted + l2['hits']}/100 ({l1_accepted + l2['hits']}%)
  Low confidence candidates flagged:     {l1['low']}/100 ({l1['low']}%)
  Unresolved:                            {100 - total_resolved - l1['low']}/100
  Total cost:                            £{l2['calls'] * 0.019:.2f}""")

    print(f"""
FALSE POSITIVE ANALYSIS
  Domains that resolved (HEAD) but failed content validation: {rejected_total}
  Breakdown — no name match: {l1['rejected_no_name']}, parked: {l1['rejected_parked']}, GET failed: {l1.get('rejected_get_fail',0)}
  Estimated false positive rate if validation skipped: {false_pos_rate:.0f}%
  — i.e. {false_pos_rate:.0f}% of HEAD-resolved domains belong to unrelated businesses
  Example false positives caught:""")
    for co, dom, reason in l1["reject_detail"][:5]:
        print(f"    {co}  |  {dom}  |  {reason}")

    print(f"""
PROJECTED FULL RUN (~{proj_remaining:,} no-website leads)
  Expected high/medium confidence websites: {proj_total:,} ({int(hit_rate * 100)}% overall)
  Expected free from Layer 1:               {proj_l1_free:,}
  Expected low-confidence candidates:       {int(proj_remaining * l1['low'] / 100):,} (excluded from email gen)
  Expected Places calls needed:             {proj_l2_calls:,} (~{proj_l2_calls // 500 + 1} days at 500/day)
  Estimated Places cost:                    £{proj_l2_cost:.2f}
  Estimated total cost:                     £{proj_total_cost:.2f}""")

    # ── recommendations ──────────────────────────────────────────────────────
    print("\nRECOMMENDATIONS")
    top_patterns = sorted(
        [(v, k) for k, v in l1["pattern_hits"].items() if v > 0], reverse=True
    )
    useful = [f"p{k} ({v} hits)" for v, k in top_patterns]
    zero   = [f"p{k}" for k, v in l1["pattern_hits"].items() if v == 0]
    print(f"  Top performing slug patterns: {', '.join(useful) if useful else 'none'}")
    print(f"  Zero-hit patterns: {', '.join(zero) if zero else 'none — all contributed'}")
    if false_pos_rate > 30:
        print(f"  ⚠  High false positive rate ({false_pos_rate:.0f}%) — content validation is critical; do not skip it")
    else:
        print(f"  Content validation false positive rate: {false_pos_rate:.0f}% — acceptable")
    if l1["low"] > 5:
        print(f"  {l1['low']} low-confidence domains flagged — review before deciding whether to include in email gen")
    print(f"  Layer 2 (Places) adds {l2['hits']} websites at £{l2['calls'] * 0.019:.2f}")
    print(f"  Layer 3 (OpenAI) was NOT run in this test — v1 test showed 0% benefit")

    print(f"\nDone. Results saved to {TEST_FILE}")
    print(f"Daily Places total: {daily_count}/{PLACES_API_DAILY_LIMIT}")


if __name__ == "__main__":
    main()
