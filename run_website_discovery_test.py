#!/usr/bin/env python3
"""
Three-layer website discovery test on 100 no-website leads.
Works exclusively from test_website_discovery.csv — never touches office_leads.csv.

Layers:
  1 — Domain inference (free, HEAD requests)
  2 — Google Places Text Search (cap: 60 calls for this test)
  3 — OpenAI trading-name normalisation → Places retry (cap: 40 OpenAI calls)

Resumable: leads already carrying a website or places_exhausted are skipped.
"""
import csv
import os
import re
import sys
import time
from datetime import datetime

sys.path.insert(0, ".")
from run_office_enrichment import (
    _find_best_place,
    _load_places_daily_count,
    _save_places_daily_count,
)
from config import PLACES_API_DAILY_LIMIT

PLACES_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

SOURCE_FILE = "office_leads.csv"
TEST_FILE   = "test_website_discovery.csv"

LAYER2_CAP = 60   # max Places calls for this test run
LAYER3_CAP = 40   # max OpenAI calls for this test run

# ─── domain inference helpers ───────────────────────────────────────────────

_LEGAL_RE = re.compile(
    r'\b(ltd|limited|llp|plc|& co\.?|and co\.?|group|uk|\(uk\))\b\.?',
    re.IGNORECASE,
)
_PUNC_RE = re.compile(r"[^a-z0-9\s-]")


def _strip_legal(name: str) -> str:
    cleaned = _LEGAL_RE.sub("", name)
    cleaned = _PUNC_RE.sub("", cleaned.lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _domain_candidates(company_name: str) -> list:
    base = _strip_legal(company_name)
    slug = base.replace(" ", "-")
    if not slug or len(slug) < 2:
        return []
    return [
        f"https://www.{slug}.co.uk",
        f"https://{slug}.co.uk",
        f"https://www.{slug}.com",
        f"https://{slug}.com",
        f"https://www.{slug}.uk",
        f"https://{slug}.uk",
    ]


def _head_resolves(url: str, timeout: int = 3):
    """Return (ok: bool, final_url: str).
    Uses allow_redirects=False to detect 301/302 quickly without following chains.
    Accepts status < 400 (200 OK, 301/302 redirects all signal a live domain).
    """
    import requests
    try:
        r = requests.head(
            url,
            allow_redirects=False,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (compatible; LeadBot/1.0)"},
        )
        if r.status_code < 400:
            # For redirects, capture where it's going as the canonical URL hint
            location = r.headers.get("Location", url)
            return True, location if r.status_code in (301, 302) else r.url
        return False, None
    except Exception:
        return False, None


# ─── Places helper ──────────────────────────────────────────────────────────

def _places_query(query: str, company_name: str, lead_location: str = "",
                  contact_name: str = ""):
    """Hit Places API, return (best_place_dict, tier) or (None, None)."""
    import requests as req
    try:
        time.sleep(0.35)
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


# ─── OpenAI helper ──────────────────────────────────────────────────────────

def _openai_trading_name(company_name: str, location: str) -> str:
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)
    prompt = (
        f"A UK company is registered at Companies House as '{company_name}' "
        f"in '{location}'. What is the most likely trading name or brand name "
        f"this business would use publicly? Return only the trading name, nothing else."
    )
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=30,
            temperature=0.2,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"    [OpenAI error] {e}")
        return ""


def _names_meaningfully_differ(ch_name: str, trading_name: str) -> bool:
    """True if trading name is clearly distinct from the CH registered name."""
    if not trading_name:
        return False
    _stop = {"ltd", "limited", "llp", "plc", "the", "and", "co", "uk", "group", "&"}
    ch_w  = set(re.sub(r"[^a-z ]", "", ch_name.lower()).split()) - _stop
    tr_w  = set(re.sub(r"[^a-z ]", "", trading_name.lower()).split()) - _stop
    if not tr_w:
        return False
    overlap = len(ch_w & tr_w) / max(len(tr_w), 1)
    return overlap < 0.5


# ─── CSV helpers ────────────────────────────────────────────────────────────

def _load_csv(path: str):
    if not os.path.exists(path):
        return [], []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
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


# ─── setup: build test_website_discovery.csv from office_leads.csv ──────────

def setup_test_file():
    existing_rows, existing_fields = _load_csv(TEST_FILE)
    existing_names = {r["company_name"] for r in existing_rows}

    source_fields = []
    candidates = []
    with open(SOURCE_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        source_fields = list(reader.fieldnames or [])
        for row in reader:
            name = (row.get("company_name") or "").strip()
            website = (row.get("website") or "").strip()
            src = row.get("enrichment_source") or ""
            if name and not website and "places_lookup" not in src:
                candidates.append(row)

    total_eligible = len(candidates)
    print(f"  Eligible candidates in {SOURCE_FILE}: {total_eligible}")

    needed = [c for c in candidates if c["company_name"] not in existing_names]
    slots = max(0, 100 - len(existing_rows))
    to_add = needed[:slots]

    fieldnames = existing_fields if existing_fields else source_fields
    all_rows = existing_rows + to_add
    _save_csv(all_rows, fieldnames, TEST_FILE)
    print(f"  Test file built: {len(existing_rows)} existing + {len(to_add)} new = {len(all_rows)} total")
    return total_eligible


# ─── main ───────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("THREE-LAYER WEBSITE DISCOVERY TEST")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    if not PLACES_API_KEY:
        print("ERROR: GOOGLE_MAPS_API_KEY not set"); sys.exit(1)
    if not OPENAI_API_KEY:
        print("ERROR: OPENAI_API_KEY not set"); sys.exit(1)

    print("\n[Setup] Preparing test file...")
    total_eligible = setup_test_file()

    rows, fieldnames = _load_csv(TEST_FILE)
    rows = rows[:100]

    daily_count = _load_places_daily_count()
    remaining_global = max(0, PLACES_API_DAILY_LIMIT - daily_count)
    print(f"\n[Places] Daily count loaded: {daily_count}/{PLACES_API_DAILY_LIMIT} ({remaining_global} remaining)")
    print(f"[Layer 2] Cap for this test: {LAYER2_CAP} calls")
    print(f"[Layer 3] Cap for this test: {LAYER3_CAP} OpenAI calls")
    print()

    # ── stats ────────────────────────────────────────────────────────────────
    l1 = {"attempted": 0, "hits": 0, "hits_detail": [], "misses_detail": []}
    l2 = {"attempted": 0, "hits": 0, "cap_skipped": 0, "calls": 0, "hits_detail": []}
    l3 = {"attempted": 0, "openai_diff": 0, "hits": 0, "calls": 0,
          "still_unresolved": 0, "samples": []}

    def _src_add(existing_src: str, tag: str) -> str:
        parts = [p for p in existing_src.split(";") if p]
        if tag not in parts:
            parts.append(tag)
        return ";".join(parts)

    for i, row in enumerate(rows, 1):
        company  = (row.get("company_name") or "").strip()
        location = (row.get("location") or "").strip()
        contact  = (row.get("contact_name") or "").strip()
        website  = (row.get("website") or "").strip()
        src      = row.get("enrichment_source") or ""

        # ── resume logic ─────────────────────────────────────────────────────
        if website or "places_exhausted" in src:
            if "layer1_domain_inference" in src:
                l1["attempted"] += 1; l1["hits"] += 1
                l1["hits_detail"].append((company, website))
            elif "layer2_places" in src:
                l1["attempted"] += 1
                l2["attempted"] += 1; l2["hits"] += 1
                l2["hits_detail"].append((company, website))
            elif "layer3_openai_places" in src:
                l1["attempted"] += 1; l2["attempted"] += 1
                l3["attempted"] += 1; l3["hits"] += 1
            elif "places_exhausted" in src:
                l1["attempted"] += 1; l2["attempted"] += 1; l3["attempted"] += 1
                l3["still_unresolved"] += 1
            if i % 10 == 0:
                status = "website" if website else "exhausted"
                print(f"[{i}/100] RESUME — {company} ({status})")
            continue

        resolved = False
        layer_result = "?"

        # ── LAYER 1: domain inference ─────────────────────────────────────────
        l1["attempted"] += 1
        candidates = _domain_candidates(company)
        hit_url = None
        for url in candidates:
            ok, final_url = _head_resolves(url)
            if ok:
                hit_url = final_url or url
                break

        if hit_url:
            l1["hits"] += 1
            l1["hits_detail"].append((company, hit_url))
            row["website"] = hit_url
            row["enrichment_source"] = _src_add(src, "layer1_domain_inference")
            resolved = True
            layer_result = f"Layer 1 HIT → {hit_url}"
        else:
            l1["misses_detail"].append((company, candidates))

        # ── LAYER 2: Google Places ────────────────────────────────────────────
        if not resolved:
            l2["attempted"] += 1

            if l2["calls"] >= LAYER2_CAP or remaining_global <= 0:
                l2["cap_skipped"] += 1
                layer_result = "Layer 2 CAPPED"
                # Not exhausted — cap-related; lead can be retried in a future run

            else:
                loc_part = location.split(",")[0].strip() if location else ""
                query = f"{company} {loc_part}".strip()
                best, tier = _places_query(query, company, location, contact)

                daily_count += 1
                l2["calls"] += 1
                remaining_global -= 1
                _save_places_daily_count(daily_count)

                if best and best.get("websiteUri"):
                    l2["hits"] += 1
                    l2["hits_detail"].append((company, best["websiteUri"]))
                    row["website"] = best["websiteUri"]
                    row["enrichment_source"] = _src_add(src, "layer2_places")
                    resolved = True
                    layer_result = f"Layer 2 HIT [tier{tier}] → {best['websiteUri']}"
                else:
                    layer_result = "Layer 2 MISS"

        # ── LAYER 3: OpenAI + Places retry ───────────────────────────────────
        # Only if Layer 2 was truly attempted (not cap-skipped) and failed
        if not resolved and l2["cap_skipped"] == 0 or (
            not resolved and l2["attempted"] - l2["cap_skipped"] > 0
            and layer_result == "Layer 2 MISS"
        ):
            if layer_result == "Layer 2 MISS":
                l3["attempted"] += 1

                if l3["calls"] >= LAYER3_CAP:
                    layer_result = "Layer 3 CAPPED"
                    # also not exhausted — cap-limited
                else:
                    trading = _openai_trading_name(company, location)
                    l3["calls"] += 1
                    if trading and len(l3["samples"]) < 20:
                        l3["samples"].append((company, trading))

                    diff = _names_meaningfully_differ(company, trading)
                    if diff:
                        l3["openai_diff"] += 1
                        if remaining_global > 0:
                            loc_part = location.split(",")[0].strip() if location else ""
                            query2 = f"{trading} {loc_part}".strip()
                            best2, tier2 = _places_query(
                                query2, trading, location, contact
                            )
                            daily_count += 1
                            remaining_global -= 1
                            _save_places_daily_count(daily_count)

                            if best2 and best2.get("websiteUri"):
                                l3["hits"] += 1
                                row["website"] = best2["websiteUri"]
                                row["enrichment_source"] = _src_add(
                                    src, "layer3_openai_places"
                                )
                                resolved = True
                                layer_result = (
                                    f"Layer 3 HIT → {best2['websiteUri']} "
                                    f"('{company}' → '{trading}')"
                                )

                    if not resolved:
                        l3["still_unresolved"] += 1
                        row["enrichment_source"] = _src_add(src, "places_exhausted")
                        layer_result = "ALL LAYERS MISS → places_exhausted"

        # ── progress every 10 leads ──────────────────────────────────────────
        if i % 10 == 0:
            print(f"[{i}/100] {layer_result} — {company}")

        # ── atomic save after every lead ─────────────────────────────────────
        _save_csv(rows, fieldnames, TEST_FILE)

    # ── REPORT ───────────────────────────────────────────────────────────────
    total_resolved   = l1["hits"] + l2["hits"] + l3["hits"]
    total_unresolved = 100 - total_resolved
    l2_cost          = l2["calls"] * 0.019
    total_cost       = l2_cost  # Layer 1 free; Layer 3 OpenAI cost negligible at <40 calls

    proj_remaining   = total_eligible - 100
    hit_rate         = total_resolved / 100

    l1_rate = l1["hits"] / max(l1["attempted"], 1)
    l2_tried = l2["attempted"] - l2["cap_skipped"]
    l2_rate  = l2["hits"] / max(l2_tried, 1)
    l3_rate  = l3["hits"] / max(l3["attempted"], 1)

    # For the full run, assume same layer proportions on remaining leads
    proj_l1_hits     = int(proj_remaining * l1_rate)
    proj_l2_targets  = proj_remaining - proj_l1_hits
    proj_l2_hits     = int(proj_l2_targets * l2_rate)
    proj_l3_targets  = proj_l2_targets - proj_l2_hits
    proj_l3_hits     = int(proj_l3_targets * l3_rate)

    proj_total_resolved  = proj_l1_hits + proj_l2_hits + proj_l3_hits
    proj_places_calls    = proj_l2_targets + int(proj_l3_targets * (l3["openai_diff"] / max(l3["attempted"], 1)))
    proj_openai_calls    = proj_l3_targets
    proj_places_cost     = proj_places_calls * 0.019
    proj_openai_cost_usd = proj_openai_calls * 0.0002
    proj_total_cost_gbp  = proj_places_cost + (proj_openai_cost_usd * 0.79)

    sep = "=" * 60
    print(f"\n\n{sep}")
    print("=== WEBSITE DISCOVERY TEST REPORT (100 leads) ===")
    print(sep)

    print(f"""
LAYER 1 — Domain inference (free)
  Attempted:   {l1['attempted']}
  Hits:        {l1['hits']} ({l1['hits'] * 100 // max(l1['attempted'], 1)}%)
  Misses:      {l1['attempted'] - l1['hits']}
  Top 5 hits:""")
    for co, dom in l1["hits_detail"][:5]:
        print(f"    {co}  |  {dom}")
    print("  Top 5 misses (first 3 candidates tried):")
    for co, cands in l1["misses_detail"][:5]:
        print(f"    {co}  |  {' / '.join(cands[:3])}")

    print(f"""
LAYER 2 — Google Places
  Attempted:   {l2['attempted']} (of {l1['attempted'] - l1['hits']} Layer 1 misses)
  Cap hits:    {l2['cap_skipped']} (capped at {LAYER2_CAP} calls — not marked exhausted)
  Hits:        {l2['hits']} ({l2['hits'] * 100 // max(l2_tried, 1)}% of actually attempted)
  Misses:      {l2_tried - l2['hits']} (of {l2_tried} actually attempted)
  API calls:   {l2['calls']}
  Cost:        £{l2_cost:.2f} ({l2['calls']} calls @ £0.019)""")

    print(f"""
LAYER 3 — OpenAI + Places retry
  Attempted:   {l3['attempted']} (of {l2_tried - l2['hits']} Layer 2 misses)
  OpenAI calls made: {l3['calls']}
  OpenAI suggested meaningfully different name: {l3['openai_diff']}
  Of those, Places then matched:               {l3['hits']} ({l3['hits'] * 100 // max(l3['openai_diff'], 1)}%)
  Still unresolved after Layer 3:              {l3['still_unresolved']}
  Sample trading name suggestions (up to 5):""")
    for ch, tr in l3["samples"][:5]:
        print(f"    {ch}  →  {tr}")

    print(f"""
OVERALL
  Resolved:    {total_resolved}/100 ({total_resolved}%)
  Unresolved:  {total_unresolved}/100 ({total_unresolved}%)
  Layer 1 share of resolved: {l1['hits'] * 100 // max(total_resolved, 1)}%
  Layer 2 share of resolved: {l2['hits'] * 100 // max(total_resolved, 1)}%
  Layer 3 share of resolved: {l3['hits'] * 100 // max(total_resolved, 1)}%
  Total cost:  £{total_cost:.2f} (Places only; OpenAI <£0.01)

PROJECTED FULL RUN (~{proj_remaining} no-website leads remaining)
  Expected resolved at current hit rate: {proj_total_resolved} ({int(hit_rate * 100)}%)
  Estimated Places calls needed:         {proj_places_calls}
  Estimated Places cost:                 £{proj_places_cost:.2f}
  Estimated OpenAI calls:                {proj_openai_calls}
  Estimated OpenAI cost:                 ${proj_openai_cost_usd:.2f}
  Estimated total cost:                  £{proj_total_cost_gbp:.2f}""")

    # ── miss pattern analysis ────────────────────────────────────────────────
    print("\n── Miss pattern analysis ──")
    miss_sectors: dict = {}
    miss_new_co = 0
    miss_names: list = []
    with open(TEST_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            src_r = row.get("enrichment_source") or ""
            w_r   = (row.get("website") or "").strip()
            if not w_r and "places_exhausted" in src_r:
                sec = (row.get("sector") or "unknown").strip()
                miss_sectors[sec] = miss_sectors.get(sec, 0) + 1
                doc = row.get("date_of_creation") or ""
                if doc >= "2022-01-01":
                    miss_new_co += 1
                miss_names.append(row.get("company_name", ""))

    if miss_sectors:
        print("  Sector breakdown of unresolved leads:")
        for sec, cnt in sorted(miss_sectors.items(), key=lambda x: -x[1])[:8]:
            print(f"    {sec}: {cnt}")
        print(f"  Incorporated after 2022 (likely no web presence yet): {miss_new_co}")
        print("  Sample unresolved company names:")
        for n in miss_names[:8]:
            print(f"    {n}")
    else:
        print("  (no fully-exhausted leads yet — run exhausted all 3 layers on 0 leads)")

    print(f"\nDone. Results saved to {TEST_FILE}")
    print(f"Daily Places total: {daily_count}/{PLACES_API_DAILY_LIMIT}")


if __name__ == "__main__":
    main()
