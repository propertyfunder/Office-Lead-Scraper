# CH Scraper Context

## What this module does and when it runs

`src/scrapers/ch_office_scraper.py` contains two classes:

1. **`CHOfficeDiscoveryScraper`** — queries the Companies House Advanced Search API to discover active UK companies with relevant SIC codes registered at GU postcodes. This is the engine behind the Office pipeline.
2. **`PlacesCrossReference`** — looks up a company on Google Places API to pull website, phone, and rating. **Currently disabled during the sweep** (see below).

It runs when `python main.py --mode office` is called (or when `worker.py` is started for headless deployment). The `discover()` generator is called from `run_office_pipeline()` in `main.py`.

---

## Key classes and functions

### `CHOfficeDiscoveryScraper`

| Method | Purpose |
|---|---|
| `__init__` | Reads `COMPANIES_HOUSE_API_KEY` from env. Initialises `seen_numbers` set for deduplication and `_last_api_call` float for rate limiting. |
| `_api_get(endpoint, params)` | Single gateway for all CH API calls. Enforces 0.6s minimum gap between requests, retries on 429 with exponential backoff (30s → 60s → 120s, max 3 retries), returns `None` on failure. |
| `_is_gu_postcode(postcode)` | Checks whether a postcode starts with a configured GU prefix (e.g. `GU7`). Must have at least one character after the prefix to rule out bare prefix matches. |
| `_format_director_name(raw)` | CH returns names as `SURNAME, Forenames`. This method flips them to `Forenames Surname` and applies `.title()`. Names without a comma are just title-cased. |
| `_get_directors(company_number)` | Calls `/company/{number}/officers`. Filters to `director` and `managing-director` roles only, skips anyone with a `resigned_on` date, requires at least a first and last name (space in result). Sorts by `appointed_on` descending — most recently appointed director comes first. |
| `_sic_to_sector(sic_codes)` | Maps company's SIC code list against `SIC_CODE_TO_SECTOR` dict (built from `config.py`). Returns the first match or `"Professional Services"` as fallback. |
| `_build_address(addr)` | Assembles address from CH's `registered_office_address` dict into a single comma-separated string. |
| `discover(postcodes, progress_callback)` | Main generator — see below. |

### `PlacesCrossReference`

| Method | Purpose |
|---|---|
| `lookup(lead)` | POST to `places.googleapis.com/v1/places:searchText`. Uses company name + first part of location as text query. Populates `lead.website`, `lead.phone`, `lead.google_rating` if a matching result is found. |
| `_find_best_match(name, places)` | Word overlap scoring. Strips common legal suffixes (`ltd`, `limited`, `plc`, `llp`, `uk`, `the`, `and`) from both the CH name and the Places display name. Accepts the first Places result with ≥50% word overlap. |

---

## The `discover()` loop — why it matters

Before the SIC loop fix, the scraper called `/advanced-search/companies` once per postcode with all SIC codes combined. The API silently caps results at ~500 per query, so large postcodes like GU1 were truncated.

**The fix**: loop over every `(postcode × sector_group)` combination — 16 postcodes × 12 sector groups = **192 queries**. Each sector group is submitted separately so its results are paginated fully.

```
for pc in OFFICE_GU_POSTCODES:          # 16 postcodes
    for sector_name, codes in OFFICE_SIC_CODES.items():   # 12 groups
        sic_str = ",".join(codes)
        while True:                      # paginate until exhausted
            data = _api_get(..., {"sic_codes": sic_str, "location": pc, "size": 100, "start_index": N})
            ...
            if len(items) < 100: break
            start_index += 100
```

Each page is 100 records. Pagination continues until a response returns fewer than 100 items, signalling the last page.

---

## Postcode filtering (double-check)

After the API returns results, each company's `registered_office_address.postal_code` is checked by `_is_gu_postcode()`. This is a safety net — the API's `location` parameter is fuzzy, so some results fall slightly outside target postcodes. Any company whose actual postcode doesn't start with a GU prefix is silently dropped.

---

## Deduplication

`seen_numbers` is a `set` of company numbers accumulated across the entire sweep. When the same company appears in multiple sector groups or postcodes (e.g. a law firm with two SIC codes), only the first occurrence is yielded. The check happens before any director API call, preventing double director lookups.

A second layer of deduplication runs in `run_office_pipeline()` in `main.py`: `existing_names` (company name, lowercased) and `existing_domains` (normalised website URL) are both checked against the existing CSV so that restarted sweeps don't add duplicates.

---

## Director name formatting — rules and edge cases

CH stores officer names in `SURNAME, Forenames` format (all-caps in some records). The formatter:
1. Splits on the first comma only
2. Applies `.title()` to each part
3. Reorders to `Forenames Surname`

Names with no comma are just title-cased as-is. Names that are shorter than 4 characters or have no space (e.g. a single word) are rejected — only full `Firstname Lastname` strings are stored.

Directors are sorted by `appointed_on` date descending, so the **most recently appointed** director is used as `contact_name`. The assumption is that recent appointments reflect current leadership.

---

## Rate limiting

Two layers:

1. **`_api_get`** — enforces a minimum 0.6 seconds between any two CH API calls using `time.sleep()`. This corresponds to ~100 calls/minute and sits safely under the CH quota of ~600 calls per 5 minutes.
2. **`rate_limit(0.3, 0.5)`** — called after each yielded lead (between director calls). This adds an additional small pause.
3. **`rate_limit(0.5, 1.0)`** — called between pages within a sector/postcode query.

On a 429 response, exponential backoff: wait = 30s × 2^retry (so 30s, 60s, 120s). After 3 retries, the request is abandoned and `None` is returned.

---

## Why Places is disabled during the sweep

`PlacesCrossReference.lookup()` is instantiated in `run_office_pipeline()` but the actual call is **commented out**:

```python
# Places lookup deferred to enrichment pass — too costly during sweep
# if places.is_available():
#     lead = places.lookup(lead)
```

**Reason**: Each Places call costs ~£0.019 and the sweep can produce thousands of leads. Calling Places for every company in the discovery phase would cost hundreds of pounds with no filtering. Instead, Places is called only in Phase 2 of `run_office_enrichment.py`, capped at `PLACES_API_DAILY_LIMIT = 500`, and only for leads that genuinely have no website. The `places` object is still instantiated and its stats are printed at the end of the run for transparency — it should always show 0 calls.

---

## `enrichment_status` set at discovery time

Each yielded lead gets a provisional `enrichment_status`:
- `"missing_email"` — has a director name but no email yet (normal case)
- `"missing_name"` — no director found on CH; nothing to guess email from

These values are overwritten by the full enrichment logic in `run_office_pipeline()` after the lead passes through `LeadEnricher`.

---

## How it connects to other modules

```
config.py
  OFFICE_SIC_CODES        ← 12 sector groups → sector label in CSV
  OFFICE_GU_POSTCODES     ← 16 prefixes → sweep targets
  SIC_CODE_TO_SECTOR      ← built dynamically from OFFICE_SIC_CODES

ch_office_scraper.py
  → yields BusinessLead (src/models.py)
  → called from run_office_pipeline() in main.py
  → main.py passes lead to LeadEnricher (src/enricher.py) if has website
  → main.py passes lead to GeoClassifier (src/geo_classifier.py)
  → main.py appends to office_leads.csv
```

---

## Known issues and things not to change

- **Do not re-enable the Places lookup inside the sweep** — it was deliberately disabled for cost reasons. Website enrichment happens in `run_office_enrichment.py` Phase 2.
- **Do not change the 0.6s rate limit floor** — CH quota is ~600 requests/5 minutes. Lower values risk bans.
- **Do not change `seen_numbers` to a list** — set lookup must stay O(1) for large sweeps.
- **`_format_director_name` must stay** — without it, CH's `SURNAME, Forenames` format would appear raw in the CSV.
- **Postcode double-check in `_is_gu_postcode` must stay** — the CH search API is fuzzy and returns out-of-area results.
- **Three retry max on 429** — increasing this risks an infinite loop on sustained rate limiting.

---

## Hardcoded values and thresholds

| Value | Location | Meaning |
|---|---|---|
| `0.6` seconds | `_api_get` | Minimum gap between CH API calls |
| `50` items | `_get_directors` | `items_per_page` for officer endpoint |
| `100` items | `discover` | Page size for company search |
| `0.5` | `_find_best_match` | Word overlap threshold for Places match |
| `30 × 2^retry` | `_api_get` | 429 backoff in seconds |
| `3` | `_api_get` | Max retries on 429 |
