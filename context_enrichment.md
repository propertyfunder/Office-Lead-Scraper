# Enrichment Context

## Overview

Two enrichment pipelines exist for the office dataset:

1. **`run_office_enrichment.py`** — the post-CH-sweep pipeline. Runs after the CH discovery sweep is complete. Processes `office_leads.csv` in four sequential phases with checkpoint saves between each phase. Triggered from the dashboard or via `python run_office_enrichment.py`.

2. **`run_surgical_enrichment.py`** — a targeted re-enrichment tool for the Unit 8 dataset (`unit8_leads_enriched.csv`). Processes leads in cohorts (A/B/C) using single-tool modes with strict per-lead guardrails. **Never touches `office_leads.csv`.**

---

## `run_office_enrichment.py` — Four Phases

### Phase 2: Website Discovery (Google Places)

**What it does**: For every lead that has no `website` field, queries the Google Places Text Search API using `"{company_name} {first part of location}"` as the query. Populates `website`, `phone`, and `google_rating` from the best match.

**Why it's Phase 2 not Phase 1**: Phase 1 was the CH sweep itself. The enrichment script picks up where the sweep left off.

**Matching logic (`_find_best_place`)**: Three-tier cascade, tried in order. Returns `(best_match_dict, tier_int)` or `(None, None)`. Stop words stripped from all name comparisons: `ltd, limited, plc, llp, uk, the, and, co, group`.

| Tier | Condition | Address guard |
|---|---|---|
| **1** | Word overlap ≥50% between CH name and Places display name | None — high confidence match |
| **2** | At least one meaningful word (≥4 chars, not a stop word) shared between names | Required — Places `formattedAddress` must contain a GU postcode (`GU\d{1,2}`) OR the first word of `lead.location` must appear in the address |
| **3** | Director surname (from `lead.contact_name`, must be ≥4 chars) found in Places display name | Same address guard as Tier 2 |

Tiers 2 and 3 exist specifically to handle trading-name situations (e.g. CH: `"J. Smith Consulting Ltd"` but Places knows the business as `"Smith Strategic"`). The address guard is mandatory on fallback tiers to prevent false positives from single-word name collisions across unrelated companies.

The Places API field mask was updated to include `places.formattedAddress` so the address guard can function. This field is fetched but never stored on the lead.

On a match, `lead.refinement_notes` gets a tag `places_match:tier1`, `places_match:tier2`, or `places_match:tier3` for auditability. The per-run summary prints tier breakdown: `tier1=N  tier2=N  tier3=N  no_match=N`.

**Daily cap**: `PLACES_API_DAILY_LIMIT = 500` (set in `config.py`). **Cost: ~£0.019/call** (logged at end of run).

**Cross-run call counting (persisted to disk)**: The daily call count is stored in `/tmp/places_daily_count.json` as `{"date": "YYYY-MM-DD", "count": N}`. At Phase 2 start the file is loaded; if the date matches today the stored count is used as the starting total, otherwise it resets to 0. After every individual Places call the count is written back immediately (atomic `.tmp` → `os.replace` pattern). This means the 500-call cap applies across all runs in a calendar day, not just within a single run — the root cause of the previous £111 cost overrun.

**Cap enforcement**: Checked **before every individual API call** against `daily_count` (the persisted total, not just this run's count). If `daily_count >= PLACES_API_DAILY_LIMIT` when Phase 2 starts, the phase is skipped entirely with a clear log message. If the cap is hit mid-loop, the loop breaks and remaining leads are left for tomorrow. The counters `daily_count` and `calls_this_run` are incremented before the HTTP call so failed/exception calls still count toward the cap.

**"Already tried" skip guard**: Targets are filtered to leads where `"places_lookup" not in (lead.enrichment_source or "")` in addition to having no website. Every lead that goes through Phase 2 — whether a match was found or not — has `;places_lookup` appended to `enrichment_source`. This means leads that returned no match, no website, or a Places API error are never re-queried on subsequent runs. Before this fix, no-match leads were silently re-queried on every re-run.

**Per-run console output**: After each call: `Places: {daily}/{cap} calls used today`. At Phase 2 start: `Places API — {n} calls used today ({remaining} remaining of {cap} daily limit). Est. cost so far: £X.XX`. At run end: calls this run, daily total, and estimated cost for both.

**Skip condition**: A lead is only targeted if it has no website AND has never been through Phase 2 (`places_lookup` absent from `enrichment_source`). Leads with social media URLs (`facebook.com`, `linkedin.com`, etc.) are given `size_signal = "small_new"` in Phase 3 without a Places call, because social URLs are not useful for email scraping.

**Checkpoint**: `save_leads(leads, fieldnames)` is called immediately after Phase 2 completes, before Phase 3 begins. Progress is never lost.

---

### Phase 3: Email Enrichment

**What it does**: For every lead without any email (`email`, `contact_email`, or `generic_email` all empty), classifies the company by size, then applies a tiered email strategy based on that classification.

#### Size classification (`_classify_size`)

Called for every target lead. Sets `lead.size_signal` to one of three values:

| Signal | Condition | Strategy |
|---|---|---|
| `small_new` | No website, social media URL only, or any other default | Director name email guess only — no HTTP requests |
| `established_small` | Has website + no `/team` page + `date_of_creation` before 2021-03-01 | Homepage scrape + mailto: links + director guess fallback |
| `larger` | Has website + detected `/team` page link or "our team" text on homepage | Homepage + team page scraping, then director guess fallback |

The `/team` page detection fetches the homepage, scans all `<a href>` links for patterns like `/team`, `/people`, `/our-team`, `/about-us/team`, `/meet-the-team`, `/staff`, `/about/team`, and also scans the full page text for "our team", "meet the team", "our people". If any match is found, the company is `larger`.

If there's no `/team` signal and the company has a website, the `date_of_creation` field (stored in `YYYY-MM-DD` format from CH) is parsed and compared against the cutoff **2021-03-01**. Companies older than this cutoff are `established_small`; newer ones are `small_new`.

**Why this cutoff matters**: A company incorporated before March 2021 has been trading for over 5 years. These are likely established businesses with real employees and accessible contact information on their homepage. Newer companies may still be sole traders or shell entities with minimal web presence.

**Skip condition for re-classification**: If `lead.size_signal` is already set AND `lead.website` is present, the function returns early without re-fetching the homepage. This prevents redundant HTTP requests during repeat runs.

#### Email strategy per signal

**`larger` (`_enrich_larger`):**
1. Fetch homepage → scan for domain-matching email via `_extract_emails_from_soup`
2. If no email, follow links matching `TEAM_PAGE_PATTERNS` (up to 3 pages)
3. If still no email, fall back to director name email guess

**`established_small` (`_enrich_established_small`):**
1. Fetch homepage → scan for domain-matching email
2. If no personal email, scan `mailto:` links for any address
3. Fall back to director name email guess

**`small_new` (`_enrich_small_new`):**
1. Skip all HTTP requests — director name email guess only

**Universal fallback** (after the strategy-specific function returns without finding an email): If `lead.contact_name` is set and `extract_domain(lead.website)` succeeds, `generate_email_guesses()` is called as a last resort regardless of size signal.

#### Email classification (`_assign_email`)

Emails found on websites are classified as `generic` (if they start with `info@`, `hello@`, `contact@`, `enquiries@`, `admin@`, `office@`, `mail@`, `support@`, `sales@`, `reception@`, `accounts@`) or `personal` (everything else). Generic emails go into `lead.generic_email`; personal emails go into `lead.email`.

#### Email field promotion rules

After enrichment, `lead.email` is promoted from other fields if still empty:
1. Check `lead.personal_email_guesses` — take the first comma-separated guess
2. Else check `lead.generic_email` — take the first comma-separated value

This ensures `lead.email` is always the best available email, so the dashboard's email count is accurate.

#### `_extract_emails_from_soup` filtering

Emails are extracted from both page text (regex) and `mailto:` hrefs. Filtered out:
- Any email containing: `example.com`, `sentry.io`, `wixpress.com`, `placeholder`, `email.com`, `domain.com`, `company.com`, `test.com`
- Emails whose domain doesn't match the site domain (when domain is known)

Only the first valid match is returned.

---

### `_update_enrichment_status`

Called after every lead is processed in Phase 3. Sets `lead.enrichment_status` to:

| Value | Condition |
|---|---|
| `complete` | Has a real email (not guessed) or generic email, AND has a contact name that differs from company name |
| `guessed_email` | Has any email (including guessed), AND has a contact name |
| `missing_name` | Has any email, but no distinct contact name |
| `missing_email` | Has a contact name, but no email anywhere |
| `incomplete` | Has neither email nor contact name |

**Boolean casing note**: The pipeline writes `"true"` and `"false"` (lowercase strings) into boolean fields like `email_guessed`, `missing_email`. The dashboard's `get_stats()` function checks `== 'True'` (capitalised) for `name_review_needed` and `missing_email`. These counts may be inaccurate for office leads. Do not change the pipeline's output format without also updating `app.py`.

---

### Phase 4: Geo Classification

**What it does**: For every lead that has no `geo_relevance` value yet (or has a website newly discovered via Places), calls `classify_from_website()` from `src/geo_classifier.py`.

**Targets**: Records where `geo_relevance` is blank OR records where `enrichment_source` contains `places_lookup` (these got a new website in Phase 2 and need a fresh geo check).

**Classification logic** (in `geo_classifier.py`):

Fetches the website homepage (unless it's a social media URL), extracts text, then applies a decision tree:

1. **Exclude** (wins over everything): explicit exclude keywords (`immigration`, `visa application`, `tier 2`, `right to work`, `offshore`, `cayman`, etc.) OR shell company keywords on the page OR shell words in company name with no local signal.
2. **Local**: GU postcode found in CH address OR GU postcode/Surrey town name found on website OR sector is inherently local (`architecture`, `accounting`, `legal`, `property management`, `training coaching`) with a local address signal.
3. **Review**: national scope keywords (`nationwide`, `global offices`, `clients worldwide`), dormant/parked website, very thin content (<50 words), email domain mismatches website domain, company name contains holding/nominee words.

The result and the reason string are stored in `lead.geo_relevance` and appended to `lead.refinement_notes` as `geo:{reason}`.

**Dashboard behaviour**: The main `index()` route filters out `geo_relevance == 'exclude'` from the office tab display. Excluded leads are still in the CSV but hidden by default. The `/api/leads` endpoint supports a `?geo=local_review` filter that hides excludes, or `?geo=local` / `?geo=review` / `?geo=exclude` for specific views.

---

## `run_surgical_enrichment.py` — Unit 8 Re-enrichment

**Important**: This script only operates on `unit8_leads_enriched.csv`. It must never be pointed at `office_leads.csv`.

### Cohorts

| Cohort | Target leads | Typical mode |
|---|---|---|
| A | Missing `contact_name`, has a real website (not social media) | `contact_recovery` |
| B | Has `contact_name`, `confidence_score` ≤ 3 | `false_positive_cleanup` or `final_confirmation` |
| C | Has guessed email (`email_guessed == 'true'`) | `email_verification` |

Leads already enriched today (`last_enriched_date == today`) are skipped unless `--force` is used. Leads marked `missing_name_final = 'true'` are permanently excluded from all cohorts (given up on).

### Modes and what they do

| Mode | Tools enabled | What it does |
|---|---|---|
| `contact_recovery` | Website only | Scrapes homepage + up to 2 sub-pages for contact name and email. No CH, no OpenAI, no LinkedIn. |
| `email_verification` | Website only | Same as contact_recovery — attempts to find/verify email from website. |
| `false_positive_cleanup` | OpenAI only | Skips website scraping. Uses GPT-4o-mini to confirm whether `contact_name` is a real person or a false positive (business term, UI element, etc.). Clears the name if AI says FALSE. |
| `final_confirmation` | OpenAI only (minimal website fetch) | Asks GPT-4o-mini whether the contact is the right person to approach for office space. Records the answer in `refinement_notes`. Does not clear the name. |

### Per-lead guardrails

Every lead processed by `SurgicalEnricher` has these hard limits enforced:
- Max 3 pages scraped (homepage + 2 sub-pages)
- Max 5 HTTP requests
- Max 1 OpenAI call
- Hard timeout: 30 seconds per lead (enforced via `threading.Thread.join(timeout=30)`)

Exceeding request limits raises `RequestLimitExceeded`. Exceeding 30s raises `LeadTimeout`. Both exceptions are caught and logged to `lead.refinement_notes` without crashing the batch.

### `should_mark_final`

After ≥2 failed enrichment attempts, a lead with no website (or social-only) and no contact name is marked `missing_name_final = 'true'`. This permanently removes it from all future cohort A runs.

### Progress saving

After every lead, `save_leads_to_csv()` is called immediately. This is intentionally slow (one write per lead) to ensure no data is lost on interrupt. Every 10 leads a checkpoint summary prints to stdout.

### OpenAI cost control

A `cost_tracker` on `LeadEnricher` (inherited by `SurgicalEnricher`) tracks daily spend. `can_make_call()` returns False if the budget is exhausted (budget defined in `src/enricher.py` — typically $2.00/day). The cost is written to `/tmp/openai_enrichment_cost.json` and displayed on the dashboard header.

---

## Configuration dependencies

| Config value | File | Used by |
|---|---|---|
| `PLACES_API_DAILY_LIMIT = 500` | `config.py` | Phase 2 cap in `run_office_enrichment.py` |
| `OFFICE_GU_POSTCODES` | `config.py` | Not used directly by enrichment (used by CH scraper) |
| `GOOGLE_MAPS_API_KEY` | env var | Phase 2 Places calls |
| `OPENAI_API_KEY` | env var | `false_positive_cleanup`, `final_confirmation` modes |

---

## What not to change

- **Do not change the Phase 2 → Phase 3 → Phase 4 order.** Phase 3 needs the websites discovered in Phase 2; Phase 4 needs the websites from both.
- **Do not remove the per-phase checkpoint saves** — enrichment runs for hours and must survive interrupts.
- **Do not change `INPUT_FILE = OUTPUT_FILE = "office_leads.csv"`** — the enrichment script reads and overwrites the same file.
- **Do not change the `2021-03-01` cutoff** without understanding the business intent: this is the 5-year boundary for "established" status.
- **`run_surgical_enrichment.py` INPUT_FILE and OUTPUT_FILE are both `unit8_leads_enriched.csv`** — do not accidentally redirect this at the office CSV.
- **The `_find_best_place` 50% word overlap threshold** is deliberately loose — it was tuned to match abbreviated or shortened company names on Places. Raising it would miss real matches.
