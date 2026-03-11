# Models Context

## Overview

`src/models.py` defines `BusinessLead`, a Python `dataclass` that is the single data structure used throughout the entire pipeline. Every lead — whether from Google Places, Companies House, or the web scraper — is stored as a `BusinessLead` and serialised to CSV via `to_dict()` (which calls `dataclasses.asdict()`).

All fields default to `""` (empty string). There are no `int`, `bool`, or `None` typed fields — everything is a string in the CSV and in memory. Boolean-like fields use the string values `"true"` / `"false"` or `"True"` / `"False"` (see the inconsistency note below).

---

## Complete field reference

### Core identity fields

| Field | Type | Populated by | Values / Notes |
|---|---|---|---|
| `company_name` | str | All scrapers | Required. The only mandatory field in `__init__`. |
| `website` | str | Enricher, Places API, JS scraper | Full URL including scheme. May be a Facebook/Instagram URL. |
| `sector` | str | CH scraper, Places scraper | Free text. CH-sourced leads use `.title()` formatted sector group names from `SIC_CODE_TO_SECTOR` (e.g. `"Software It"`, `"Management Consultancy"`). Unit 8 leads use Google Places category names. |
| `location` | str | CH scraper (`_build_address`), Places scraper | CH address format: `"Line1, Line2, Town, Region, Postcode"`. |
| `source` | str | All scrapers | `"Companies House"`, `"Google Places"`, `"Yell"`, `"Google Search"` |
| `category` | str | `main.py`, enrichment scripts | `"office"` or `"unit8"`. Set at lead creation time. |
| `place_id` | str | Google Places scraper | Google Places unique ID. Empty for CH-sourced leads. |
| `search_town` | str | Main scraper loop | Town name passed to scraper (e.g. `"Godalming"`). Empty for CH-sourced leads (they use postcodes instead). |

---

### Contact fields

| Field | Type | Populated by | Values / Notes |
|---|---|---|---|
| `contact_name` | str | CH `_get_directors`, website enricher, Surgical enricher | The primary person to contact. For CH leads, this is the most recently appointed director in `Firstname Lastname` format. For web-scraped leads, extracted from website text. |
| `contact_names` | str | Website enricher (multi-contact), Surgical enricher | Semicolon-separated list of all contacts found on the website (e.g. `"Alice Smith; Bob Jones"`). Includes `contact_name` if that person was found on the site. |
| `contact_titles` | str | Website enricher | Semicolon-separated titles for entries in `contact_names` (e.g. `"Director; Partner"`). |
| `principal_name` | str | CH API enricher | Director name from Companies House (as opposed to website-extracted name). Used when CH-sourced name differs from website-sourced name. |
| `principal_email_guess` | str | Enricher | Email guess generated from `principal_name` + domain. |
| `contact_source` | str | Surgical enricher | Where `contact_name` came from: `"website"`, `"companies_house"`, `"unknown"`. |
| `contact_verified` | str | Surgical enricher | `"true"` if contact name passed all validation checks; `"false"` if flagged suspicious but kept; `""` if not yet assessed. |
| `multiple_contacts` | str | Website enricher | `"true"` if more than one contact was found on the site. |

---

### Email fields

| Field | Type | Populated by | Values / Notes |
|---|---|---|---|
| `email` | str | Enricher, JS scraper, Email rescrape | **Primary email field**. Should hold the best available email. Promoted from `personal_email_guesses` or `generic_email` if no other email is found (done in `run_office_pipeline` and `_update_enrichment_status`). |
| `contact_email` | str | Enricher | Personal email found directly on website (not guessed). May differ from `email` if `email` was set by a different method. |
| `generic_email` | str | Enricher, Phase 3 | Catch-all addresses: `info@`, `hello@`, `contact@`, `enquiries@`, `admin@`, `office@`, `mail@`, `support@`, `sales@`, `reception@`, `accounts@`. |
| `personal_email_guesses` | str | Enricher, `generate_email_guesses()` | Semicolon-separated list of guessed personal emails for `contact_name` (e.g. `"john.smith@example.com; j.smith@example.com; jsmith@example.com"`). |
| `team_email_guesses` | str | Surgical enricher | Semicolon-separated guesses for other members of `contact_names` (secondary contacts). |
| `email_guessed` | str | Enricher | `"true"` if `email` is a guessed address (not scraped). `"false"` or `""` if scraped. |
| `email_type` | str | Enricher, Phase 3 `_assign_email` | `"personal"`, `"generic"`, `"personal_guess"`, `"none"`. |

---

### Enrichment tracking fields

| Field | Type | Populated by | Values / Notes |
|---|---|---|---|
| `enrichment_source` | str | All enrichers | How data was obtained. Examples: `"companies_house"`, `"website"`, `"not_found"`, `"companies_house;places_lookup"`. Semicolon-separated if multiple sources contributed. |
| `enrichment_status` | str | `run_office_pipeline`, `_update_enrichment_status`, Surgical enricher | See table below. |
| `enrichment_attempts` | str | Surgical enricher | Integer stored as string. Incremented each time a lead is processed by Surgical enricher. Used to decide when to mark `missing_name_final`. |
| `last_enriched_date` | str | Surgical enricher | `YYYY-MM-DD` string. Used to skip leads already processed today (unless `--force`). |
| `ai_enriched` | str | AI enricher | `"true"` if OpenAI was used to enrich this lead. |
| `website_verified` | str | Enricher | `"yes"` if website was successfully fetched, `"facebook"` if only a Facebook page was found, `""` if not checked. |

**`enrichment_status` values:**

| Value | Meaning |
|---|---|
| `complete` | Has real (non-guessed) email AND named contact |
| `guessed_email` | Has email (possibly guessed) AND named contact |
| `missing_email` | Has named contact but no email |
| `missing_name` | Has email but no contact name |
| `incomplete` | Neither email nor contact name |
| `missing_name` | Set at CH discovery time if no director was found |

---

### Quality and review fields

| Field | Type | Populated by | Values / Notes |
|---|---|---|---|
| `name_review_needed` | str | Enricher | `"True"` (capitalised) if the contact name was flagged as suspicious but not removed. |
| `missing_email` | str | Enricher, Phase 2 | `"True"` (capitalised) if the lead genuinely has no email and none was findable. Also set to `"true"` (lowercase) by `run_office_enrichment.py` Phase 3 when enrichment fails. **Inconsistent casing — see note.** |
| `missing_name_final` | str | Surgical enricher | `"true"` if the lead has been permanently given up on for contact name discovery. Excludes the lead from all future Cohort A runs. |
| `confidence_score` | str | Enricher | Integer `1`–`5` stored as string. Reflects how reliable the contact data is. ≤3 puts the lead in Cohort B for surgical re-enrichment. |
| `data_score` | str | Enricher | `"high"`, `"medium"`, or `"low"`. Overall data quality rating. |
| `data_quality` | str | Enricher | Free text quality notes. |
| `refinement_notes` | str | All enrichers | Semicolon-separated audit trail. Examples: `"geo:gu_postcode_in_address"`, `"fp_check:REAL: name appears on website"`, `"timeout_exceeded"`. Accumulates across runs. |

---

### AI scoring fields

| Field | Type | Populated by | Values / Notes |
|---|---|---|---|
| `ai_score` | str | `AILeadScorer` | Integer `1`–`10` stored as string. Empty if AI scoring was not run. |
| `ai_reason` | str | `AILeadScorer` | One-sentence explanation of the score. |
| `tag` | str | `AILeadScorer` | Categorical tag assigned by AI (e.g. `"high_potential"`, `"speculative"`). |
| `mailshot_category` | str | Enricher | `"priority"`, `"fallback"`, or `"do_not_email"`. Drives mailshot segmentation. |

---

### Company metadata fields

| Field | Type | Populated by | Values / Notes |
|---|---|---|---|
| `employee_count` | str | Enricher | Text estimate (e.g. `"2-10"`, `"11-50"`). Rarely populated for office leads. |
| `phone` | str | CH enricher, Places API, website scraper | UK phone number. |
| `google_rating` | str | Places API | Format: `"4.5/5 (23 reviews)"`. Empty for CH-only leads. |
| `linkedin` | str | Enricher | LinkedIn company page URL. |
| `facebook_url` | str | Enricher | Facebook page URL. Sometimes used as `website` if no real website exists. |
| `date_of_creation` | str | CH scraper | `YYYY-MM-DD` format from Companies House `date_of_creation` field. Used in `_classify_size` to determine `established_small` vs `small_new`. |
| `size_signal` | str | Phase 3 `_classify_size` | `"larger"` (has /team page), `"established_small"` (pre-2021-03-01, no team page), `"small_new"` (default/new/no-website). |
| `geo_relevance` | str | `geo_classifier.py`, Phase 4 | `"local"`, `"review"`, or `"exclude"`. Drives dashboard visibility and download filtering. |

---

## Key methods

### `to_dict()`
Returns `dataclasses.asdict(self)` — a flat dict of all fields. Used for CSV writing by `csv.DictWriter`. All values are strings (or empty strings).

### `get_key()`
Returns `"{company_name.lower()}|{email.lower()}"`. Used for deduplication in the non-CH pipelines. **Not used by the CH pipeline** — CH pipeline deduplicates by company number and normalised company name.

### `get_website_key()`
Returns the website domain with scheme, `www.`, and trailing slash stripped. Used in `run_office_pipeline` to deduplicate by domain (prevents two CH companies with the same website being added separately).

### `get_name_location_key()`
Returns `"{company_name.lower()}|{location.lower()}"`. Used in refinement pipelines to identify the same business when email changes.

---

## The boolean inconsistency

Fields set by the Unit 8 / Surgical enrichment pipeline use **capitalised** boolean strings (`"True"`, `"False"`) because Python's `str(True)` returns `"True"`. Fields set by the office pipeline (`run_office_enrichment.py`, `run_office_pipeline` in `main.py`) use **lowercase** strings (`"true"`, `"false"`) because they are written as literal strings in code.

| Field | Written as |
|---|---|
| `name_review_needed` | `"True"` (capitalised, from Python `str(bool)`) |
| `missing_email` (Unit 8 path) | `"True"` (capitalised) |
| `missing_email` (office path) | `"true"` (lowercase literal) |
| `email_guessed` | `"true"` (lowercase literal) |
| `contact_verified` | `"true"` / `"false"` (lowercase literals) |
| `ai_enriched` | `"true"` (lowercase literal) |

The dashboard's `get_stats()` function checks `== 'True'` for `name_review_needed` and `missing_email`, which means those counts are accurate for Unit 8 leads but will show 0 for office leads (which write `"true"`). **Do not fix this inconsistency unless you update all writers and all readers in the same change.**

---

## Field population timeline

For a typical office lead, fields are populated in this order:

```
1. CH discovery (ch_office_scraper.py)
   → company_name, sector, location, contact_name (director), source,
      category, enrichment_source, date_of_creation, enrichment_status

2. run_office_pipeline enrichment (main.py → enricher.py)
   → website, email, generic_email, contact_email, personal_email_guesses,
      email_type, email_guessed, enrichment_status (updated)

3. Geo classification (geo_classifier.py)
   → geo_relevance, refinement_notes

4. Phase 2 (run_office_enrichment.py)
   → website (if missing), phone, google_rating, enrichment_source (appended)

5. Phase 3 (run_office_enrichment.py)
   → size_signal, email, generic_email, personal_email_guesses,
      email_type, email_guessed, missing_email, enrichment_status (updated)

6. Phase 4 (run_office_enrichment.py → geo_classifier.py)
   → geo_relevance (updated if website changed), refinement_notes (appended)
```

For a Unit 8 lead, fields are populated by the Google Places scraper first, then `LeadEnricher`, then optionally `AILeadScorer`, then the Surgical enricher for re-enrichment passes.
