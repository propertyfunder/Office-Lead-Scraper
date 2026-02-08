# Business Lead Scraper for Office Leasing

## Overview
This project is a Python-based lead generation tool designed to identify and collect potential small business leads for office leasing, specifically targeting Surrey, UK. Its primary purpose is to assist in finding professional service companies in key towns like Guildford, Godalming, Farnham, and Woking that could benefit from flexible office spaces. It also features a specialized "wellness mode" to identify businesses suitable for a particular clinical/therapeutic space, Unit 8 Godalming Business Centre. The tool automates lead discovery, enrichment, and scoring, providing a streamlined process for sales and marketing teams.

## User Preferences
The user wants to interact with the system via a Command Line Interface (CLI) for scraping and a Flask web dashboard for lead visualization and management. The user prefers to specify search parameters such as target towns, sectors, and run modes (e.g., wellness mode) via CLI arguments. Output should be in CSV format. The user requires detailed logging for debugging purposes and the ability to perform dry runs without saving data. The user values detailed explanations of AI scores and reasons.

## System Architecture
The system is built around a modular Python architecture comprising scraping, enrichment, AI scoring, and a web-based dashboard.

**UI/UX Decisions:**
- **Web Dashboard:** A Flask application provides a web-based dashboard accessible via `app.py`. It features two tabs for lead categorization (Unit 8 Occupiers and Office Occupiers). Users can filter leads by minimum AI score, search by company name, sector, or location, and view aggregated statistics (total leads, emails, contacts, average score). Filtered data can be downloaded as CSV. Dashboard shows review flags (name_review_needed, missing_email) and data quality scores.
- **CLI Interface:** `main.py` serves as the CLI entry point, allowing users to control scraping parameters, target specific towns or sectors, enable wellness mode, and manage output.

**Technical Implementations & Feature Specifications:**
- **Scraping:** Primarily uses the Google Places API for rich, structured business data. The Companies House API provides official company registry information, including director names. Failed attempts with Yell.com and Google Search due to anti-bot measures led to reliance on APIs.
- **Targeting:** Supports "Professional Services Mode" (default) for accountants, lawyers, tech companies, etc., and a "Wellness Mode" for Unit 8 Godalming, targeting clinical and wellness businesses within a 10-mile radius of Godalming. Excludes retail, logistics, and industrial sectors.
- **Lead Enrichment:**
    - **Dual-contact model (Feb 2026):** Separates Companies House director data (`principal_name`, `principal_email_guess`) from website team contacts (`contact_name`). Enricher scans website first for team contacts, then Companies House independently for directors. `ch_enrich.py` provides standalone CH-only enrichment for existing leads.
    - Gathers company name, website, sector, contact name (Director/MD), email address, phone number, LinkedIn profile, physical address, employee count, and source.
    - Automates LinkedIn profile discovery.
    - Extracts phone numbers and websites.
    - Implements retry logic with exponential backoff and rate limiting.
    - Performs de-duplication based on company name.
    - Includes multi-contact extraction and multi-format email guessing.
    - Features a robust refinement pipeline (`refine_leads.py`) for deduplication, validation, and re-enrichment, generating `unit8_leads_enriched.csv` and `unit8_leads_excluded.csv`.
- **Refinement Pipeline v2 (Feb 2026):**
    - **Exclusion logic:** Only excludes leads with NO website AND NO Facebook page. All other leads go to enriched file with flags.
    - **Flag-based system:** Uses `name_review_needed` and `missing_email` flags instead of excluding suspect leads.
    - **Multi-contact team email guesses:** Generates `team_email_guesses` for each person found in `contact_names`.
    - **Email deduplication:** When `contact_name` matches `principal_name`, email guesses are deduplicated.
    - **Smart name validation:** Vowel checks, gibberish detection (unusual bigrams/trigrams), job title stripping, repeated character detection.
    - **Email formats:** firstname.lastname@, f.lastname@, firstname@, initials@, lastname@, firstname_lastname@, firstnamelastname@
    - **Output columns:** company_name, website, website_verified, facebook_url, contact_name, contact_names, contact_email, personal_email_guesses, team_email_guesses, principal_name, principal_email_guess, generic_email, email_type, name_review_needed, missing_email, data_score, confidence_score, enrichment_attempts, refinement_notes, plus metadata fields.
    - **Two output files only:** `unit8_leads_enriched.csv` (all usable/flagged leads) and `unit8_leads_excluded.csv` (no web presence at all).
- **AI Lead Scoring:** Utilizes the OpenAI API to provide an AI Score (1-10) indicating the likelihood of needing office space, or suitability for Unit 8 in wellness mode, along with an AI Reason.
- **Data Model:** `src/models.py` defines the `BusinessLead` dataclass with fields for dual-contact model, flags, and team emails.
- **Utility Functions:** `src/utils.py` handles requests, CSV operations, and email extraction.
- **Output Fields:** Generates comprehensive CSV files with fields like Company name, Website, Sector, Contact name, Email address, Phone number, LinkedIn profile, Physical location, Estimated employee count, Source, AI Score, AI Reason, Tag, Google Rating, and Category.

**Enrichment Pipeline v2 (Feb 2026):**
    - **Expanded page targeting:** 12 fallback URL paths (/team, /clinicians, /practitioners, /about-us, /staff, /meet-the-team, /our-story, /leadership, /who-we-are, /our-team, /therapists, /our-people) tried when nav discovery yields <3 pages.
    - **Improved email extraction:** JS script scanning, full-text regex with domain validation, icon-only mailto parsing, data-*/aria-*/alt attribute scanning.
    - **Email classification:** email_type field set to personal/generic/both/guessed/none based on extraction results.
    - **LinkedIn search:** Multiple query strategies with wellness-specific role terms (practitioner, therapist, lead).
    - **Multi-contact extraction:** Limit raised to 8 with role-based prioritization (founder/director > practice lead > senior > general staff). Global sorting before truncation ensures highest-priority contacts retained. Always attempted regardless of contact_name status.
    - **Name validation:** Handles Dr/BSc/MSc/PhD titles, qualification suffixes, hyphenated names, non-English names. Consonant cluster threshold relaxed from 5 to 6. Common UK first name whitelist (~180 names) prevents over-aggressive filtering.
    - **Two-stage enrichment:** Stage 1 uses structured links/headings. Stage 2 deep DOM scan now runs even when single contact found (to discover team members), checks homepage AND all subpages for card-based/image alt/contextual patterns.
    - **Responsiveness check:** Early return when website is unresponsive or returns HTTP 400+, with failure logging.
    - **No-web-presence exclusion:** Records with no website URL AND no Facebook URL are automatically excluded with enrichment_status='excluded' and logged in refinement_notes.
    - **Field preservation:** principal_name, principal_email_guess, generic_email, contact_names protected from overwrites unless new data is better.
    - **Principal email guessing:** Always attempts to guess principal_email when principal_name exists but principal_email_guess is empty, regardless of CH API key or CH lookup status.
    - **Contact backfill:** When principal_name exists but contact_name is missing, automatically backfills contact_name from principal with refinement_notes logging.
    - **Suspicious name detection:** _is_suspicious_name method checks for unusual bigrams, repeated characters, low vowel ratio. When suspicious name found with valid team alternatives, assigns best team member and logs replacement in refinement_notes.
    - **Enrichment tracking:** enrichment_attempts counter tracks retry count per lead. refinement_notes logs scraping challenges (e.g., social_media_url, website_no_data, openai_triggered:reason, contact_backfilled_from_principal, principal_email_guessed, suspicious_name_replaced, invalid_name_rejected).
    - **Confidence scoring:** 1-5 score based on enrichment completeness (contact name, email quality, website verification, team contacts, principal data).
    - **Email classification (refined):** email_type checks primary email field AND generic_email field. Personal classification requires name-part match in email local part or non-guessed status. Guessed emails without name match classified as 'guessed'.
    - **OpenAI trigger logging:** Logs specific reason why OpenAI was invoked (scraper_failed_no_contact, scraper_failed_no_email, etc.).

**System Design Choices:**
- **Modularity:** Separation of concerns into `scrapers/`, `enricher.py`, `ai_scorer.py`, and `refine_leads.py`.
- **API-First Scraping:** Prioritizes reliable APIs (Google Places, Companies House) over traditional web scraping to mitigate blocking issues.
- **Incremental Processing:** Supports incremental CSV saving during enrichment to prevent data loss.
- **Processing Order:** Refinement reads from `leads.csv` and writes to `unit8_leads_enriched.csv`. CH enrichment (`ch_enrich.py`) runs AFTER refinement to populate `principal_name`/`principal_email_guess` on the enriched output. Running refinement after CH enrichment will overwrite CH data.
- **Command-Line Flexibility:** Extensive CLI arguments allow for highly customizable scraping and enrichment runs.

## External Dependencies
- **Google Places API:** Primary data source for business information. Requires `GOOGLE_MAPS_API_KEY`.
- **Companies House API:** Used for official UK company registry data and director names. Requires `COMPANIES_HOUSE_API_KEY`.
- **OpenAI API:** Powers the AI lead scoring feature. Requires `OPENAI_API_KEY`.
- **Python Libraries:**
    - `requests`: For HTTP requests.
    - `beautifulsoup4`, `lxml`: For parsing HTML during web enrichment.
    - `pandas`: For data manipulation.
    - `fake-useragent`: For rotating user agents to avoid detection.
