# Business Lead Scraper for Office Leasing

## Overview
This project is a Python-based lead generation tool for identifying and collecting potential small business leads for office leasing, primarily in Surrey, UK. It targets professional service companies and, in a specialized "wellness mode," businesses suitable for clinical/therapeutic spaces. The tool automates lead discovery, enrichment, and AI-driven scoring to streamline lead generation for sales and marketing teams. Its core capability lies in providing rich, scored lead data for flexible office spaces.

## User Preferences
The user wants to interact with the system via a Command Line Interface (CLI) for scraping and a Flask web dashboard for lead visualization and management. The user prefers to specify search parameters such as target towns, sectors, and run modes (e.g., wellness mode) via CLI arguments. Output should be in CSV format. The user requires detailed logging for debugging purposes and the ability to perform dry runs without saving data. The user values detailed explanations of AI scores and reasons.

## System Architecture
The system employs a modular Python architecture for scraping, enrichment, AI scoring, and a web-based dashboard.

**UI/UX Decisions:**
- **Web Dashboard:** A Flask application provides a web interface for lead management. It features two tabs for lead categorization (Unit 8 Occupiers and Office Occupiers), filtering options by AI score, company name, sector, or location, and aggregated statistics. Data can be downloaded as CSV, and the dashboard displays review flags and data quality scores.
- **CLI Interface:** `main.py` serves as the CLI entry point, allowing users to configure scraping parameters, target specific towns or sectors, enable wellness mode, and manage output.

**Technical Implementations & Feature Specifications:**
- **Scraping:** Primarily uses the Google Places API for business data.
- **Targeting:** Supports "Professional Services Mode" (default) and "Wellness Mode" for specific geographical areas and business types, excluding retail, logistics, and industrial sectors.
- **Lead Enrichment:** Gathers comprehensive company and contact information including website, sector, contact names (via a dual-contact model leveraging website scans and Companies House data), email addresses, phone numbers, LinkedIn profiles, addresses, and employee counts. It includes robust retry logic, de-duplication, multi-contact extraction, and multi-format email guessing.
    - **Refinement Pipeline:** Features a comprehensive pipeline for deduplication, validation, and re-enrichment. It utilizes a flag-based system for `name_review_needed` and `missing_email` rather than immediate exclusion. It supports multi-contact team email guesses, smart name validation, and various email formats. Output is separated into `unit8_leads_enriched.csv` (all usable leads with flags) and `unit8_leads_excluded.csv` (leads with no web presence).
    - **Enrichment Enhancements:** Includes expanded page targeting for contact discovery, improved email extraction methods (JS script scanning, regex, icon-only mailto parsing), email classification, LinkedIn search strategies, and multi-contact extraction with role-based prioritization. It features name validation for various formats and titles, two-stage enrichment for deeper DOM scanning, responsiveness checks, and preservation of existing high-quality data. It also incorporates suspicious name detection, enrichment attempt tracking, and confidence scoring based on data completeness. New features include contact source tracking, mailshot categorization, contact extraction from company names, vanity name detection, enhanced email guess validation, and separate storage for personal and team email guesses.
- **AI Lead Scoring:** Integrates with the OpenAI API to assign an AI Score (1-10) and an AI Reason, indicating the lead's suitability.
- **Data Model:** Uses a `BusinessLead` dataclass (`src/models.py`) to define the structure of lead data, supporting the dual-contact model and enrichment flags.
- **Utility Functions:** Common operations like HTTP requests, CSV handling, and email extraction are managed in `src/utils.py`.
- **Output Fields:** Generates detailed CSV files with extensive fields covering company details, contact information, AI scores, and various metadata.

**Enricher v2 Enhancements (Feb 2026):**
    - **URL priority scoring:** Pages scored by intent (about/team/contact=100, unknown=30, our-/the- noisy=20, services/blog=15, privacy/legal=5). Sorted by score before slicing to 8 pages, ensuring highest-value pages always visited first.
    - **Heading candidate fallback:** When structured contact extraction fails, scans h1-h4 headings for 2-3 word names with role-term proximity boost (founder/director/therapist etc within 200 chars). Validated with _is_valid_contact_name and UK first name whitelist. Falls back after nav pages exhausted.
    - **CTA link discovery:** When no contact found after nav pages, scans homepage body for about/team/meet CTA links (anchors with relevant text/href patterns). Follows up to 3 CTA links to find contacts/emails. Logged as cta_about_page_followed in refinement_notes.
    - **First-name-only extraction:** Last-resort fallback for patterns like "Hi, I'm Louise" or "Meet Sarah, our founder". Single first name extracted only when 2+ word name not found. Sets contact_verified="false", logs single_name_only in refinement_notes. Confidence penalty of -0.5 applied. Email guessing skipped for single-word names.
    - **Company number extraction:** Regex patterns extract UK company numbers (6-8 digits) only when preceded by context keywords ("company number", "registration number", "companies house"). Enables direct CH API lookup by company number (faster/more reliable than name search). Logged as company_number_found:XXXXXXXX and director_from_ch_number:Name.
    - **Data integrity logging:** All fallback paths logged in refinement_notes without overwriting valid existing data. Principal/contact fields protected from downgrade overwrites. _notes initialized in result dict for consistent tracking.
    - **Heading-driven team detection (Feb 2026):** New `_detect_heading_team_pattern()` method scans ALL headings (h1-h6, uncapped) on a page. When 3+ headings pass `_looks_like_name` and `_is_valid_contact_name`, the page is treated as a team page. Each heading is paired with next-sibling role text (<150 chars) via role keyword matching. Runs as first pass in `_find_multiple_contacts()` before CSS class matching, so flat HTML team pages (like Medicspot) are detected without requiring team/staff CSS classes. Dr/Prof names get automatic "Doctor" title if no explicit role text found. max_contacts raised from 8 to 20. Contact promotion selects highest-role person when no single owner exists. Diagnostic notes: team_detected_via_headings, contact_from_heading_role_pair, multi_contact_team_page:N. Role priority updated to include doctor/gp/surgeon (tier 3), nurse/midwife/pharmacist (tier 7), with Dr/Prof name prefix as tier 3 fallback.

**Surgical Enrichment Runner (Feb 2026):**
    - **Cohort-based processing:** Replaced monolithic "enrich everything" with targeted cohorts:
        - Cohort A: leads missing contact_name (with website, excluding social-only)
        - Cohort B: leads with contact_name but low confidence_score (<=3)
        - Cohort C: leads with unverified/guessed emails
    - **Single-tool run modes:** Each run uses exactly one tool to prevent pipeline fragility:
        - `contact_recovery`: Website scrape only (max 3 pages per lead)
        - `false_positive_cleanup`: OpenAI classification only (validates names as real vs false positive)
        - `email_verification`: Website scrape only (targeted email search)
        - `final_confirmation`: OpenAI confirmation only (validates contact accuracy)
    - **SurgicalEnricher subclass:** Extends LeadEnricher with tool isolation via method overrides and API key nulling. Disables Companies House, LinkedIn, and OpenAI selectively per mode.
    - **Per-lead guardrails:** Max 3 pages, max 5 HTTP requests, max 1 OpenAI call, hard 30-second timeout per lead. Timeout/limit breaches logged in refinement_notes.
    - **Bulletproof progress saving:** CSV saved after every single lead. Tracks last_enriched_date and enrichment_attempts. On restart, skips leads enriched today unless --force flag used.
    - **Stop conditions:** After 2 failed attempts with no website/social-only + no company number + no names found, leads are permanently marked `missing_name_final=true`. Not retried in future runs.
    - **CLI interface:** `run_surgical_enrichment.py --cohort A|B|C --mode <mode> [--limit N] [--force] [--dry-run] [--stats]`
    - **Performance:** ~12 leads/min (vs ~1/min for monolithic enricher) due to tool isolation and page caps.
    - **Success criteria:** Target ~85-90% real defensible contacts, ~10-15% explicitly marked unreachable.

**System Design Choices:**
- **Modularity:** Clear separation of concerns for maintainability and scalability.
- **API-First Scraping:** Prioritizes APIs over traditional web scraping to ensure reliability.
- **Incremental Processing:** Supports incremental CSV saving to prevent data loss during long enrichment processes.
- **Processing Order:** Defines a specific order for refinement and Companies House enrichment to ensure data integrity.
- **Command-Line Flexibility:** Provides extensive CLI arguments for customizable operations.
- **Surgical over Monolithic:** Enrichment switched from "enrich everything" to cohort-based single-tool runs for reliability and speed.

## External Dependencies
- **Google Places API:** Main source for business information.
- **Companies House API:** Provides official UK company registry data and director names.
- **OpenAI API:** Used for AI lead scoring.
- **Python Libraries:**
    - `requests`
    - `beautifulsoup4`
    - `lxml`
    - `pandas`
    - `fake-useragent`
