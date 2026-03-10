# Business Lead Scraper for Office Leasing

## Overview
This project is a Python-based lead generation tool designed to identify and collect potential small business leads for office leasing, primarily in Surrey, UK. It targets professional service companies and, with a specialized "wellness mode," businesses suitable for clinical/therapeutic spaces. The tool automates lead discovery, enrichment, and AI-driven scoring to provide sales and marketing teams with rich, scored lead data for flexible office spaces, streamlining their lead generation process.

## User Preferences
The user wants to interact with the system via a Command Line Interface (CLI) for scraping and a Flask web dashboard for lead visualization and management. The user prefers to specify search parameters such as target towns, sectors, and run modes (e.g., wellness mode) via CLI arguments. Output should be in CSV format. The user requires detailed logging for debugging purposes and the ability to perform dry runs without saving data. The user values detailed explanations of AI scores and reasons.

## System Architecture
The system employs a modular Python architecture for scraping, enrichment, AI scoring, and a web-based dashboard.

**UI/UX Decisions:**
- **Web Dashboard:** A Flask application provides a web interface for lead management with two tabs for lead categorization (Unit 8 Occupiers and Office Occupiers). It offers filtering by AI score, company name, sector, or location, displays aggregated statistics, and allows data download as CSV, including review flags and data quality scores.
- **CLI Interface:** `main.py` serves as the CLI entry point, enabling users to configure scraping parameters, target towns or sectors, activate wellness mode, and manage output.

**Technical Implementations & Feature Specifications:**
- **Scraping:** Two primary pipelines: (1) Google Places API for wellness/Unit 8 leads, (2) Companies House Advanced Search API for office occupier leads via SIC codes.
- **Office Pipeline:** `python main.py --mode office` uses `CHOfficeDiscoveryScraper` (SIC code + GU postcode discovery) as primary, with `PlacesCrossReference` for website/phone lookup. Output: `office_leads.csv`. Config in `config.py`. Uses fast enrichment path: when CH provides a valid director name, only the homepage is scraped (1 request vs 9) and personal email guesses are generated from director name + domain. Falls through to full enrichment only when no director name exists or homepage yields no email.
- **Targeting:** Supports "Professional Services Mode" (default), "Wellness Mode" for Unit 8 clinical leads, and "Office Mode" for CH SIC-code-based discovery across 12 sector groups and 16 GU postcodes.
- **Lead Enrichment:** Gathers comprehensive company and contact information including website, sector, contact names (via a dual-contact model leveraging website scans and Companies House data), email addresses, phone numbers, LinkedIn profiles, addresses, and employee counts. It incorporates robust retry logic, deduplication, multi-contact extraction, and multi-format email guessing. A refinement pipeline handles deduplication, validation, and re-enrichment, using flags for `name_review_needed` and `missing_email`. It supports multi-contact team email guesses, smart name validation, and various email formats.
- **Enrichment Enhancements:** Includes URL priority scoring, heading candidate fallback for name extraction, CTA link discovery for contacts, first-name-only extraction as a last resort, UK company number extraction via regex, and comprehensive data integrity logging. It also features heading-driven team detection for flat HTML team pages, allowing for multi-contact extraction and role-based prioritization.
- **Surgical Enrichment Runner:** Replaced monolithic enrichment with cohort-based processing (e.g., leads missing contact_name, low confidence leads, unverified emails). It uses single-tool run modes (e.g., `contact_recovery`, `false_positive_cleanup`, `email_verification`, `final_confirmation`) with per-lead guardrails (max 3 pages, max 5 HTTP requests, max 1 OpenAI call, 30-second timeout). Progress is saved incrementally, and leads are marked for permanent exclusion after multiple failures.
- **Quality Cleanup Pipeline:** Performs post-enrichment quality control, identifying and removing/correcting fake contact names and replacing guessed emails. It flags suspect names and bad email patterns, and selectively uses re-scraping and OpenAI for resolution.
- **Final Sanitisation Pipeline:** Executes a multi-step process for last-mile data quality, including garbage name filtering, sole trader inference from company names, email re-validation via website scraping, and AI-confirmation for borderline names, with anti-cycle logic and checkpoint saves.
- **JS-Rendered Email Scraping:** Extracts emails from JavaScript-rendered pages using Playwright with headless Chromium, employing methods like DOM text regex, mailto: link parsing, Cloudflare data-cfemail decoding, and schema.org JSON-LD extraction. It includes domain-matching validation for safety and early exit logic.
- **Email Rescrape Pipeline:** A multi-pass static scraping process with progressive retry markers to find additional emails.
- **AI Lead Scoring:** Integrates with the OpenAI API to assign an AI Score (1-10) and an AI Reason for lead suitability.
- **Data Model:** Uses a `BusinessLead` dataclass (`src/models.py`) for lead data structure, supporting the dual-contact model and enrichment flags.
- **Utility Functions:** Common operations like HTTP requests, CSV handling, and email extraction are managed in `src/utils.py`.
- **Output Fields:** Generates detailed CSV files with extensive fields covering company details, contact information, AI scores, and various metadata.

**System Design Choices:**
- **Modularity:** Clear separation of concerns for maintainability and scalability.
- **API-First Scraping:** Prioritizes APIs over traditional web scraping for reliability.
- **Incremental Processing:** Supports incremental CSV saving to prevent data loss.
- **Command-Line Flexibility:** Provides extensive CLI arguments for customizable operations.
- **Surgical over Monolithic:** Enrichment switched from "enrich everything" to cohort-based single-tool runs for reliability and speed.

## External Dependencies
- **Google Places API:** Main source for business information.
- **Companies House API:** Provides official UK company registry data.
- **OpenAI API:** Used for AI lead scoring.
- **Playwright:** Used for JavaScript-rendered email scraping.
- **Python Libraries:**
    - `requests`
    - `beautifulsoup4`
    - `lxml`
    - `pandas`
    - `fake-useragent`