# Business Lead Scraper for Office Leasing

## Overview
This project is a Python-based lead generation tool designed to identify and collect potential small business leads for office leasing, specifically targeting Surrey, UK. Its primary purpose is to assist in finding professional service companies in key towns like Guildford, Godalming, Farnham, and Woking that could benefit from flexible office spaces. It also features a specialized "wellness mode" to identify businesses suitable for a particular clinical/therapeutic space, Unit 8 Godalming Business Centre. The tool automates lead discovery, enrichment, and scoring, providing a streamlined process for sales and marketing teams.

## User Preferences
The user wants to interact with the system via a Command Line Interface (CLI) for scraping and a Flask web dashboard for lead visualization and management. The user prefers to specify search parameters such as target towns, sectors, and run modes (e.g., wellness mode) via CLI arguments. Output should be in CSV format. The user requires detailed logging for debugging purposes and the ability to perform dry runs without saving data. The user values detailed explanations of AI scores and reasons.

## System Architecture
The system is built around a modular Python architecture comprising scraping, enrichment, AI scoring, and a web-based dashboard.

**UI/UX Decisions:**
- **Web Dashboard:** A Flask application provides a web-based dashboard accessible via `app.py`. It features two tabs for lead categorization (Unit 8 Occupiers and Office Occupiers). Users can filter leads by minimum AI score, search by company name, sector, or location, and view aggregated statistics (total leads, emails, contacts, average score). Filtered data can be downloaded as CSV.
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
- **AI Lead Scoring:** Utilizes the OpenAI API to provide an AI Score (1-10) indicating the likelihood of needing office space, or suitability for Unit 8 in wellness mode, along with an AI Reason.
- **Data Model:** `src/models.py` defines the `BusinessLead` dataclass.
- **Utility Functions:** `src/utils.py` handles requests, CSV operations, and email extraction.
- **Output Fields:** Generates comprehensive CSV files with fields like Company name, Website, Sector, Contact name, Email address, Phone number, LinkedIn profile, Physical location, Estimated employee count, Source, AI Score, AI Reason, Tag, Google Rating, and Category.

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