# Business Lead Scraper for Office Leasing

## Overview
A Python-based lead generation tool that collects small business leads for office leasing in Surrey, UK. It targets professional service companies in Guildford, Godalming, Farnham, and Woking that may be interested in flexible office space.

## Project Structure
```
.
├── main.py              # CLI entry point for scraping
├── app.py               # Flask web dashboard
├── templates/
│   └── index.html       # Dashboard HTML template
├── src/
│   ├── __init__.py
│   ├── models.py        # BusinessLead dataclass
│   ├── utils.py         # Utility functions (requests, CSV, email extraction)
│   ├── enricher.py      # Lead enrichment (emails, contacts, LinkedIn)
│   ├── ai_scorer.py     # AI-powered lead scoring (OpenAI)
│   └── scrapers/
│       ├── __init__.py
│       ├── base_scraper.py      # Abstract base scraper
│       ├── google_scraper.py    # Google search scraper (blocked)
│       ├── google_places.py     # Google Places API scraper (primary)
│       ├── yell_scraper.py      # Yell.com directory scraper (blocked)
│       ├── companies_house.py   # Companies House scraper
│       └── companies_house_api.py  # Companies House API scraper
└── leads.csv            # Output file (generated)
```

## Web Dashboard
The project includes a web dashboard to view and filter leads:
- Run `python app.py` to start the dashboard on port 5000
- **Two tabs**: Unit 8 Occupiers (wellness/clinical) and Office Occupiers
- Filter by minimum AI score
- Search by company name, sector, or location
- View statistics: total leads, emails, named contacts, average score
- **Download CSV** for each category separately

## Data Sources (by reliability)
1. **Google Places API** (Primary) - Best results, structured data with phone/website/ratings
2. **Companies House API** - Official company registry, director names
3. ~~Yell.com~~ - Blocked (403 Forbidden)
4. ~~Google Search~~ - Blocked (CAPTCHA protection)

## Target Sectors

### Professional Services Mode (default)
- Professional services (accountants, lawyers, recruiters, consultants)
- Tech and software companies
- Engineering / R&D firms
- Digital marketing / media agencies
- Clean energy / environmental services
- Architects and design firms

### Wellness Mode (--wellness flag) - Unit 8 Godalming Business Centre
Target businesses for Unit 8 - a small, professional space ideal for clinical, therapeutic, and wellness businesses:
- Physiotherapists, osteopaths, chiropractors
- Private GPs and health clinics
- Dentists (especially cosmetic or private)
- Podiatrists
- Psychotherapists and mental health professionals
- Pilates and yoga studios
- Massage therapy and acupuncture
- Holistic health and wellness services

Searches focus on Surrey towns within 10-mile radius of Godalming Business Centre: Godalming, Guildford, Farnham, Woking, Haslemere, Cranleigh, Milford, Shalford, Compton, Bramley, and Hindhead.

## Excluded Sectors
- Retail, logistics, trades, industrial businesses

## Usage

### Basic Usage (all towns)
```bash
python main.py
```

### Single Town
```bash
python main.py --town Guildford
```

### Specific Sector
```bash
python main.py --sector "IT companies"
```

### Wellness Mode (Unit 8)
```bash
python main.py --wellness
```

### Wellness Mode - Single Town
```bash
python main.py --wellness --town Godalming
```

### All Options
```bash
python main.py --town Woking --sector accountants --pages 3 --output my_leads.csv --no-enrich --fresh
```

### Wellness with Output File
```bash
python main.py --wellness --output wellness_leads.csv --pages 3
```

### Debug Mode
```bash
python main.py --verbose
```

### Test Without Saving
```bash
python main.py --dry-run
```

## CLI Arguments
- `--town, -t`: Target town (default: all Surrey towns)
- `--sector, -s`: Specific sector to focus on
- `--output, -o`: Output CSV file (default: leads.csv)
- `--pages, -p`: Max pages per source (default: 2)
- `--no-enrich`: Skip website enrichment (faster)
- `--fresh`: Start fresh, overwrite existing CSV
- `--verbose, -v`: Show detailed debug output
- `--dry-run`: Test scraping without saving to CSV
- `--wellness`: Search for wellness/clinical businesses suitable for Unit 8 (Godalming Business Centre)
- `--require-enrichment`: Only save leads with both email AND named contact

## Output Fields
- Company name
- Website
- Sector / business description
- Contact name (Director/MD if found)
- Email address
- Phone number
- LinkedIn profile
- Physical location (full address with postcode)
- Estimated employee count
- Source (Google Places, Companies House API, etc.)
- AI Score (1-10) - likelihood of needing office space (or Unit 8 suitability in wellness mode)
- AI Reason - explanation of the score
- Tag - Lead category (wellness, clinic-target, or empty)
- Google Rating - Rating from Google Places if available
- Category - 'unit8' for wellness/clinical or 'office' for general office leads

## Features
- **Google Places API** - Primary data source with rich business info
- **Companies House API** - Official UK company registry
- **AI Lead Scoring** - OpenAI-powered scoring (1-10) for office space potential
- Automatic LinkedIn profile discovery
- Phone numbers and websites from Google Places
- Business ratings and review counts
- Retry logic with exponential backoff
- Rate limiting to avoid blocks
- De-duplication by company name
- Incremental CSV saving
- Lead enrichment from company websites
- Verbose logging for debugging

## Dependencies
- requests
- beautifulsoup4
- lxml
- pandas
- fake-useragent

## API Setup (Required)

### Google Places API (Primary Source)
1. Go to Google Cloud Console: https://console.cloud.google.com/
2. Create a project and enable "Places API (New)"
3. Create an API key with Places API restrictions
4. Add as Replit Secret: `GOOGLE_MAPS_API_KEY`

### Companies House API (Supplementary)
1. Register at: https://developer.company-information.service.gov.uk/
2. Get your free API key
3. Add as Replit Secret: `COMPANIES_HOUSE_API_KEY`

### OpenAI API (Optional - AI Scoring)
1. Get an API key at: https://platform.openai.com/
2. Add as Replit Secret: `OPENAI_API_KEY`
3. Each lead will receive an AI score (1-10) for office space potential

## Sample Results
Recent run for Guildford found 173+ unique leads including:
- **Gaming Studios**: Ubisoft, Hello Games, Electronic Arts, Criterion Games
- **IT/Software**: Software Planet Group, Eagle Eye Solutions, Person Centred Software
- **Engineering**: Surrey Satellite Technology, WSP, Vision Engineering
- **Professional Services**: RSM, BDO, Scott Brownrigg, Clyde & Co
- **Marketing**: Air Social, Flourish, Delivered Social, Caffeine Marketing

## Known Limitations
- **Yell.com**: Blocked (403 Forbidden) - anti-bot protection
- **Google Search**: Blocked (CAPTCHA) - use Google Places API instead
- **Rate Limits**: Both APIs have usage limits (Google: 1000 requests/day free)

## Recent Changes
- 2026-02-05: **OpenAI guardrails** - Max 3 calls per record, skip if email AND contact_name present, console logging when skipped
- 2026-02-05: **LinkedIn headcount filter** - Skip large orgs (50+ employees) to focus on small businesses
- 2026-02-05: **OpenAI session tracking** - Track tokens and calls per session with reset capability
- 2026-02-05: **Reduced token usage** - Shortened OpenAI prompt, truncate text to 2000 chars (~500 tokens)
- 2026-02-05: **New enrichment priority order**: Companies House → Website → LinkedIn → OpenAI (last resort)
- 2026-02-05: **Email guessing** - Pattern firstname.lastname@domain.com when contact found but no email
- 2026-02-05: **email_guessed flag** - Track whether email was guessed (true) or verified from website
- 2026-02-05: **contact_verified flag** - Track whether contact found from authoritative source (CH, website)
- 2026-02-05: **Email cleanup** - Strip HTML artifacts, trailing text (Mobile, Contact, Clinic, etc.), normalize TLDs
- 2026-02-05: **Name normalization** - Proper case for contact names (John Smith, not john smith)
- 2026-02-05: **Stats dashboard** - Shows % complete, guessed emails, verified contacts, OpenAI budget, total leads
- 2026-02-05: **Enhanced Dashboard** - Added filtering by enrichment status and source, download filtered CSVs
- 2026-02-05: **Improved name validation** - Reject placeholder names like "New Title", require two-word names
- 2026-02-05: **LinkedIn tracking** - Track attempted profiles to prevent re-querying same businesses
- 2026-02-05: **Incremental saving** - Enrichment now saves after each lead to prevent data loss if process stops
- 2026-02-05: Added --save-interval CLI option to control how often progress is saved during enrichment
- 2026-02-04: **Refined enrichment workflow** with priority order: Companies House → Website → LinkedIn → OpenAI
- 2026-02-04: Added OpenAI last-resort enrichment with $2 daily cost cap and ai_enriched tracking flag
- 2026-02-04: LinkedIn retry guardrails (max 50 attempts per session)
- 2026-02-04: Companies House now also searches PSC (Persons with Significant Control)
- 2026-02-04: Smart email handling: prefers personal emails, accepts Gmail/Yahoo for sole traders
- 2026-02-04: **Smarter enrichment strategy** - Dynamic page discovery from nav links instead of hardcoded URLs
- 2026-02-04: Enhanced name extraction with schema.org/JSON-LD, meta author, title patterns
- 2026-02-04: LinkedIn search via Bing with refined query: `site:linkedin.com/in "{company}" {town} owner founder director`
- 2026-02-04: Companies House lookup for most recently appointed active director
- 2026-02-04: Added batch enrichment command (--enrich-existing) to re-enrich leads missing contact name or email
- 2026-02-04: Added enrichment_source and enrichment_status fields to track data provenance
- 2026-02-04: Enhanced enricher to check multiple website pages (About, Team, Contact, etc.)
- 2026-02-04: Added Companies House fallback for director name lookup
- 2026-02-03: Expanded wellness search area to include 11 towns within 10-mile radius of Godalming Business Centre
- 2026-02-03: Added enhanced duplicate checking (by name, website, location, and Google Place ID)
- 2026-02-03: Added place_id and search_town fields to track lead origins and enable deduplication
- 2026-02-03: Duplicate leads are now skipped before enrichment/scoring to save processing time
- 2026-02-03: Added wellness mode for Unit 8 (Godalming Business Centre) targeting clinical/therapeutic businesses
- 2026-02-03: Added new search categories: physiotherapy, osteopath, chiropractor, dentist, pilates, yoga, massage, mental health, holistic therapy
- 2026-02-03: Added tag field for lead categorization (wellness, clinic-target)
- 2026-02-03: Added phone and google_rating fields to output
- 2026-02-03: Updated AI scorer with Unit 8 suitability evaluation
- 2026-02-03: Added AI-powered lead scoring with OpenAI
- 2026-02-03: Added Google Places API as primary data source
- 2026-02-03: Integrated LinkedIn profile discovery during enrichment
- 2026-02-03: Added phone numbers to leads from Google Places
- 2026-02-03: Improved sector classification with ratings
