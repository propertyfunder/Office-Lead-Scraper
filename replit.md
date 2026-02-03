# Business Lead Scraper for Office Leasing

## Overview
A Python-based lead generation tool that collects small business leads for office leasing in Surrey, UK. It targets professional service companies in Guildford, Godalming, Farnham, and Woking that may be interested in flexible office space.

## Project Structure
```
.
├── main.py              # CLI entry point
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

## Data Sources (by reliability)
1. **Google Places API** (Primary) - Best results, structured data with phone/website/ratings
2. **Companies House API** - Official company registry, director names
3. ~~Yell.com~~ - Blocked (403 Forbidden)
4. ~~Google Search~~ - Blocked (CAPTCHA protection)

## Target Sectors
- Professional services (accountants, lawyers, recruiters, consultants)
- Tech and software companies
- Engineering / R&D firms
- Digital marketing / media agencies
- Clean energy / environmental services
- Architects and design firms

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

### All Options
```bash
python main.py --town Woking --sector accountants --pages 3 --output my_leads.csv --no-enrich --fresh
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

## Output Fields
- Company name
- Website
- Sector / business description (with ratings from Google Places)
- Contact name (Director/MD if found)
- Email address or phone number
- LinkedIn profile
- Physical location (full address with postcode)
- Estimated employee count
- Source (Google Places, Companies House API, etc.)
- AI Score (1-10) - likelihood of needing office space
- AI Reason - explanation of the score

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
- 2026-02-03: Added AI-powered lead scoring with OpenAI
- 2026-02-03: Added Google Places API as primary data source
- 2026-02-03: Integrated LinkedIn profile discovery during enrichment
- 2026-02-03: Added phone numbers to leads from Google Places
- 2026-02-03: Improved sector classification with ratings
