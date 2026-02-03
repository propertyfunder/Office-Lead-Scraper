# Business Lead Scraper for Office Leasing

## Overview
A Python-based web scraper that collects small business leads for office leasing in Surrey, UK. It targets professional service companies in Guildford, Godalming, Farnham, and Woking that may be interested in flexible office space.

## Project Structure
```
.
├── main.py              # CLI entry point
├── src/
│   ├── __init__.py
│   ├── models.py        # BusinessLead dataclass
│   ├── utils.py         # Utility functions (requests, CSV, email extraction)
│   ├── enricher.py      # Lead enrichment (emails, contacts, LinkedIn)
│   └── scrapers/
│       ├── __init__.py
│       ├── base_scraper.py      # Abstract base scraper
│       ├── google_scraper.py    # Google search scraper
│       ├── yell_scraper.py      # Yell.com directory scraper
│       └── companies_house.py   # Companies House scraper
└── leads.csv            # Output file (generated)
```

## Target Sectors
- Professional services (accountants, lawyers, recruiters, consultants)
- Tech and software companies
- Engineering / R&D firms
- Digital marketing / media agencies
- Clean energy / environmental services

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

## CLI Arguments
- `--town, -t`: Target town (default: all Surrey towns)
- `--sector, -s`: Specific sector to focus on
- `--output, -o`: Output CSV file (default: leads.csv)
- `--pages, -p`: Max pages per source (default: 2)
- `--no-enrich`: Skip website enrichment (faster)
- `--fresh`: Start fresh, overwrite existing CSV

## Output Fields
- Company name
- Website
- Sector / business description
- Contact name (Director/MD if found)
- Email address
- LinkedIn profile
- Physical location (town + postcode)
- Estimated employee count
- Source

## Features
- Multi-source scraping (Yell.com, Companies House, Google)
- Rate limiting to avoid blocks
- User-agent rotation
- De-duplication by company name/email
- Incremental CSV saving
- Lead enrichment from company websites
- Email guessing based on contact names

## Dependencies
- requests
- beautifulsoup4
- lxml
- pandas
- fake-useragent

## Known Limitations
Many websites implement anti-bot protection that may block automated scraping:
- **Yell.com**: Often returns 403 Forbidden
- **Google**: May require CAPTCHA or block repeated requests
- **Companies House**: Generally more accessible

### Workarounds
1. Use a VPN or proxy service
2. Add delays between requests (already implemented)
3. Consider using Selenium for JavaScript-heavy sites
4. Use official APIs where available (e.g., Companies House API)
5. Run during off-peak hours

## Future Improvements
- Add Companies House API integration (requires API key)
- Implement proxy rotation
- Add Selenium support for JavaScript-heavy sites
- Add Crunchbase and LinkedIn scrapers
- Export to additional formats (JSON, Excel)
