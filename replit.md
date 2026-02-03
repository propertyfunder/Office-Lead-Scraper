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
│       ├── companies_house.py   # Companies House scraper
│       └── companies_house_api.py  # Companies House API scraper
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
- Sector / business description
- Contact name (Director/MD if found)
- Email address
- LinkedIn profile
- Physical location (town + postcode)
- Estimated employee count
- Source

## Features
- Multi-source scraping (Yell.com, Companies House, Google)
- Companies House API integration (optional, more reliable)
- Retry logic with exponential backoff
- Rate limiting to avoid blocks
- User-agent rotation with realistic browser headers
- Block/CAPTCHA detection with helpful warnings
- De-duplication by company name/email
- Incremental CSV saving
- Lead enrichment from company websites
- Email guessing based on contact names
- Verbose logging for debugging

## Dependencies
- requests
- beautifulsoup4
- lxml
- pandas
- fake-useragent

## Companies House API Setup (Recommended)
For more reliable results, use the official Companies House API:

1. Get a free API key at: https://developer.company-information.service.gov.uk/
2. Set the environment variable:
   ```bash
   export COMPANIES_HOUSE_API_KEY="your-api-key-here"
   ```
3. Run the scraper - it will automatically use the API

## Known Limitations
Many websites implement anti-bot protection that may block automated scraping:
- **Yell.com**: Often returns 403 Forbidden
- **Google**: May require CAPTCHA or block repeated requests
- **Companies House Website**: Generally accessible but may rate-limit
- **Companies House API**: Most reliable (requires free API key)

### Workarounds
1. Use the Companies House API (recommended)
2. Use a VPN or proxy service
3. Add delays between requests (already implemented)
4. Consider using Selenium for JavaScript-heavy sites
5. Run during off-peak hours
6. Use --verbose flag to debug issues

## Future Improvements
- Implement proxy rotation
- Add Selenium support for JavaScript-heavy sites
- Add Crunchbase and LinkedIn scrapers
- Export to additional formats (JSON, Excel)
