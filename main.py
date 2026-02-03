#!/usr/bin/env python3
"""
Business Lead Scraper for Office Leasing
Collects small business leads in Surrey, UK for flexible office space marketing.
"""

import argparse
import os
import sys
from datetime import datetime
from typing import List, Set

from src.models import BusinessLead
from src.scrapers import GoogleSearchScraper, YellScraper, CompaniesHouseScraper
from src.enricher import LeadEnricher
from src.utils import save_leads_to_csv, load_existing_keys, is_target_sector

DEFAULT_TOWNS = ["Guildford", "Godalming", "Farnham", "Woking"]
DEFAULT_OUTPUT = "leads.csv"

def create_scrapers(town: str, sector: str = "") -> list:
    return [
        YellScraper(town, sector),
        CompaniesHouseScraper(town, sector),
        GoogleSearchScraper(town, sector),
    ]

def scrape_town(town: str, sector: str, max_pages: int, enrich: bool = True) -> List[BusinessLead]:
    print(f"\n{'='*60}")
    print(f"Scraping leads in: {town}")
    print(f"{'='*60}")
    
    scrapers = create_scrapers(town, sector)
    enricher = LeadEnricher() if enrich else None
    leads = []
    
    for scraper in scrapers:
        print(f"\n[{scraper.get_source_name()}] Starting scrape...")
        try:
            for lead in scraper.scrape(max_pages=max_pages):
                if lead:
                    if enricher:
                        lead = enricher.enrich(lead)
                    leads.append(lead)
                    print(f"  Found: {lead.company_name}")
        except Exception as e:
            print(f"  Error with {scraper.get_source_name()}: {e}")
    
    return leads

def deduplicate_leads(leads: List[BusinessLead], existing_keys: Set[str]) -> List[BusinessLead]:
    unique_leads = []
    seen_keys = existing_keys.copy()
    
    for lead in leads:
        key = lead.get_key()
        name_key = lead.company_name.lower().strip()
        
        if key not in seen_keys and name_key not in [k.split('|')[0] for k in seen_keys]:
            seen_keys.add(key)
            unique_leads.append(lead)
    
    return unique_leads

def main():
    parser = argparse.ArgumentParser(
        description="Scrape small business leads for office leasing in Surrey, UK",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                          # Scrape all default towns
  python main.py --town Guildford         # Scrape only Guildford
  python main.py --sector "IT companies"  # Focus on IT sector
  python main.py --pages 5 --no-enrich    # More pages, skip enrichment
        """
    )
    
    parser.add_argument(
        '--town', '-t',
        type=str,
        help='Target town to scrape (default: all Surrey towns)'
    )
    parser.add_argument(
        '--sector', '-s',
        type=str,
        default='',
        help='Specific sector to focus on (e.g., "IT companies", "accountants")'
    )
    parser.add_argument(
        '--output', '-o',
        type=str,
        default=DEFAULT_OUTPUT,
        help=f'Output CSV file (default: {DEFAULT_OUTPUT})'
    )
    parser.add_argument(
        '--pages', '-p',
        type=int,
        default=2,
        help='Maximum pages to scrape per source (default: 2)'
    )
    parser.add_argument(
        '--no-enrich',
        action='store_true',
        help='Skip enrichment (faster but less data)'
    )
    parser.add_argument(
        '--fresh',
        action='store_true',
        help='Start fresh (overwrite existing CSV)'
    )
    
    args = parser.parse_args()
    
    print("="*60)
    print("Business Lead Scraper for Office Leasing")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    towns = [args.town] if args.town else DEFAULT_TOWNS
    
    print(f"\nTarget towns: {', '.join(towns)}")
    print(f"Sector filter: {args.sector or 'All professional services'}")
    print(f"Output file: {args.output}")
    print(f"Max pages per source: {args.pages}")
    print(f"Enrichment: {'Disabled' if args.no_enrich else 'Enabled'}")
    
    if args.fresh and os.path.exists(args.output):
        os.remove(args.output)
        print(f"\nRemoved existing file: {args.output}")
    
    existing_keys = load_existing_keys(args.output)
    print(f"Existing leads in file: {len(existing_keys)}")
    
    all_leads = []
    
    for town in towns:
        try:
            leads = scrape_town(
                town=town,
                sector=args.sector,
                max_pages=args.pages,
                enrich=not args.no_enrich
            )
            all_leads.extend(leads)
            
            unique_leads = deduplicate_leads(leads, existing_keys)
            if unique_leads:
                save_leads_to_csv(unique_leads, args.output)
                for lead in unique_leads:
                    existing_keys.add(lead.get_key())
                print(f"\nSaved {len(unique_leads)} new leads from {town}")
            
        except KeyboardInterrupt:
            print("\n\nScraping interrupted by user. Saving progress...")
            break
        except Exception as e:
            print(f"\nError scraping {town}: {e}")
            continue
    
    unique_total = deduplicate_leads(all_leads, set())
    
    print("\n" + "="*60)
    print("SCRAPING COMPLETE")
    print("="*60)
    print(f"Total leads found: {len(all_leads)}")
    print(f"Unique leads (this session): {len(unique_total)}")
    print(f"Output saved to: {args.output}")
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if len(all_leads) == 0:
        print("\n" + "!"*60)
        print("WARNING: No leads were collected!")
        print("!"*60)
        print("This often happens because websites block automated requests.")
        print("\nPossible solutions:")
        print("  1. Wait and try again later (rate limiting)")
        print("  2. Use a VPN or proxy service")
        print("  3. Use the --no-enrich flag to reduce requests")
        print("  4. Try different sectors with --sector")
        print("  5. Consider using a headless browser (Selenium)")
        print("\nNote: Many websites have anti-bot protection that blocks")
        print("      scrapers. This is normal and the code is working correctly.")

if __name__ == "__main__":
    main()
