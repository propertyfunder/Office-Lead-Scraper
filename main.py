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
from src.scrapers import GoogleSearchScraper, YellScraper, CompaniesHouseScraper, CompaniesHouseAPIScraper, GooglePlacesScraper
from src.enricher import LeadEnricher
from src.ai_scorer import AILeadScorer
from src.utils import save_leads_to_csv, load_existing_keys, is_target_sector, set_verbose

DEFAULT_TOWNS = ["Guildford", "Godalming", "Farnham", "Woking"]
DEFAULT_OUTPUT = "leads.csv"

def create_scrapers(town: str, sector: str = "", use_api: bool = True) -> list:
    scrapers = []
    
    if use_api:
        api_scraper = CompaniesHouseAPIScraper(town, sector)
        if api_scraper.is_available():
            scrapers.append(api_scraper)
        else:
            scrapers.append(CompaniesHouseScraper(town, sector))
        
        places_scraper = GooglePlacesScraper(town, sector)
        if places_scraper.is_available():
            scrapers.append(places_scraper)
    else:
        scrapers.append(CompaniesHouseScraper(town, sector))
    
    scrapers.append(YellScraper(town, sector))
    scrapers.append(GoogleSearchScraper(town, sector))
    
    return scrapers

def scrape_town(town: str, sector: str, max_pages: int, enrich: bool = True, use_api: bool = True, ai_score: bool = True) -> List[BusinessLead]:
    print(f"\n{'='*60}")
    print(f"Scraping leads in: {town}")
    print(f"{'='*60}")
    
    scrapers = create_scrapers(town, sector, use_api)
    enricher = LeadEnricher() if enrich else None
    scorer = AILeadScorer() if ai_score else None
    leads = []
    
    if scorer and scorer.enabled:
        print("  [AI Scoring] Enabled - leads will be scored for office space potential")
    
    fallback_needed = False
    
    for scraper in scrapers:
        print(f"\n[{scraper.get_source_name()}] Starting scrape...")
        scraper_leads = 0
        try:
            for lead in scraper.scrape(max_pages=max_pages):
                if lead:
                    if enricher:
                        lead = enricher.enrich(lead)
                    if scorer and scorer.enabled:
                        lead = scorer.score_lead(lead)
                    leads.append(lead)
                    scraper_leads += 1
                    score_info = f" [Score: {lead.ai_score}/10]" if lead.ai_score else ""
                    print(f"    + {lead.company_name}{score_info}")
            
            if hasattr(scraper, 'api_failed') and scraper.api_failed:
                fallback_needed = True
                
        except Exception as e:
            print(f"  Error with {scraper.get_source_name()}: {e}")
        
        if scraper_leads > 0:
            print(f"  [{scraper.get_source_name()}] Found {scraper_leads} leads")
    
    if fallback_needed:
        print(f"\n[Companies House Web] Falling back to web scraper...")
        fallback_scraper = CompaniesHouseScraper(town, sector)
        try:
            for lead in fallback_scraper.scrape(max_pages=max_pages):
                if lead:
                    if enricher:
                        lead = enricher.enrich(lead)
                    if scorer and scorer.enabled:
                        lead = scorer.score_lead(lead)
                    leads.append(lead)
                    score_info = f" [Score: {lead.ai_score}/10]" if lead.ai_score else ""
                    print(f"    + {lead.company_name}{score_info}")
        except Exception as e:
            print(f"  Error with fallback scraper: {e}")
    
    return leads

def deduplicate_leads(leads: List[BusinessLead], existing_keys: Set[str]) -> List[BusinessLead]:
    unique_leads = []
    seen_keys = existing_keys.copy()
    seen_names = {k.split('|')[0] for k in seen_keys}
    
    for lead in leads:
        key = lead.get_key()
        name_key = lead.company_name.lower().strip()
        
        if key not in seen_keys and name_key not in seen_names:
            seen_keys.add(key)
            seen_names.add(name_key)
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
  python main.py --verbose                # Show debug output
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
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Show detailed debug output'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Test scraping without saving to CSV'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        set_verbose(True)
    
    print("="*60)
    print("Business Lead Scraper for Office Leasing")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    towns = [args.town] if args.town else DEFAULT_TOWNS
    
    api_key_present = bool(os.environ.get("COMPANIES_HOUSE_API_KEY"))
    places_key_present = bool(os.environ.get("GOOGLE_MAPS_API_KEY"))
    openai_key_present = bool(os.environ.get("OPENAI_API_KEY"))
    
    print(f"\nTarget towns: {', '.join(towns)}")
    print(f"Sector filter: {args.sector or 'All professional services'}")
    print(f"Output file: {args.output}")
    print(f"Max pages per source: {args.pages}")
    print(f"Enrichment: {'Disabled' if args.no_enrich else 'Enabled'}")
    print(f"AI Lead Scoring: {'Enabled' if openai_key_present else 'Disabled (add OPENAI_API_KEY)'}")
    print(f"Verbose mode: {'Enabled' if args.verbose else 'Disabled'}")
    print(f"Companies House API: {'Available' if api_key_present else 'Not configured (using web scraper)'}")
    print(f"Google Places API: {'Available' if places_key_present else 'Not configured'}")
    if args.dry_run:
        print(f"DRY RUN MODE: Results will not be saved")
    
    if args.fresh and os.path.exists(args.output) and not args.dry_run:
        os.remove(args.output)
        print(f"\nRemoved existing file: {args.output}")
    
    existing_keys = load_existing_keys(args.output) if not args.dry_run else set()
    print(f"Existing leads in file: {len(existing_keys)}")
    
    all_leads = []
    
    for town in towns:
        try:
            leads = scrape_town(
                town=town,
                sector=args.sector,
                max_pages=args.pages,
                enrich=not args.no_enrich,
                use_api=api_key_present,
                ai_score=openai_key_present
            )
            all_leads.extend(leads)
            
            unique_leads = deduplicate_leads(leads, existing_keys)
            if unique_leads and not args.dry_run:
                save_leads_to_csv(unique_leads, args.output)
                for lead in unique_leads:
                    existing_keys.add(lead.get_key())
                print(f"\nSaved {len(unique_leads)} new leads from {town}")
            elif unique_leads and args.dry_run:
                print(f"\n[DRY RUN] Would save {len(unique_leads)} new leads from {town}")
            
        except KeyboardInterrupt:
            print("\n\nScraping interrupted by user. Saving progress...")
            break
        except Exception as e:
            print(f"\nError scraping {town}: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            continue
    
    unique_total = deduplicate_leads(all_leads, set())
    
    print("\n" + "="*60)
    print("SCRAPING COMPLETE")
    print("="*60)
    print(f"Total leads found: {len(all_leads)}")
    print(f"Unique leads (this session): {len(unique_total)}")
    if not args.dry_run:
        print(f"Output saved to: {args.output}")
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if len(all_leads) == 0:
        print("\n" + "!"*60)
        print("WARNING: No leads were collected!")
        print("!"*60)
        print("This often happens because websites block automated requests.")
        print("\nPossible solutions:")
        print("  1. Set COMPANIES_HOUSE_API_KEY environment variable")
        print("     Get a free key: https://developer.company-information.service.gov.uk/")
        print("  2. Use a VPN or proxy service")
        print("  3. Wait and try again later (rate limiting)")
        print("  4. Use --verbose flag to see detailed debug info")
        print("  5. Try different sectors with --sector")
        print("\nNote: Many websites have anti-bot protection. This is normal.")

if __name__ == "__main__":
    main()
