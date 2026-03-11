#!/usr/bin/env python3
"""
Business Lead Scraper for Office Leasing
Collects small business leads in Surrey, UK for flexible office space marketing.
"""

import argparse
import csv
import os
import sys
from datetime import datetime
from typing import List, Set

from src.models import BusinessLead
from src.scrapers import GoogleSearchScraper, YellScraper, CompaniesHouseScraper, CompaniesHouseAPIScraper, GooglePlacesScraper
from src.scrapers.ch_office_scraper import CHOfficeDiscoveryScraper, PlacesCrossReference
from src.enricher import LeadEnricher, batch_enrich_leads
from src.ai_scorer import AILeadScorer
from src.utils import save_leads_to_csv, load_existing_keys, is_target_sector, set_verbose, load_existing_leads_for_dedup, is_duplicate_lead, add_lead_to_existing
from config import OFFICE_TOWNS, OFFICE_GU_POSTCODES, OFFICE_OUTPUT_FILE, DEFAULT_TOWNS, WELLNESS_TOWNS

DEFAULT_OUTPUT = "leads.csv"

def create_scrapers(town: str, sector: str = "", use_api: bool = True, wellness_mode: bool = False) -> list:
    scrapers = []
    
    if use_api:
        if not wellness_mode:
            api_scraper = CompaniesHouseAPIScraper(town, sector)
            if api_scraper.is_available():
                scrapers.append(api_scraper)
            else:
                scrapers.append(CompaniesHouseScraper(town, sector))
        
        places_scraper = GooglePlacesScraper(town, sector, wellness_mode=wellness_mode)
        if places_scraper.is_available():
            scrapers.append(places_scraper)
    else:
        if not wellness_mode:
            scrapers.append(CompaniesHouseScraper(town, sector))
    
    if not wellness_mode:
        scrapers.append(YellScraper(town, sector))
        scrapers.append(GoogleSearchScraper(town, sector))
    
    return scrapers

def scrape_town(town: str, sector: str, max_pages: int, enrich: bool = True, use_api: bool = True, ai_score: bool = True, wellness_mode: bool = False, existing_data = None) -> List[BusinessLead]:
    print(f"\n{'='*60}")
    print(f"Scraping leads in: {town}")
    if wellness_mode:
        print(f"MODE: Wellness & Clinical businesses for Unit 8")
    print(f"{'='*60}")
    
    scrapers = create_scrapers(town, sector, use_api, wellness_mode)
    enricher = LeadEnricher() if enrich else None
    scorer = AILeadScorer(wellness_mode=wellness_mode) if ai_score else None
    leads = []
    skipped_duplicates = 0
    
    if scorer and scorer.enabled:
        if wellness_mode:
            print("  [AI Scoring] Enabled - leads will be scored for Unit 8 suitability")
        else:
            print("  [AI Scoring] Enabled - leads will be scored for office space potential")
    
    fallback_needed = False
    
    for scraper in scrapers:
        print(f"\n[{scraper.get_source_name()}] Starting scrape...")
        scraper_leads = 0
        try:
            for lead in scraper.scrape(max_pages=max_pages):
                if lead:
                    if existing_data and is_duplicate_lead(lead, existing_data):
                        skipped_duplicates += 1
                        continue
                    
                    if enricher:
                        lead = enricher.enrich(lead)
                    if scorer and scorer.enabled:
                        lead = scorer.score_lead(lead)
                    lead.category = "unit8" if wellness_mode else "office"
                    leads.append(lead)
                    
                    if existing_data:
                        add_lead_to_existing(lead, existing_data)
                    
                    scraper_leads += 1
                    score_info = f" [Score: {lead.ai_score}/10]" if lead.ai_score else ""
                    print(f"    + {lead.company_name} ({town}){score_info}")
            
            if hasattr(scraper, 'api_failed') and scraper.api_failed:
                fallback_needed = True
                
        except Exception as e:
            print(f"  Error with {scraper.get_source_name()}: {e}")
        
        if scraper_leads > 0:
            print(f"  [{scraper.get_source_name()}] Found {scraper_leads} new leads")
    
    if fallback_needed:
        print(f"\n[Companies House Web] Falling back to web scraper...")
        fallback_scraper = CompaniesHouseScraper(town, sector)
        try:
            for lead in fallback_scraper.scrape(max_pages=max_pages):
                if lead:
                    if existing_data and is_duplicate_lead(lead, existing_data):
                        skipped_duplicates += 1
                        continue
                    
                    if enricher:
                        lead = enricher.enrich(lead)
                    if scorer and scorer.enabled:
                        lead = scorer.score_lead(lead)
                    lead.category = "unit8" if wellness_mode else "office"
                    leads.append(lead)
                    
                    if existing_data:
                        add_lead_to_existing(lead, existing_data)
                    
                    score_info = f" [Score: {lead.ai_score}/10]" if lead.ai_score else ""
                    print(f"    + {lead.company_name} ({town}){score_info}")
        except Exception as e:
            print(f"  Error with fallback scraper: {e}")
    
    if skipped_duplicates > 0:
        print(f"  [Dedup] Skipped {skipped_duplicates} duplicates from existing data")
    
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

def load_leads_from_csv(filepath: str) -> List[BusinessLead]:
    import csv
    leads = []
    if not os.path.exists(filepath):
        return leads
    
    from dataclasses import fields as dc_fields
    all_fields = [f.name for f in dc_fields(BusinessLead)]
    
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            kwargs = {field: row.get(field, '') for field in all_fields}
            lead = BusinessLead(**kwargs)
            leads.append(lead)
    
    return leads

def dedupe_csv(filepath: str) -> int:
    """Remove duplicate leads from CSV, keeping the most enriched version."""
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return 0
    
    leads = load_leads_from_csv(filepath)
    original_count = len(leads)
    
    seen = {}
    for lead in leads:
        key = lead.company_name.lower().strip()
        if key not in seen:
            seen[key] = lead
        else:
            existing = seen[key]
            existing_score = (1 if existing.email else 0) + (1 if existing.contact_name else 0)
            new_score = (1 if lead.email else 0) + (1 if lead.contact_name else 0)
            if new_score > existing_score:
                seen[key] = lead
    
    unique_leads = list(seen.values())
    removed = original_count - len(unique_leads)
    
    if removed > 0:
        save_leads_to_csv(unique_leads, filepath, mode='w')
        print(f"Removed {removed} duplicates ({original_count} -> {len(unique_leads)} leads)")
    else:
        print(f"No duplicates found in {len(unique_leads)} leads")
    
    return removed

def run_batch_enrichment(filepath: str, verbose: bool = False, save_interval: int = 1):
    print(f"\n{'='*60}")
    print("BATCH ENRICHMENT MODE")
    print(f"{'='*60}")
    
    dedupe_csv(filepath)
    
    if os.path.exists("failed_urls.log"):
        os.remove("failed_urls.log")
    
    if not os.path.exists(filepath):
        print(f"Error: File not found: {filepath}")
        return
    
    leads = load_leads_from_csv(filepath)
    print(f"Loaded {len(leads)} leads from {filepath}")
    
    needs_enrichment = [l for l in leads if not l.contact_name or not l.email]
    print(f"Leads needing enrichment: {len(needs_enrichment)}")
    
    if not needs_enrichment:
        print("All leads already have contact name and email. Nothing to do.")
        return
    
    enriched_leads, stats = batch_enrich_leads(leads, skip_complete=True, filepath=filepath, save_interval=save_interval)
    
    print(f"\n{'='*60}")
    print("ENRICHMENT COMPLETE")
    print(f"{'='*60}")
    print(f"Total leads: {stats['total']}")
    print(f"Skipped (already complete): {stats['skipped']}")
    print(f"Enriched: {stats['enriched']}")
    print(f"  - Complete (email + contact): {stats['complete']}")
    print(f"  - Incomplete: {stats['incomplete']}")
    print(f"  - AI enriched: {stats['ai_enriched']}")
    print(f"\nSources used:")
    for source, count in stats['sources'].items():
        if count > 0:
            print(f"  - {source}: {count}")
    print(f"\nResults saved to: {filepath}")

def filter_qualified_leads(leads: List[BusinessLead], require_enrichment: bool = True) -> tuple:
    if not require_enrichment:
        return leads, []
    
    qualified = []
    filtered_out = []
    
    for lead in leads:
        has_email = bool(lead.email and '@' in lead.email)
        has_contact = bool(lead.contact_name and len(lead.contact_name.strip()) > 2)
        
        if has_email and has_contact:
            qualified.append(lead)
        else:
            filtered_out.append(lead)
    
    return qualified, filtered_out

def _checkpoint_office_csv(filepath, all_leads):
    import tempfile
    from src.utils import get_all_fieldnames
    tmp_path = filepath + ".tmp"
    fieldnames = get_all_fieldnames()
    try:
        with open(tmp_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            for lead in all_leads:
                if hasattr(lead, 'to_dict'):
                    writer.writerow(lead.to_dict())
                else:
                    writer.writerow(lead)
        os.replace(tmp_path, filepath)
    except Exception as e:
        print(f"  [Checkpoint] Warning: atomic save failed: {e}")
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def run_office_pipeline(args):
    print("="*60)
    print("OFFICE OCCUPIER PIPELINE (Companies House SIC Discovery)")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    output_file = args.output if args.output != DEFAULT_OUTPUT else OFFICE_OUTPUT_FILE
    enrich = not args.no_enrich
    ai_score = bool(os.environ.get("OPENAI_API_KEY"))

    ch_scraper = CHOfficeDiscoveryScraper()
    places = PlacesCrossReference()
    enricher = LeadEnricher() if enrich else None
    scorer = AILeadScorer(wellness_mode=False) if ai_score else None

    print(f"\nOutput file: {output_file}")
    print(f"Companies House API: {'Available' if ch_scraper.is_available() else 'NOT AVAILABLE'}")
    print(f"Google Places API: {'Available' if places.is_available() else 'NOT AVAILABLE'}")
    print(f"Enrichment: {'Enabled' if enrich else 'Disabled'}")
    print(f"AI Scoring: {'Enabled' if ai_score and scorer and scorer.enabled else 'Disabled'}")
    print(f"Postcodes: {', '.join(OFFICE_GU_POSTCODES)}")

    if not ch_scraper.is_available():
        print("\nERROR: COMPANIES_HOUSE_API_KEY required for office pipeline")
        return

    if args.fresh and os.path.exists(output_file) and not args.dry_run:
        os.remove(output_file)
        print(f"\nRemoved existing file: {output_file}")

    existing_leads = load_leads_from_csv(output_file) if os.path.exists(output_file) else []
    existing_names = {l.company_name.lower().strip() for l in existing_leads}
    existing_domains = set()
    for l in existing_leads:
        wk = l.get_website_key()
        if wk:
            existing_domains.add(wk)
    print(f"Existing office leads: {len(existing_leads)}")

    all_leads = []
    saved_count = 0

    def progress(count, name, director):
        d_str = f" (Dir: {director})" if director else ""
        print(f"  [{count}] {name}{d_str}")

    print(f"\n{'='*60}")
    print("Phase 1: Companies House SIC Discovery")
    print(f"{'='*60}")

    skipped = 0
    failed = 0

    for lead in ch_scraper.discover(postcodes=OFFICE_GU_POSTCODES, progress_callback=progress):
        name_key = lead.company_name.lower().strip()
        if name_key in existing_names:
            skipped += 1
            continue
        existing_names.add(name_key)

        try:
            if places.is_available():
                lead = places.lookup(lead)

            wk = lead.get_website_key()
            if wk and wk in existing_domains:
                skipped += 1
                continue
            if wk:
                existing_domains.add(wk)

            if enrich and enricher and lead.website:
                director_name_backup = lead.contact_name
                has_director = (
                    bool(director_name_backup and director_name_backup.strip())
                    and " " in director_name_backup.strip()
                    and not any(w in director_name_backup.lower() for w in ("ltd", "limited", "llp", "plc", "inc"))
                    and enricher._is_valid_contact_name(director_name_backup)
                )
                if has_director:
                    lead = enricher.enrich_office_fast(lead)
                    if not lead.email and not lead.generic_email:
                        lead = enricher.enrich(lead)
                        if director_name_backup and (not lead.contact_name or lead.contact_name == lead.company_name):
                            lead.contact_name = director_name_backup
                else:
                    lead = enricher.enrich(lead)
                    if director_name_backup and (not lead.contact_name or lead.contact_name == lead.company_name):
                        lead.contact_name = director_name_backup

            if not lead.geo_relevance:
                from src.geo_classifier import classify_from_website
                geo, geo_reason = classify_from_website(
                    website=lead.website,
                    location=lead.location,
                    sector=lead.sector,
                    generic_email=lead.generic_email,
                    company_name=lead.company_name,
                )
                lead.geo_relevance = geo
                if geo_reason:
                    existing = lead.refinement_notes or ""
                    note = f"geo:{geo_reason}"
                    lead.refinement_notes = f"{existing}; {note}".strip("; ") if existing else note

            if ai_score and scorer and scorer.enabled:
                lead = scorer.score_lead(lead)
        except Exception as e:
            failed += 1
            print(f"  [ERROR] Failed to enrich {lead.company_name}: {e} — saving raw CH record")

        lead.category = "office"

        if not lead.email:
            if lead.personal_email_guesses:
                first = lead.personal_email_guesses.split(",")[0].strip()
                if first and "@" in first:
                    lead.email = first
                    lead.email_guessed = "true"
            elif lead.generic_email:
                lead.email = lead.generic_email.split(",")[0].strip()

        has_real_email = (
            (lead.email and lead.email_guessed != "true")
            or lead.contact_email
            or lead.generic_email
        )
        has_any_email = bool(lead.email or lead.contact_email or lead.generic_email)
        has_name = bool(lead.contact_name and lead.contact_name != lead.company_name)

        if has_real_email and has_name:
            lead.enrichment_status = "complete"
        elif has_any_email and has_name:
            lead.enrichment_status = "guessed_email"
        elif has_any_email and not has_name:
            lead.enrichment_status = "missing_name"
        elif has_name and not has_any_email:
            lead.enrichment_status = "missing_email"
        else:
            lead.enrichment_status = "incomplete"

        all_leads.append(lead)

        if not args.dry_run:
            save_leads_to_csv([lead], output_file)
            saved_count += 1
            if saved_count % 25 == 0:
                _checkpoint_office_csv(output_file, existing_leads + all_leads)
                print(f"  [Checkpoint] {saved_count} leads saved to {output_file}")

    print(f"\n{'='*60}")
    print("OFFICE PIPELINE COMPLETE")
    print(f"{'='*60}")
    print(f"Total new leads: {len(all_leads)}")
    if skipped:
        print(f"Skipped (duplicates): {skipped}")
    if failed:
        print(f"Failed (errors): {failed}")
    total_with_name = sum(1 for l in all_leads if l.contact_name and l.contact_name != l.company_name)
    total_with_email = sum(1 for l in all_leads if l.email or l.contact_email or l.generic_email)
    total_with_website = sum(1 for l in all_leads if l.website)
    total_complete = sum(1 for l in all_leads if l.enrichment_status == "complete")
    print(f"With named contact: {total_with_name} ({total_with_name*100//max(len(all_leads),1)}%)")
    print(f"With email: {total_with_email} ({total_with_email*100//max(len(all_leads),1)}%)")
    print(f"With website: {total_with_website} ({total_with_website*100//max(len(all_leads),1)}%)")
    print(f"Complete: {total_complete} ({total_complete*100//max(len(all_leads),1)}%)")

    scored = [l for l in all_leads if l.ai_score]
    if scored:
        avg = sum(int(l.ai_score) for l in scored) / len(scored)
        print(f"Avg AI Score: {avg:.1f} ({len(scored)} scored)")

    if not args.dry_run and all_leads:
        _checkpoint_office_csv(output_file, existing_leads + all_leads)
        print(f"\n[Final Save] Atomic checkpoint: {len(existing_leads) + len(all_leads)} total leads in {output_file}")

    print(f"\nCH API stats: {ch_scraper.stats}")
    if places.is_available():
        print(f"Places stats: {places.stats}")

    if not args.dry_run:
        print(f"\nSaved to: {output_file}")
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


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
  python main.py --mode office            # Office pipeline via Companies House
  python main.py --wellness               # Wellness mode (Unit 8)
        """
    )
    
    parser.add_argument(
        '--mode',
        type=str,
        choices=['office', 'wellness'],
        help='Pipeline mode: "office" for CH SIC discovery, "wellness" for Unit 8 clinical leads'
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
    parser.add_argument(
        '--wellness',
        action='store_true',
        help='Search for wellness/clinical businesses suitable for Unit 8 (Godalming Business Centre)'
    )
    parser.add_argument(
        '--require-enrichment',
        action='store_true',
        help='Only save leads with both email and named contact'
    )
    parser.add_argument(
        '--enrich-existing',
        action='store_true',
        help='Re-enrich existing leads in CSV that are missing contact name or email'
    )
    parser.add_argument(
        '--save-interval',
        type=int,
        default=1,
        help='Save progress every N leads during enrichment (default: 1 = save after each lead)'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        set_verbose(True)

    if args.mode == 'office' or (args.mode == 'wellness'):
        if args.mode == 'wellness':
            args.wellness = True

    if args.mode == 'office':
        run_office_pipeline(args)
        return
    
    if args.wellness or args.mode == 'wellness':
        args.wellness = True
    
    print("="*60)
    print("Business Lead Scraper for Office Leasing")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    if args.enrich_existing:
        run_batch_enrichment(args.output, args.verbose, args.save_interval)
        return
    
    if args.town:
        towns = [args.town]
    elif args.wellness:
        towns = WELLNESS_TOWNS
    else:
        towns = DEFAULT_TOWNS
    
    api_key_present = bool(os.environ.get("COMPANIES_HOUSE_API_KEY"))
    places_key_present = bool(os.environ.get("GOOGLE_MAPS_API_KEY"))
    openai_key_present = bool(os.environ.get("OPENAI_API_KEY"))
    
    print(f"\nTarget towns: {', '.join(towns)}")
    if args.wellness:
        print(f"MODE: Wellness & Clinical leads for Unit 8 (Godalming Business Centre)")
    print(f"Sector filter: {args.sector or ('Wellness/Clinical businesses' if args.wellness else 'All professional services')}")
    print(f"Output file: {args.output}")
    print(f"Max pages per source: {args.pages}")
    print(f"Enrichment: {'Disabled' if args.no_enrich else 'Enabled'}")
    print(f"AI Lead Scoring: {'Enabled' if openai_key_present else 'Disabled (add OPENAI_API_KEY)'}")
    print(f"Verbose mode: {'Enabled' if args.verbose else 'Disabled'}")
    if not args.wellness:
        print(f"Companies House API: {'Available' if api_key_present else 'Not configured (using web scraper)'}")
    print(f"Google Places API: {'Available' if places_key_present else 'Not configured'}")
    if args.dry_run:
        print(f"DRY RUN MODE: Results will not be saved")
    
    if args.fresh and os.path.exists(args.output) and not args.dry_run:
        os.remove(args.output)
        print(f"\nRemoved existing file: {args.output}")
    
    existing_keys = load_existing_keys(args.output) if not args.dry_run else set()
    existing_data = load_existing_leads_for_dedup(args.output) if not args.dry_run else None
    print(f"Existing leads in file: {len(existing_keys)}")
    
    all_leads = []
    
    for town in towns:
        try:
            leads = scrape_town(
                town=town,
                sector=args.sector,
                max_pages=args.pages,
                enrich=not args.no_enrich,
                use_api=api_key_present or places_key_present,
                ai_score=openai_key_present,
                wellness_mode=args.wellness,
                existing_data=existing_data
            )
            all_leads.extend(leads)
            
            unique_leads = deduplicate_leads(leads, existing_keys)
            qualified_leads, filtered_out = filter_qualified_leads(unique_leads, args.require_enrichment)
            
            if qualified_leads and not args.dry_run:
                save_leads_to_csv(qualified_leads, args.output)
                for lead in qualified_leads:
                    existing_keys.add(lead.get_key())
                print(f"\nSaved {len(qualified_leads)} new leads from {town}")
                if filtered_out and args.require_enrichment:
                    print(f"  [Filter] {len(filtered_out)} leads skipped (missing email or contact)")
            elif qualified_leads and args.dry_run:
                print(f"\n[DRY RUN] Would save {len(qualified_leads)} new leads from {town}")
            
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
