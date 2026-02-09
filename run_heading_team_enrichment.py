#!/usr/bin/env python3
"""
Targeted re-enrichment script for leads missing contact_name.
Tests the heading-team detection feature at scale.
Saves after every lead and is resumable.
"""

import sys
import os
import time
from datetime import datetime

sys.path.insert(0, '.')
from main import load_leads_from_csv
from src.enricher import LeadEnricher
from src.utils import save_leads_to_csv

INPUT_FILE = 'unit8_leads_enriched.csv'
OUTPUT_FILE = 'unit8_leads_enriched.csv'

def _is_empty(val):
    return not val or str(val).strip() in ('', 'nan', 'None')

def main():
    print("=" * 60)
    print("TARGETED RE-ENRICHMENT: Missing Contact Names")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    leads = load_leads_from_csv(INPUT_FILE)
    print(f"Loaded {len(leads)} leads")

    targets = []
    for idx, lead in enumerate(leads):
        if _is_empty(lead.contact_name) and lead.website:
            already_attempted = (lead.enrichment_attempts and 
                               lead.enrichment_attempts.isdigit() and
                               int(lead.enrichment_attempts) > 0 and
                               lead.contact_source == 'website_team_heading')
            if not already_attempted:
                targets.append(idx)

    print(f"Leads to process: {len(targets)}")

    enricher = LeadEnricher()
    stats = {
        'processed': 0,
        'gained_contact': 0,
        'heading_team_source': 0,
        'gained_email': 0,
        'errors': 0,
    }

    start_time = time.time()

    for count, idx in enumerate(targets):
        lead = leads[idx]
        stats['processed'] += 1
        try:
            old_contact = lead.contact_name
            old_email = lead.email
            enricher.enrich(lead, skip_if_complete=False)

            if not _is_empty(lead.contact_name) and _is_empty(old_contact):
                stats['gained_contact'] += 1
            if not _is_empty(lead.email) and _is_empty(old_email):
                stats['gained_email'] += 1
            if lead.contact_source == 'website_team_heading':
                stats['heading_team_source'] += 1

        except Exception as e:
            stats['errors'] += 1
            print(f"    ERROR enriching {lead.company_name}: {e}")

        if stats['processed'] % 5 == 0:
            save_leads_to_csv(leads, OUTPUT_FILE, mode='w')

        if stats['processed'] % 10 == 0:
            elapsed = time.time() - start_time
            rate = stats['processed'] / elapsed * 60 if elapsed > 0 else 0
            print(f"\n  --- Progress: {stats['processed']}/{len(targets)} "
                  f"({rate:.0f}/min) | Gained: {stats['gained_contact']} | "
                  f"HeadingTeam: {stats['heading_team_source']} | "
                  f"Errors: {stats['errors']} ---\n")

    save_leads_to_csv(leads, OUTPUT_FILE, mode='w')

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print("RE-ENRICHMENT COMPLETE")
    print(f"{'=' * 60}")
    print(f"Duration: {elapsed/60:.1f} minutes")
    print(f"Processed: {stats['processed']}")
    print(f"Gained contact_name: {stats['gained_contact']}")
    print(f"Heading team sourced: {stats['heading_team_source']}")
    print(f"Gained email: {stats['gained_email']}")
    print(f"Errors: {stats['errors']}")
    print(f"\nSaved to: {OUTPUT_FILE}")

    with open('enrichment_run_stats.txt', 'w') as f:
        f.write(f"Run completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Duration: {elapsed/60:.1f} minutes\n")
        for k, v in stats.items():
            f.write(f"{k}: {v}\n")

if __name__ == '__main__':
    main()
