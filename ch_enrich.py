import csv
import os
import sys
import time
from src.enricher import LeadEnricher
from src.utils import extract_domain, guess_email, clean_email, normalize_name

INPUT_FILE = "unit8_leads_enriched.csv"
OUTPUT_FILE = "unit8_leads_enriched.csv"
BACKUP_FILE = "unit8_leads_enriched_backup.csv"

def main():
    enricher = LeadEnricher()
    if not enricher.companies_house_api_key:
        print("ERROR: COMPANIES_HOUSE_API_KEY not set")
        sys.exit(1)

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    import shutil
    shutil.copy2(INPUT_FILE, BACKUP_FILE)
    print(f"Backed up {len(rows)} leads to {BACKUP_FILE}")

    already_have = sum(1 for r in rows if r.get('principal_name', '').strip())
    need_ch = [i for i, r in enumerate(rows) if not r.get('principal_name', '').strip()]
    print(f"Already have CH director: {already_have}")
    print(f"Need CH lookup: {len(need_ch)}")

    if not need_ch:
        print("All leads already have principal_name. Nothing to do.")
        return

    found = 0
    errors = 0
    save_interval = 50

    for count, idx in enumerate(need_ch, 1):
        row = rows[idx]
        company = row.get('company_name', '')
        if not company.strip():
            continue

        try:
            director = enricher._get_director_from_companies_house(company)
            if director and enricher._is_valid_contact_name(director):
                ch_name = normalize_name(director)
                row['principal_name'] = ch_name

                website = row.get('website', '').strip()
                if website:
                    domain = extract_domain(website)
                    if domain:
                        guessed = guess_email(company, ch_name, domain)
                        if guessed:
                            row['principal_email_guess'] = clean_email(guessed)

                found += 1
                print(f"  [{count}/{len(need_ch)}] {company} -> {ch_name}")
            else:
                if director:
                    print(f"  [{count}/{len(need_ch)}] {company} -> invalid name: {director}")
                else:
                    print(f"  [{count}/{len(need_ch)}] {company} -> not found")
        except Exception as e:
            errors += 1
            print(f"  [{count}/{len(need_ch)}] {company} -> ERROR: {e}")
            if "429" in str(e) or "rate" in str(e).lower():
                print("  Rate limited, waiting 30s...")
                time.sleep(30)

        if count % save_interval == 0:
            _save(rows, fieldnames)
            print(f"  --- Saved progress: {found} found so far ---")

    _save(rows, fieldnames)
    print(f"\nDone! Found {found} directors out of {len(need_ch)} lookups ({errors} errors)")
    print(f"Results saved to {OUTPUT_FILE}")


def _save(rows, fieldnames):
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
