#!/usr/bin/env python3
import csv
import os
import sys
from dataclasses import fields as dc_fields

from src.models import BusinessLead
from src.geo_classifier import classify_from_website
from src.utils import rate_limit

OFFICE_CSV = "office_leads.csv"


def load_leads(filepath):
    leads = []
    if not os.path.exists(filepath):
        return leads
    all_fields = [f.name for f in dc_fields(BusinessLead)]
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            kwargs = {field: row.get(field, "") for field in all_fields}
            leads.append(BusinessLead(**kwargs))
    return leads


def save_leads(leads, filepath):
    tmp_path = filepath + ".tmp"
    all_fields = [f.name for f in dc_fields(BusinessLead)]
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_fields, extrasaction="ignore")
        writer.writeheader()
        for lead in leads:
            writer.writerow(lead.to_dict())
    os.replace(tmp_path, filepath)


def main():
    force = "--force" in sys.argv
    stats_only = "--stats" in sys.argv

    filepath = OFFICE_CSV
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return

    leads = load_leads(filepath)
    print(f"Loaded {len(leads)} leads from {filepath}")

    already_classified = sum(1 for l in leads if l.geo_relevance)
    needs_classification = [l for l in leads if not l.geo_relevance or force]

    if stats_only:
        local_count = sum(1 for l in leads if l.geo_relevance == "local")
        review_count = sum(1 for l in leads if l.geo_relevance == "review")
        exclude_count = sum(1 for l in leads if l.geo_relevance == "exclude")
        unclassified = sum(1 for l in leads if not l.geo_relevance)
        print(f"\nGeo Relevance Stats:")
        print(f"  Local:        {local_count}")
        print(f"  Review:       {review_count}")
        print(f"  Exclude:      {exclude_count}")
        print(f"  Unclassified: {unclassified}")
        print(f"  Total:        {len(leads)}")
        return

    print(f"Already classified: {already_classified}")
    print(f"Needs classification: {len(needs_classification)}")

    if not needs_classification:
        print("All leads already classified. Use --force to reclassify.")
        return

    classified = 0
    for i, lead in enumerate(leads):
        if lead.geo_relevance and not force:
            continue

        print(f"  [{i+1}/{len(leads)}] {lead.company_name}...", end=" ")

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
            if "geo:" in existing:
                parts = [p.strip() for p in existing.split(";") if not p.strip().startswith("geo:")]
                existing = "; ".join(parts)
            note = f"geo:{geo_reason}"
            lead.refinement_notes = f"{existing}; {note}".strip("; ") if existing else note

        print(f"-> {geo}" + (f" ({geo_reason[:60]})" if geo_reason else ""))
        classified += 1

        if classified % 10 == 0:
            save_leads(leads, filepath)
            print(f"  [Checkpoint] Saved after {classified} classifications")

        rate_limit(0.3, 0.8)

    save_leads(leads, filepath)

    local_count = sum(1 for l in leads if l.geo_relevance == "local")
    review_count = sum(1 for l in leads if l.geo_relevance == "review")
    exclude_count = sum(1 for l in leads if l.geo_relevance == "exclude")

    print(f"\nClassification Complete:")
    print(f"  Local:   {local_count}")
    print(f"  Review:  {review_count}")
    print(f"  Exclude: {exclude_count}")
    print(f"  Total:   {len(leads)}")
    print(f"Saved to: {filepath}")


if __name__ == "__main__":
    main()
