import os
import re
import time
import requests
from typing import Generator, Optional, List
from ..models import BusinessLead
from ..utils import rate_limit, clean_text, log_verbose

try:
    from config import OFFICE_SIC_CODES, OFFICE_GU_POSTCODES, SIC_CODE_TO_SECTOR, OFFICE_SIC_CODES_FLAT
except ImportError:
    OFFICE_SIC_CODES = {}
    OFFICE_GU_POSTCODES = []
    SIC_CODE_TO_SECTOR = {}
    OFFICE_SIC_CODES_FLAT = []


class CHOfficeDiscoveryScraper:
    def __init__(self, api_key: str = ""):
        self.api_key = api_key or os.environ.get("COMPANIES_HOUSE_API_KEY", "")
        self.base_url = "https://api.company-information.service.gov.uk"
        self.source_name = "Companies House"
        self.seen_numbers = set()
        self.stats = {
            "api_calls": 0,
            "companies_found": 0,
            "directors_found": 0,
            "errors": 0,
            "rate_limits": 0,
        }

    def is_available(self) -> bool:
        return bool(self.api_key)

    def get_source_name(self) -> str:
        return self.source_name

    def _api_get(self, endpoint: str, params: dict = None, _retries: int = 0) -> Optional[dict]:
        self.stats["api_calls"] += 1
        try:
            r = requests.get(
                f"{self.base_url}{endpoint}",
                params=params,
                auth=(self.api_key, ""),
                timeout=15,
            )
            if r.status_code == 429:
                self.stats["rate_limits"] += 1
                if _retries >= 3:
                    print("  [CH] Rate limited 3 times — skipping request")
                    return None
                wait = 30 * (2 ** _retries)
                print(f"  [CH] Rate limited — waiting {wait}s (retry {_retries + 1}/3)")
                time.sleep(wait)
                return self._api_get(endpoint, params, _retries + 1)
            if r.status_code == 401:
                print("  [CH] Invalid API key")
                return None
            if r.status_code != 200:
                self.stats["errors"] += 1
                log_verbose(f"CH API {endpoint} returned {r.status_code}")
                return None
            return r.json()
        except requests.exceptions.Timeout:
            self.stats["errors"] += 1
            log_verbose(f"CH API timeout: {endpoint}")
            return None
        except Exception as e:
            self.stats["errors"] += 1
            log_verbose(f"CH API error: {e}")
            return None

    def _is_gu_postcode(self, postcode: str) -> bool:
        if not postcode:
            return False
        pc = postcode.upper().strip().replace(" ", "")
        for prefix in OFFICE_GU_POSTCODES:
            p = prefix.upper().replace(" ", "")
            if pc.startswith(p) and len(pc) > len(p):
                rest = pc[len(p):]
                if rest and rest[0].isdigit() is False:
                    return True
                if rest and rest[0].isdigit():
                    return True
        return False

    def _format_director_name(self, raw_name: str) -> str:
        if not raw_name:
            return ""
        raw_name = raw_name.strip()
        if "," in raw_name:
            parts = raw_name.split(",", 1)
            surname = parts[0].strip().title()
            forenames = parts[1].strip().title()
            return f"{forenames} {surname}"
        return raw_name.title()

    def _get_directors(self, company_number: str) -> List[dict]:
        data = self._api_get(f"/company/{company_number}/officers", {"items_per_page": 50})
        if not data:
            return []

        directors = []
        for officer in data.get("items", []):
            if officer.get("resigned_on"):
                continue
            role = (officer.get("officer_role", "") or "").lower()
            if role not in ("director", "managing-director", "corporate-director", "secretary"):
                continue
            name = officer.get("name", "")
            if not name:
                continue
            formatted = self._format_director_name(name)
            if formatted and len(formatted) > 3 and " " in formatted:
                directors.append({
                    "name": formatted,
                    "role": role,
                    "appointed": officer.get("appointed_on", ""),
                })

        directors.sort(key=lambda d: d.get("appointed", ""), reverse=True)
        return directors

    def _sic_to_sector(self, sic_codes: list) -> str:
        for code in sic_codes:
            if code in SIC_CODE_TO_SECTOR:
                return SIC_CODE_TO_SECTOR[code]
        return "Professional Services"

    def _build_address(self, addr: dict) -> str:
        parts = []
        for key in ["address_line_1", "address_line_2", "locality", "region", "postal_code"]:
            val = addr.get(key, "")
            if val:
                parts.append(val)
        return ", ".join(parts)

    def discover(self, postcodes: list = None, progress_callback=None) -> Generator[BusinessLead, None, None]:
        if not self.api_key:
            print("  [CH] No API key — set COMPANIES_HOUSE_API_KEY")
            return

        target_postcodes = postcodes or OFFICE_GU_POSTCODES
        sic_str = ",".join(OFFICE_SIC_CODES_FLAT)
        total_yielded = 0

        for pc in target_postcodes:
            start_index = 0
            page_size = 100

            while True:
                data = self._api_get("/advanced-search/companies", {
                    "sic_codes": sic_str,
                    "location": pc,
                    "company_status": "active",
                    "size": page_size,
                    "start_index": start_index,
                })
                if not data:
                    break

                items = data.get("items", [])
                if not items:
                    break

                for company in items:
                    company_number = company.get("company_number", "")
                    if company_number in self.seen_numbers:
                        continue
                    self.seen_numbers.add(company_number)

                    addr = company.get("registered_office_address", {})
                    postcode = addr.get("postal_code", "")
                    if not self._is_gu_postcode(postcode):
                        continue

                    company_name = clean_text(company.get("company_name", ""))
                    if not company_name:
                        continue

                    sic_codes = company.get("sic_codes", [])
                    sector = self._sic_to_sector(sic_codes)
                    location = self._build_address(addr)
                    date_created = company.get("date_of_creation", "")

                    directors = self._get_directors(company_number)
                    self.stats["companies_found"] += 1
                    director_name = ""
                    if directors:
                        director_name = directors[0]["name"]
                        self.stats["directors_found"] += 1

                    lead = BusinessLead(
                        company_name=company_name,
                        sector=sector,
                        location=location,
                        contact_name=director_name,
                        source=self.source_name,
                        category="office",
                        enrichment_source="companies_house",
                    )
                    lead.enrichment_status = "missing_email"
                    if not director_name:
                        lead.enrichment_status = "missing_name"

                    total_yielded += 1
                    if progress_callback:
                        progress_callback(total_yielded, company_name, director_name)

                    yield lead
                    rate_limit(0.3, 0.5)

                if len(items) < page_size:
                    break
                start_index += page_size
                rate_limit(0.5, 1.0)

        print(f"  [CH] Discovery complete: {self.stats['companies_found']} companies, "
              f"{self.stats['directors_found']} directors, "
              f"{self.stats['api_calls']} API calls, "
              f"{self.stats['errors']} errors")


class PlacesCrossReference:
    def __init__(self, api_key: str = ""):
        self.api_key = api_key or os.environ.get("GOOGLE_MAPS_API_KEY", "")
        self.stats = {"lookups": 0, "matches": 0, "errors": 0}

    def is_available(self) -> bool:
        return bool(self.api_key)

    def lookup(self, lead: BusinessLead) -> BusinessLead:
        if not self.api_key:
            return lead

        self.stats["lookups"] += 1
        query = f"{lead.company_name} {lead.location.split(',')[0] if lead.location else ''}"

        try:
            r = requests.post(
                "https://places.googleapis.com/v1/places:searchText",
                headers={
                    "X-Goog-Api-Key": self.api_key,
                    "X-Goog-FieldMask": "places.displayName,places.websiteUri,places.nationalPhoneNumber,places.rating,places.userRatingCount",
                },
                json={"textQuery": query, "maxResultCount": 3},
                timeout=15,
            )

            if r.status_code != 200:
                self.stats["errors"] += 1
                return lead

            data = r.json()
            places = data.get("places", [])
            if not places:
                return lead

            best = self._find_best_match(lead.company_name, places)
            if not best:
                return lead

            self.stats["matches"] += 1

            website = best.get("websiteUri", "")
            if website:
                lead.website = website

            phone = best.get("nationalPhoneNumber", "")
            if phone:
                lead.phone = phone

            rating = best.get("rating")
            review_count = best.get("userRatingCount")
            if rating:
                rating_str = f"{rating}/5"
                if review_count:
                    rating_str += f" ({review_count} reviews)"
                lead.google_rating = rating_str

        except Exception as e:
            self.stats["errors"] += 1
            log_verbose(f"Places lookup error for {lead.company_name}: {e}")

        rate_limit(0.1, 0.3)
        return lead

    def _find_best_match(self, company_name: str, places: list) -> Optional[dict]:
        cn_lower = company_name.lower().strip()
        cn_words = set(re.sub(r'[^a-z0-9\s]', '', cn_lower).split())
        cn_words -= {"ltd", "limited", "plc", "llp", "uk", "the", "and"}

        for place in places:
            display = (place.get("displayName", {}).get("text", "") or "").lower()
            display_words = set(re.sub(r'[^a-z0-9\s]', '', display).split())
            display_words -= {"ltd", "limited", "plc", "llp", "uk", "the", "and"}

            if not cn_words or not display_words:
                continue

            overlap = cn_words & display_words
            score = len(overlap) / max(len(cn_words), 1)

            if score >= 0.5:
                return place

        return None
