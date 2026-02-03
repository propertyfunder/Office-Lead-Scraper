import os
import re
from typing import Generator, Optional
import requests

from .base_scraper import BaseScraper
from ..models import BusinessLead
from ..utils import rate_limit, clean_text, log_verbose

class CompaniesHouseAPIScraper(BaseScraper):
    def __init__(self, town: str, sector: str = "", api_key: str = ""):
        super().__init__(town, sector)
        self.source_name = "Companies House API"
        self.api_key = api_key or os.environ.get("COMPANIES_HOUSE_API_KEY", "")
        self.base_url = "https://api.company-information.service.gov.uk"
        
        self.sic_codes = {
            "consulting": ["70229", "70220", "70210"],
            "technology": ["62011", "62012", "62020", "62090"],
            "software": ["62011", "62012"],
            "legal": ["69101", "69102", "69109"],
            "accounting": ["69201", "69202", "69203"],
            "recruitment": ["78100", "78200", "78300"],
            "marketing": ["73110", "73120", "73200"],
            "engineering": ["71121", "71122", "71129"],
            "environmental": ["71112", "74909"],
        }
    
    def is_available(self) -> bool:
        return bool(self.api_key)
    
    @property
    def api_failed(self) -> bool:
        return getattr(self, '_api_failed', False)
    
    @api_failed.setter
    def api_failed(self, value: bool):
        self._api_failed = value
    
    def scrape(self, max_pages: int = 3) -> Generator[BusinessLead, None, None]:
        if not self.api_key:
            print(f"  [Companies House API] No API key found. Set COMPANIES_HOUSE_API_KEY env var.")
            print(f"  [Companies House API] Get a free key at: https://developer.company-information.service.gov.uk/")
            self.api_failed = True
            return
        
        if not self._test_api_connection():
            print(f"  [Companies House API] API connection failed - will fallback to web scraper")
            self.api_failed = True
            return
        
        search_terms = self._get_search_terms()
        total_found = 0
        errors = 0
        
        for term in search_terms:
            print(f"  [Companies House API] Searching: {term}")
            
            for page in range(max_pages):
                start_index = page * 20
                
                try:
                    response = requests.get(
                        f"{self.base_url}/search/companies",
                        params={
                            "q": term,
                            "items_per_page": 20,
                            "start_index": start_index,
                        },
                        auth=(self.api_key, ""),
                        timeout=15
                    )
                    
                    if response.status_code == 401:
                        print(f"  [Companies House API] Invalid API key - check COMPANIES_HOUSE_API_KEY")
                        self.api_failed = True
                        return
                    
                    if response.status_code == 429:
                        print(f"  [Companies House API] Rate limited - waiting before retry")
                        rate_limit(5.0, 10.0)
                        continue
                    
                    if response.status_code != 200:
                        errors += 1
                        print(f"  [Companies House API] Warning: API returned status {response.status_code}")
                        if errors > 3:
                            print(f"  [Companies House API] Too many errors - stopping")
                            break
                        continue
                    
                    data = response.json()
                    items = data.get("items", [])
                    
                    if not items:
                        break
                    
                    for company in items:
                        lead = self._parse_company(company)
                        if lead and self._is_in_target_area(lead.location):
                            total_found += 1
                            yield lead
                    
                    rate_limit(0.5, 1.0)
                    
                except requests.exceptions.Timeout:
                    print(f"  [Companies House API] Request timeout - continuing")
                    continue
                except requests.exceptions.RequestException as e:
                    print(f"  [Companies House API] Network error: {str(e)[:50]}")
                    errors += 1
                    continue
                except Exception as e:
                    log_verbose(f"API error: {e}")
                    continue
        
        print(f"  [Companies House API] Total found: {total_found} leads")
    
    def _test_api_connection(self) -> bool:
        try:
            response = requests.get(
                f"{self.base_url}/search/companies",
                params={"q": "test", "items_per_page": 1},
                auth=(self.api_key, ""),
                timeout=10
            )
            if response.status_code == 401:
                print(f"  [Companies House API] Invalid API key")
                return False
            return response.status_code == 200
        except Exception as e:
            log_verbose(f"API connection test failed: {e}")
            return False
    
    def _get_search_terms(self) -> list:
        if self.sector:
            return [f"{self.sector} {self.town}"]
        
        return [
            f"consulting {self.town}",
            f"technology {self.town}",
            f"software {self.town}",
            f"marketing {self.town}",
            f"legal services {self.town}",
            f"accountants {self.town}",
            f"recruitment {self.town}",
        ]
    
    def _parse_company(self, company: dict) -> Optional[BusinessLead]:
        try:
            title = company.get("title", "")
            if not title:
                return None
            
            status = company.get("company_status", "")
            if status in ["dissolved", "liquidation", "converted-closed"]:
                return None
            
            address = company.get("address", {})
            location_parts = []
            if address.get("locality"):
                location_parts.append(address["locality"])
            if address.get("postal_code"):
                location_parts.append(address["postal_code"])
            location = ", ".join(location_parts)
            
            company_number = company.get("company_number", "")
            
            description = company.get("description", "") or ""
            snippet = company.get("snippet", "") or ""
            sector = description or snippet or status
            
            return BusinessLead(
                company_name=clean_text(title),
                website=f"https://find-and-update.company-information.service.gov.uk/company/{company_number}" if company_number else "",
                sector=sector[:200],
                location=location,
                source=self.source_name
            )
        except Exception as e:
            log_verbose(f"Error parsing company: {e}")
            return None
    
    def _is_in_target_area(self, location: str) -> bool:
        if not location:
            return True
        
        target_areas = [
            'surrey', 'guildford', 'godalming', 'farnham', 'woking',
            'gu1', 'gu2', 'gu3', 'gu4', 'gu5', 'gu7', 'gu8', 'gu9', 'gu10',
            'gu21', 'gu22', 'gu23', 'gu24'
        ]
        location_lower = location.lower()
        return any(area in location_lower for area in target_areas)
