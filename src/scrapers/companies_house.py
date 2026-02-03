import re
from typing import Generator, Optional
from bs4 import BeautifulSoup
from urllib.parse import quote_plus

from .base_scraper import BaseScraper
from ..models import BusinessLead
from ..utils import make_request, rate_limit, clean_text, is_target_sector

class CompaniesHouseScraper(BaseScraper):
    def __init__(self, town: str, sector: str = ""):
        super().__init__(town, sector)
        self.source_name = "Companies House"
        self.search_url = "https://find-and-update.company-information.service.gov.uk/search/companies"
    
    def scrape(self, max_pages: int = 3) -> Generator[BusinessLead, None, None]:
        search_terms = self._get_search_terms()
        
        for term in search_terms:
            print(f"[Companies House] Searching: {term}")
            for page in range(1, max_pages + 1):
                url = f"{self.search_url}?q={quote_plus(term)}&page={page}"
                
                response = make_request(url)
                if not response:
                    continue
                
                soup = BeautifulSoup(response.text, 'lxml')
                results = soup.find_all('li', class_='type-company')
                
                if not results:
                    break
                
                for result in results:
                    lead = self._parse_result(result)
                    if lead:
                        if self._is_in_target_area(lead.location):
                            yield lead
                
                rate_limit(1.0, 2.0)
    
    def _get_search_terms(self) -> list:
        if self.sector:
            return [f"{self.sector} {self.town}"]
        
        return [
            f"consulting {self.town}",
            f"technology {self.town}",
            f"software {self.town}",
            f"marketing {self.town}",
            f"legal {self.town}",
            f"accountants {self.town}",
            f"recruitment {self.town}",
            f"engineering {self.town}",
        ]
    
    def _parse_result(self, result) -> Optional[BusinessLead]:
        try:
            link_elem = result.find('a', href=True)
            if not link_elem:
                return None
            
            company_name = clean_text(link_elem.get_text())
            company_url = link_elem.get('href', '')
            
            if not company_name:
                return None
            
            address_elem = result.find('p') or result.find('dd')
            location = clean_text(address_elem.get_text()) if address_elem else ""
            
            status_elem = result.find('span', class_='meta')
            status = clean_text(status_elem.get_text()) if status_elem else ""
            
            if 'dissolved' in status.lower() or 'liquidation' in status.lower():
                return None
            
            return BusinessLead(
                company_name=company_name,
                website=f"https://find-and-update.company-information.service.gov.uk{company_url}" if company_url else "",
                sector=status,
                location=location,
                source=self.source_name
            )
        except Exception as e:
            print(f"Error parsing Companies House result: {e}")
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
