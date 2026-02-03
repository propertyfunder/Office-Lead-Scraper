import re
from typing import Generator, Optional
from bs4 import BeautifulSoup
from urllib.parse import quote_plus

from .base_scraper import BaseScraper
from ..models import BusinessLead
from ..utils import make_request, rate_limit, clean_text, log_verbose

class CompaniesHouseScraper(BaseScraper):
    def __init__(self, town: str, sector: str = ""):
        super().__init__(town, sector)
        self.source_name = "Companies House"
        self.search_url = "https://find-and-update.company-information.service.gov.uk/search/companies"
        self.total_found = 0
    
    def scrape(self, max_pages: int = 3) -> Generator[BusinessLead, None, None]:
        search_terms = self._get_search_terms()
        
        for term in search_terms:
            print(f"  [Companies House] Search: {term}")
            term_found = 0
            
            for page in range(1, max_pages + 1):
                url = f"{self.search_url}?q={quote_plus(term)}&page={page}"
                
                response = make_request(url, referer=self.search_url)
                if not response:
                    continue
                
                soup = BeautifulSoup(response.text, 'lxml')
                
                is_valid, message = self._validate_page(soup, response.text)
                if not is_valid:
                    log_verbose(f"Page validation failed: {message}")
                    continue
                
                results = soup.find_all('li', class_='type-company')
                if not results:
                    results = soup.select('ul.results-list li')
                
                log_verbose(f"Found {len(results)} company results on page {page}")
                
                if not results:
                    break
                
                for result in results:
                    lead = self._parse_result(result)
                    if lead and self._is_in_target_area(lead.location):
                        term_found += 1
                        self.total_found += 1
                        yield lead
                
                rate_limit(1.0, 2.0)
            
            if term_found > 0:
                log_verbose(f"Found {term_found} leads for term")
        
        print(f"  [Companies House] Total found: {self.total_found} leads")
    
    def _validate_page(self, soup: BeautifulSoup, html: str) -> tuple:
        if 'Service unavailable' in html:
            return False, "Service temporarily unavailable"
        
        if len(html) < 1000:
            return False, f"Very short response ({len(html)} bytes)"
        
        return True, "OK"
    
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
            if not address_elem:
                address_elem = result.select_one('[class*="address"]')
            location = clean_text(address_elem.get_text()) if address_elem else ""
            
            status_elem = result.find('span', class_='meta')
            if not status_elem:
                status_elem = result.select_one('[class*="status"], [class*="meta"]')
            status = clean_text(status_elem.get_text()) if status_elem else ""
            
            status_lower = status.lower()
            if any(x in status_lower for x in ['dissolved', 'liquidation', 'struck off', 'closed']):
                return None
            
            full_url = ""
            if company_url:
                if company_url.startswith('/'):
                    full_url = f"https://find-and-update.company-information.service.gov.uk{company_url}"
                else:
                    full_url = company_url
            
            return BusinessLead(
                company_name=company_name,
                website=full_url,
                sector=status,
                location=location,
                source=self.source_name
            )
        except Exception as e:
            log_verbose(f"Error parsing result: {e}")
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
