import re
from typing import Generator, Optional
from bs4 import BeautifulSoup
from urllib.parse import quote_plus

from .base_scraper import BaseScraper
from ..models import BusinessLead
from ..utils import make_request, rate_limit, clean_text, log_verbose

class YellScraper(BaseScraper):
    def __init__(self, town: str, sector: str = ""):
        super().__init__(town, sector)
        self.source_name = "Yell.com"
        self.base_url = "https://www.yell.com"
        self.total_found = 0
        self.total_blocked = 0
    
    def scrape(self, max_pages: int = 3) -> Generator[BusinessLead, None, None]:
        categories = self._get_categories()
        
        for category in categories:
            print(f"  [Yell] Category: {category} in {self.town}")
            category_found = 0
            
            for page in range(1, max_pages + 1):
                url = f"{self.base_url}/s/{quote_plus(category)}-{quote_plus(self.town.lower())}.html"
                if page > 1:
                    url = f"{self.base_url}/s/{quote_plus(category)}-{quote_plus(self.town.lower())}-page{page}.html"
                
                referer = self.base_url if page == 1 else url.replace(f"-page{page}", f"-page{page-1}" if page > 2 else "")
                response = make_request(url, referer=referer)
                
                if not response:
                    self.total_blocked += 1
                    continue
                
                soup = BeautifulSoup(response.text, 'lxml')
                
                is_valid, message = self._validate_page(soup, response.text)
                if not is_valid:
                    log_verbose(f"Page validation failed: {message}")
                    self.total_blocked += 1
                    continue
                
                listings = soup.find_all('div', class_='businessCapsule')
                if not listings:
                    listings = soup.find_all('article', class_='businessCapsule')
                if not listings:
                    listings = soup.select('[class*="businessCapsule"]')
                
                log_verbose(f"Found {len(listings)} listings on page {page}")
                
                if not listings:
                    if category_found == 0:
                        print(f"    No listings found - page structure may have changed")
                    break
                
                for listing in listings:
                    lead = self._parse_listing(listing)
                    if lead:
                        category_found += 1
                        self.total_found += 1
                        yield lead
                
                rate_limit(2.0, 4.0)
            
            if category_found > 0:
                print(f"    Found {category_found} leads in {category}")
        
        if self.total_blocked > 0 and self.total_found == 0:
            print(f"  [Yell] Warning: All {self.total_blocked} requests were blocked (403/CAPTCHA)")
            print(f"         Yell.com has anti-bot protection. Consider using a VPN or proxy.")
    
    def _validate_page(self, soup: BeautifulSoup, html: str) -> tuple:
        html_lower = html.lower()
        
        if 'captcha' in html_lower or 'robot' in html_lower:
            return False, "CAPTCHA detected"
        
        if 'access denied' in html_lower or 'forbidden' in html_lower:
            return False, "Access denied page"
        
        if len(html) < 2000:
            return False, f"Suspiciously short response ({len(html)} bytes)"
        
        if not soup.find('body'):
            return False, "No body element found"
        
        return True, "OK"
    
    def _get_categories(self) -> list:
        if self.sector:
            return [self.sector]
        
        return [
            "accountants", "solicitors", "recruitment-agencies",
            "business-consultants", "it-services", "software-companies",
            "marketing-consultants", "graphic-designers", "web-designers",
            "engineers", "environmental-consultants", "management-consultants"
        ]
    
    def _parse_listing(self, listing) -> Optional[BusinessLead]:
        try:
            name_elem = listing.find('h2') or listing.find('a', class_='businessCapsule--name')
            if not name_elem:
                name_elem = listing.select_one('[class*="name"]')
            if not name_elem:
                return None
            
            company_name = clean_text(name_elem.get_text())
            if not company_name:
                return None
            
            website = ""
            website_elem = listing.find('a', {'data-tracking': 'WEBSITE'})
            if not website_elem:
                website_elem = listing.find('a', class_='businessCapsule--callToAction')
            if not website_elem:
                website_elem = listing.select_one('a[href*="http"]:not([href*="yell.com"])')
            if website_elem:
                website = website_elem.get('href', '')
            
            address_elem = listing.find('span', class_='address') or listing.find('address')
            if not address_elem:
                address_elem = listing.select_one('[class*="address"]')
            location = clean_text(address_elem.get_text()) if address_elem else self.town
            
            desc_elem = listing.find('p', class_='businessCapsule--description')
            if not desc_elem:
                desc_elem = listing.select_one('[class*="description"]')
            sector = clean_text(desc_elem.get_text()) if desc_elem else ""
            
            return BusinessLead(
                company_name=company_name,
                website=website,
                sector=sector[:200],
                location=location or self.town,
                source=self.source_name
            )
        except Exception as e:
            log_verbose(f"Error parsing listing: {e}")
            return None
