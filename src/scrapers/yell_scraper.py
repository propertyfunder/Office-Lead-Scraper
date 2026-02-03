import re
from typing import Generator, Optional
from bs4 import BeautifulSoup
from urllib.parse import quote_plus

from .base_scraper import BaseScraper
from ..models import BusinessLead
from ..utils import make_request, rate_limit, clean_text, extract_email_from_text

class YellScraper(BaseScraper):
    def __init__(self, town: str, sector: str = ""):
        super().__init__(town, sector)
        self.source_name = "Yell.com"
        self.base_url = "https://www.yell.com/ucs/UcsSearchAction.do"
    
    def scrape(self, max_pages: int = 3) -> Generator[BusinessLead, None, None]:
        categories = self._get_categories()
        
        for category in categories:
            print(f"[Yell] Searching category: {category} in {self.town}")
            for page in range(1, max_pages + 1):
                url = f"https://www.yell.com/s/{quote_plus(category)}-{quote_plus(self.town.lower())}.html"
                if page > 1:
                    url = f"https://www.yell.com/s/{quote_plus(category)}-{quote_plus(self.town.lower())}-page{page}.html"
                
                response = make_request(url)
                if not response:
                    continue
                
                soup = BeautifulSoup(response.text, 'lxml')
                listings = soup.find_all('div', class_='businessCapsule')
                
                if not listings:
                    listings = soup.find_all('article', class_='businessCapsule')
                
                if not listings:
                    break
                
                for listing in listings:
                    lead = self._parse_listing(listing)
                    if lead:
                        yield lead
                
                rate_limit(1.5, 3.0)
    
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
                return None
            
            company_name = clean_text(name_elem.get_text())
            if not company_name:
                return None
            
            website = ""
            website_elem = listing.find('a', {'data-tracking': 'WEBSITE'}) or listing.find('a', class_='businessCapsule--callToAction')
            if website_elem:
                website = website_elem.get('href', '')
            
            address_elem = listing.find('span', class_='address') or listing.find('address')
            location = clean_text(address_elem.get_text()) if address_elem else self.town
            
            desc_elem = listing.find('p', class_='businessCapsule--description')
            sector = clean_text(desc_elem.get_text()) if desc_elem else ""
            
            phone_elem = listing.find('span', class_='telephone') or listing.find('a', {'data-tracking': 'PHONE'})
            
            return BusinessLead(
                company_name=company_name,
                website=website,
                sector=sector[:200],
                location=location or self.town,
                source=self.source_name
            )
        except Exception as e:
            print(f"Error parsing Yell listing: {e}")
            return None
