import re
from typing import Generator, Optional
from bs4 import BeautifulSoup
from urllib.parse import quote_plus

from .base_scraper import BaseScraper
from ..models import BusinessLead
from ..utils import make_request, rate_limit, clean_text, extract_domain

class GoogleSearchScraper(BaseScraper):
    def __init__(self, town: str, sector: str = ""):
        super().__init__(town, sector)
        self.source_name = "Google Search"
        self.base_url = "https://www.google.co.uk/search"
    
    def scrape(self, max_pages: int = 3) -> Generator[BusinessLead, None, None]:
        search_queries = self._build_queries()
        
        for query in search_queries:
            print(f"[Google] Searching: {query}")
            for page in range(max_pages):
                start = page * 10
                url = f"{self.base_url}?q={quote_plus(query)}&start={start}"
                
                response = make_request(url)
                if not response:
                    continue
                
                soup = BeautifulSoup(response.text, 'lxml')
                results = soup.find_all('div', class_='g')
                
                if not results:
                    results = soup.find_all('div', {'data-sokoban-container': True})
                
                for result in results:
                    lead = self._parse_result(result)
                    if lead:
                        yield lead
                
                rate_limit(2.0, 4.0)
    
    def _build_queries(self) -> list:
        base_sectors = [
            "accountants", "solicitors", "law firms", "recruitment agencies",
            "IT companies", "software companies", "consulting firms",
            "marketing agencies", "digital agencies", "engineering companies",
            "environmental consultants", "management consultants"
        ]
        
        if self.sector:
            base_sectors = [self.sector]
        
        queries = []
        for sector in base_sectors:
            queries.append(f"{sector} in {self.town} UK")
            queries.append(f"{sector} {self.town} site:uk")
        
        return queries
    
    def _parse_result(self, result) -> Optional[BusinessLead]:
        try:
            link_elem = result.find('a', href=True)
            if not link_elem:
                return None
            
            url = link_elem.get('href', '')
            if not url or 'google' in url or url.startswith('/'):
                return None
            
            title_elem = result.find('h3')
            title = clean_text(title_elem.get_text()) if title_elem else ""
            
            if not title:
                return None
            
            snippet_elem = result.find('div', class_='VwiC3b') or result.find('span', class_='aCOpRe')
            snippet = clean_text(snippet_elem.get_text()) if snippet_elem else ""
            
            company_name = self._extract_company_name(title)
            
            return BusinessLead(
                company_name=company_name,
                website=url,
                sector=snippet[:200] if snippet else "",
                location=self.town,
                source=self.source_name
            )
        except Exception as e:
            print(f"Error parsing Google result: {e}")
            return None
    
    def _extract_company_name(self, title: str) -> str:
        separators = [' - ', ' | ', ' – ', ' :: ']
        for sep in separators:
            if sep in title:
                return title.split(sep)[0].strip()
        return title
