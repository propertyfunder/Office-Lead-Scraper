import re
from typing import Generator, Optional
from bs4 import BeautifulSoup
from urllib.parse import quote_plus

from .base_scraper import BaseScraper
from ..models import BusinessLead
from ..utils import make_request, rate_limit, clean_text, log_verbose

class GoogleSearchScraper(BaseScraper):
    def __init__(self, town: str, sector: str = ""):
        super().__init__(town, sector)
        self.source_name = "Google Search"
        self.base_url = "https://www.google.co.uk/search"
        self.total_found = 0
        self.max_results_per_query = 5
    
    def scrape(self, max_pages: int = 3) -> Generator[BusinessLead, None, None]:
        search_queries = self._build_queries()
        
        for query in search_queries:
            print(f"  [Google] Query: {query}")
            query_found = 0
            
            for page in range(max_pages):
                start = page * 10
                url = f"{self.base_url}?q={quote_plus(query)}&start={start}&num=10"
                
                response = make_request(url, referer="https://www.google.co.uk/")
                if not response:
                    continue
                
                soup = BeautifulSoup(response.text, 'lxml')
                
                is_valid, message = self._validate_page(soup, response.text)
                if not is_valid:
                    log_verbose(f"Page validation failed: {message}")
                    print(f"    Google may be blocking: {message}")
                    break
                
                results = soup.find_all('div', class_='g')
                if not results:
                    results = soup.find_all('div', {'data-sokoban-container': True})
                if not results:
                    results = soup.select('div.g, div[data-hveid]')
                
                log_verbose(f"Found {len(results)} raw results")
                
                for result in results:
                    if query_found >= self.max_results_per_query:
                        break
                    
                    lead = self._parse_result(result)
                    if lead:
                        query_found += 1
                        self.total_found += 1
                        yield lead
                
                rate_limit(3.0, 5.0)
            
            if query_found > 0:
                log_verbose(f"Found {query_found} leads for query")
        
        if self.total_found == 0:
            print(f"  [Google] Warning: No results extracted. Google may be blocking or using CAPTCHA.")
            print(f"           Consider using alternative sources or running from a different network.")
    
    def _validate_page(self, soup: BeautifulSoup, html: str) -> tuple:
        html_lower = html.lower()
        
        if 'captcha' in html_lower or 'unusual traffic' in html_lower:
            return False, "CAPTCHA/unusual traffic detected"
        
        if 'sorry' in html_lower and 'automated' in html_lower:
            return False, "Automated query block"
        
        if len(html) < 5000:
            return False, f"Suspiciously short response ({len(html)} bytes)"
        
        if not soup.find('div', class_='g') and 'search' not in html_lower:
            return False, "No search results structure found"
        
        return True, "OK"
    
    def _build_queries(self) -> list:
        base_sectors = [
            "accountants", "solicitors", "recruitment agencies",
            "IT companies", "software companies", "consulting firms",
            "marketing agencies", "engineering companies"
        ]
        
        if self.sector:
            base_sectors = [self.sector]
        
        queries = []
        for sector in base_sectors:
            queries.append(f"{sector} in {self.town} UK")
        
        return queries
    
    def _parse_result(self, result) -> Optional[BusinessLead]:
        try:
            link_elem = result.find('a', href=True)
            if not link_elem:
                return None
            
            url = link_elem.get('href', '')
            if not url:
                return None
            
            if any(x in url.lower() for x in ['google', 'youtube', 'facebook', 'twitter', 'linkedin.com/pulse']):
                return None
            if url.startswith('/'):
                return None
            if not url.startswith('http'):
                return None
            
            title_elem = result.find('h3')
            title = clean_text(title_elem.get_text()) if title_elem else ""
            
            if not title:
                return None
            
            snippet_elem = result.find('div', class_='VwiC3b')
            if not snippet_elem:
                snippet_elem = result.find('span', class_='aCOpRe')
            if not snippet_elem:
                snippet_elem = result.select_one('[data-content-feature]')
            snippet = clean_text(snippet_elem.get_text()) if snippet_elem else ""
            
            company_name = self._extract_company_name(title)
            
            log_verbose(f"Parsed: {company_name} -> {url[:50]}...")
            
            return BusinessLead(
                company_name=company_name,
                website=url,
                sector=snippet[:200] if snippet else "",
                location=self.town,
                source=self.source_name
            )
        except Exception as e:
            log_verbose(f"Error parsing result: {e}")
            return None
    
    def _extract_company_name(self, title: str) -> str:
        separators = [' - ', ' | ', ' – ', ' :: ', ' — ']
        for sep in separators:
            if sep in title:
                return title.split(sep)[0].strip()
        return title
