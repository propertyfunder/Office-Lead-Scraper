import re
from typing import Optional, Any
from bs4 import BeautifulSoup

from .models import BusinessLead
from .utils import make_request, rate_limit, extract_email_from_text, guess_email, extract_domain, clean_text

class LeadEnricher:
    def __init__(self):
        self.director_patterns = [
            r'director[s]?', r'managing director', r'md', r'ceo', 
            r'founder', r'owner', r'partner', r'principal'
        ]
    
    def enrich(self, lead: BusinessLead) -> BusinessLead:
        if not lead.website or 'find-and-update.company-information' in lead.website:
            return lead
        
        try:
            print(f"  Enriching: {lead.company_name}")
            response = make_request(lead.website)
            if not response:
                return lead
            
            soup = BeautifulSoup(response.text, 'lxml')
            page_text = soup.get_text()
            
            if not lead.email:
                lead.email = self._find_email(soup, page_text)
            
            if not lead.contact_name:
                lead.contact_name = self._find_contact_name(soup)
            
            if not lead.email and lead.contact_name and lead.website:
                domain = extract_domain(lead.website)
                lead.email = guess_email(lead.company_name, lead.contact_name, domain)
            
            if not lead.employee_count:
                lead.employee_count = self._estimate_employee_count(soup, page_text)
            
            if not lead.sector or len(lead.sector) < 10:
                lead.sector = self._extract_sector(soup) or lead.sector
            
            lead.linkedin = self._find_linkedin(soup)
            
            rate_limit(0.5, 1.5)
            
        except Exception as e:
            print(f"  Error enriching {lead.company_name}: {e}")
        
        return lead
    
    def _find_email(self, soup: BeautifulSoup, page_text: str) -> str:
        mailto_links = soup.find_all('a', href=re.compile(r'^mailto:', re.I))
        for link in mailto_links:
            href = str(link.get('href', ''))
            email = href.replace('mailto:', '').split('?')[0].strip()
            if email and '@' in email:
                if not any(x in email.lower() for x in ['example', 'test', 'domain']):
                    return email
        
        return extract_email_from_text(page_text)
    
    def _find_contact_name(self, soup: BeautifulSoup) -> str:
        for pattern in self.director_patterns:
            elements = soup.find_all(string=re.compile(pattern, re.I))
            for elem in elements:
                parent = elem.parent
                if parent:
                    text = clean_text(parent.get_text())
                    name = self._extract_name_from_text(text)
                    if name:
                        return name
        
        about_sections = soup.find_all(['section', 'div'], class_=re.compile(r'about|team|management', re.I))
        for section in about_sections[:2]:
            name_candidates = section.find_all(['h2', 'h3', 'h4', 'strong', 'b'])
            for candidate in name_candidates:
                text = clean_text(candidate.get_text())
                if self._looks_like_name(text):
                    return text
        
        return ""
    
    def _extract_name_from_text(self, text: str) -> str:
        words = text.split()
        for i, word in enumerate(words):
            if self._looks_like_name(word) and i + 1 < len(words):
                potential_name = f"{words[i]} {words[i+1]}"
                if self._looks_like_name(potential_name):
                    return potential_name[:50]
        return ""
    
    def _looks_like_name(self, text: str) -> bool:
        if not text or len(text) < 3 or len(text) > 50:
            return False
        words = text.split()
        if len(words) < 2 or len(words) > 4:
            return False
        exclude_words = ['the', 'and', 'our', 'team', 'about', 'contact', 'director', 'ceo', 'founder']
        if any(w.lower() in exclude_words for w in words):
            return False
        return all(w[0].isupper() and w.isalpha() for w in words)
    
    def _estimate_employee_count(self, soup: BeautifulSoup, page_text: str) -> str:
        patterns = [
            r'(\d+)\s*(?:\+\s*)?employees?',
            r'team\s*(?:of\s*)?(\d+)',
            r'(\d+)\s*(?:member|staff|people)',
        ]
        for pattern in patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                count = int(match.group(1))
                if 1 <= count <= 500:
                    return str(count)
        return ""
    
    def _extract_sector(self, soup: BeautifulSoup) -> str:
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            content = meta_desc.get('content', '')
            if content and isinstance(content, str):
                return clean_text(content)[:200]
        
        og_desc = soup.find('meta', attrs={'property': 'og:description'})
        if og_desc:
            content = og_desc.get('content', '')
            if content and isinstance(content, str):
                return clean_text(content)[:200]
        
        return ""
    
    def _find_linkedin(self, soup: BeautifulSoup) -> str:
        linkedin_links = soup.find_all('a', href=re.compile(r'linkedin\.com', re.I))
        for link in linkedin_links:
            href = str(link.get('href', ''))
            if 'linkedin.com/company' in href or 'linkedin.com/in/' in href:
                return href
        return ""
