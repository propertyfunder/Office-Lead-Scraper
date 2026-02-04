import re
import os
from typing import Optional, Tuple, List
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import requests

from .models import BusinessLead
from .utils import make_request, rate_limit, extract_email_from_text, guess_email, extract_domain, clean_text, log_verbose

class LeadEnricher:
    def __init__(self):
        self.director_patterns = [
            r'director[s]?', r'managing director', r'md', r'ceo', 
            r'founder', r'owner', r'partner', r'principal',
            r'proprietor', r'clinical director', r'practice owner'
        ]
        self.companies_house_api_key = os.environ.get("COMPANIES_HOUSE_API_KEY", "")
        self.ch_base_url = "https://api.company-information.service.gov.uk"
    
    def enrich(self, lead: BusinessLead, skip_if_complete: bool = True) -> BusinessLead:
        if skip_if_complete and lead.contact_name and lead.email:
            lead.enrichment_status = "complete"
            return lead
        
        if not lead.website or 'find-and-update.company-information' in lead.website:
            if not lead.contact_name:
                lead = self._try_companies_house(lead)
            lead.enrichment_status = "complete" if (lead.contact_name and lead.email) else "incomplete"
            lead.enrichment_source = lead.enrichment_source or "not_found"
            return lead
        
        sources_tried = []
        
        try:
            print(f"  Enriching: {lead.company_name}")
            
            found_email, found_contact, source = self._enrich_from_website(lead)
            if found_email:
                lead.email = found_email
            if found_contact:
                lead.contact_name = found_contact
            if source:
                sources_tried.append(source)
            
            if (not lead.contact_name or not lead.email) and self.companies_house_api_key:
                ch_contact = self._get_director_from_companies_house(lead.company_name)
                if ch_contact and not lead.contact_name:
                    lead.contact_name = ch_contact
                    sources_tried.append("companies_house")
            
            if not lead.email and lead.contact_name and lead.website:
                domain = extract_domain(lead.website)
                guessed = guess_email(lead.company_name, lead.contact_name, domain)
                if guessed:
                    lead.email = guessed
                    if "website" not in sources_tried:
                        sources_tried.append("website")
            
        except Exception as e:
            print(f"  Error enriching {lead.company_name}: {e}")
        
        if sources_tried:
            lead.enrichment_source = sources_tried[0]
        else:
            lead.enrichment_source = "not_found"
        
        lead.enrichment_status = "complete" if (lead.contact_name and lead.email) else "incomplete"
        
        return lead
    
    def _enrich_from_website(self, lead: BusinessLead) -> Tuple[str, str, str]:
        email = ""
        contact = ""
        source = ""
        
        pages_to_check = self._get_pages_to_check(lead.website)
        
        for page_url in pages_to_check:
            try:
                response = make_request(page_url)
                if not response:
                    continue
                
                soup = BeautifulSoup(response.text, 'lxml')
                page_text = soup.get_text()
                
                if not email:
                    found = self._find_email(soup, page_text)
                    if found:
                        email = found
                        source = "website"
                
                if not contact:
                    found = self._find_contact_name(soup)
                    if found:
                        contact = found
                        source = "website"
                
                if not lead.linkedin:
                    lead.linkedin = self._find_linkedin(soup)
                
                if not lead.employee_count:
                    lead.employee_count = self._estimate_employee_count(soup, page_text)
                
                if not lead.sector or len(lead.sector) < 10:
                    lead.sector = self._extract_sector(soup) or lead.sector
                
                if email and contact:
                    break
                
                rate_limit(0.3, 0.7)
                
            except Exception as e:
                log_verbose(f"Error checking {page_url}: {e}")
                continue
        
        return email, contact, source
    
    def _get_pages_to_check(self, base_url: str) -> List[str]:
        pages = [base_url]
        
        common_paths = [
            '/about', '/about-us', '/about-me',
            '/team', '/our-team', '/meet-the-team',
            '/contact', '/contact-us',
            '/who-we-are', '/the-team',
            '/staff', '/our-people', '/people',
            '/practitioner', '/practitioners', '/therapists',
            '/our-practice', '/the-practice'
        ]
        
        for path in common_paths:
            pages.append(urljoin(base_url, path))
        
        return pages[:8]
    
    def _find_email(self, soup: BeautifulSoup, page_text: str) -> str:
        mailto_links = soup.find_all('a', href=re.compile(r'^mailto:', re.I))
        for link in mailto_links:
            href = str(link.get('href', ''))
            email = href.replace('mailto:', '').split('?')[0].strip()
            if email and '@' in email:
                if not any(x in email.lower() for x in ['example', 'test', 'domain', 'email@']):
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
        
        about_sections = soup.find_all(['section', 'div', 'article'], 
                                       class_=re.compile(r'about|team|management|staff|bio|profile', re.I))
        for section in about_sections[:3]:
            name_candidates = section.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'span'])
            for candidate in name_candidates:
                text = clean_text(candidate.get_text())
                if self._looks_like_name(text):
                    return text
        
        meta_author = soup.find('meta', attrs={'name': 'author'})
        if meta_author:
            author = meta_author.get('content', '')
            if author and isinstance(author, str) and self._looks_like_name(author):
                return clean_text(str(author))
        
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
        exclude_words = ['the', 'and', 'our', 'team', 'about', 'contact', 'director', 
                        'ceo', 'founder', 'welcome', 'meet', 'staff', 'practitioner',
                        'ltd', 'limited', 'inc', 'clinic', 'practice', 'services']
        if any(w.lower() in exclude_words for w in words):
            return False
        return all(w[0].isupper() and w.replace("'", "").replace("-", "").isalpha() for w in words)
    
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
    
    def _try_companies_house(self, lead: BusinessLead) -> BusinessLead:
        if not self.companies_house_api_key:
            return lead
        
        director = self._get_director_from_companies_house(lead.company_name)
        if director:
            lead.contact_name = director
            lead.enrichment_source = "companies_house"
        
        return lead
    
    def _get_director_from_companies_house(self, company_name: str) -> str:
        if not self.companies_house_api_key:
            return ""
        
        try:
            clean_name = re.sub(r'\s*(ltd|limited|llp|plc|inc)\.?\s*$', '', company_name, flags=re.I)
            
            response = requests.get(
                f"{self.ch_base_url}/search/companies",
                params={"q": clean_name, "items_per_page": 5},
                auth=(self.companies_house_api_key, ""),
                timeout=10
            )
            
            if response.status_code != 200:
                return ""
            
            data = response.json()
            items = data.get("items", [])
            
            company_number = None
            for item in items:
                title = item.get("title", "").lower()
                if clean_name.lower() in title or title in clean_name.lower():
                    status = item.get("company_status", "")
                    if status not in ["dissolved", "liquidation"]:
                        company_number = item.get("company_number")
                        break
            
            if not company_number:
                return ""
            
            rate_limit(0.3, 0.5)
            
            officers_response = requests.get(
                f"{self.ch_base_url}/company/{company_number}/officers",
                params={"items_per_page": 10},
                auth=(self.companies_house_api_key, ""),
                timeout=10
            )
            
            if officers_response.status_code != 200:
                return ""
            
            officers_data = officers_response.json()
            officers = officers_data.get("items", [])
            
            for officer in officers:
                if officer.get("resigned_on"):
                    continue
                
                role = officer.get("officer_role", "").lower()
                if role in ["director", "managing-director", "corporate-director"]:
                    name = officer.get("name", "")
                    if name:
                        formatted = self._format_companies_house_name(name)
                        if formatted:
                            return formatted
            
            for officer in officers:
                if officer.get("resigned_on"):
                    continue
                name = officer.get("name", "")
                if name:
                    formatted = self._format_companies_house_name(name)
                    if formatted:
                        return formatted
            
        except Exception as e:
            log_verbose(f"Companies House lookup error: {e}")
        
        return ""
    
    def _format_companies_house_name(self, name: str) -> str:
        if not name:
            return ""
        
        if "," in name:
            parts = name.split(",")
            if len(parts) >= 2:
                surname = parts[0].strip().title()
                forenames = parts[1].strip().split()[0].title() if parts[1].strip() else ""
                if forenames and surname:
                    return f"{forenames} {surname}"
        
        return name.title()


def batch_enrich_leads(leads: List[BusinessLead], skip_complete: bool = True) -> Tuple[List[BusinessLead], dict]:
    enricher = LeadEnricher()
    stats = {
        "total": len(leads),
        "skipped": 0,
        "enriched": 0,
        "complete": 0,
        "incomplete": 0,
        "sources": {"website": 0, "companies_house": 0, "linkedin": 0, "not_found": 0}
    }
    
    needs_enrichment = []
    for lead in leads:
        if skip_complete and lead.contact_name and lead.email:
            stats["skipped"] += 1
            lead.enrichment_status = "complete"
        else:
            needs_enrichment.append(lead)
    
    print(f"\n  Processing {len(needs_enrichment)} leads needing enrichment...")
    print(f"  (Skipping {stats['skipped']} already complete leads)")
    
    for i, lead in enumerate(needs_enrichment):
        enricher.enrich(lead, skip_if_complete=False)
        stats["enriched"] += 1
        
        if lead.enrichment_status == "complete":
            stats["complete"] += 1
        else:
            stats["incomplete"] += 1
        
        source = lead.enrichment_source or "not_found"
        if source in stats["sources"]:
            stats["sources"][source] += 1
        
        if (i + 1) % 10 == 0:
            print(f"  Progress: {i + 1}/{len(needs_enrichment)} leads processed")
    
    return leads, stats
