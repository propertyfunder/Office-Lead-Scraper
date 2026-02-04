import re
import os
import json
from datetime import datetime, date
from typing import Optional, Tuple, List
from urllib.parse import urljoin, urlparse, quote_plus
from bs4 import BeautifulSoup
import requests

from .models import BusinessLead
from .utils import make_request, rate_limit, extract_email_from_text, guess_email, extract_domain, clean_text, log_verbose, get_headers

DAILY_OPENAI_COST_LIMIT = 2.00
COST_PER_1K_TOKENS = 0.00015  # gpt-4o-mini pricing
LINKEDIN_MAX_ATTEMPTS_PER_SESSION = 50

class OpenAICostTracker:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_tracker()
        return cls._instance
    
    def _init_tracker(self):
        self.cost_file = "/tmp/openai_enrichment_cost.json"
        self._load_costs()
    
    def _load_costs(self):
        try:
            if os.path.exists(self.cost_file):
                with open(self.cost_file, 'r') as f:
                    data = json.load(f)
                    if data.get('date') == str(date.today()):
                        self.daily_cost = data.get('cost', 0.0)
                        self.call_count = data.get('calls', 0)
                        return
        except:
            pass
        self.daily_cost = 0.0
        self.call_count = 0
        self._save_costs()
    
    def _save_costs(self):
        try:
            with open(self.cost_file, 'w') as f:
                json.dump({
                    'date': str(date.today()),
                    'cost': self.daily_cost,
                    'calls': self.call_count
                }, f)
        except:
            pass
    
    def can_make_call(self) -> bool:
        if str(date.today()) != self._get_stored_date():
            self.daily_cost = 0.0
            self.call_count = 0
            self._save_costs()
        return self.daily_cost < DAILY_OPENAI_COST_LIMIT
    
    def _get_stored_date(self) -> str:
        try:
            if os.path.exists(self.cost_file):
                with open(self.cost_file, 'r') as f:
                    return json.load(f).get('date', '')
        except:
            pass
        return str(date.today())
    
    def record_usage(self, tokens_used: int):
        cost = (tokens_used / 1000) * COST_PER_1K_TOKENS
        self.daily_cost += cost
        self.call_count += 1
        self._save_costs()
        print(f"    [OpenAI] Used {tokens_used} tokens (${cost:.4f}). Daily total: ${self.daily_cost:.4f}/{DAILY_OPENAI_COST_LIMIT}")
    
    def get_remaining_budget(self) -> float:
        return max(0, DAILY_OPENAI_COST_LIMIT - self.daily_cost)


class LeadEnricher:
    def __init__(self):
        self.director_patterns = [
            r'director[s]?', r'managing director', r'md', r'ceo', 
            r'founder', r'owner', r'partner', r'principal',
            r'proprietor', r'clinical director', r'practice owner',
            r'dr\.?\s+', r'physiotherapist', r'osteopath', r'therapist'
        ]
        self.companies_house_api_key = os.environ.get("COMPANIES_HOUSE_API_KEY", "")
        self.ch_base_url = "https://api.company-information.service.gov.uk"
        self.generic_email_prefixes = ['info', 'contact', 'enquiries', 'hello', 'admin', 'reception', 'office', 'mail', 'enquiry', 'general', 'support', 'help', 'sales']
        self.nav_keywords = ['about', 'team', 'contact', 'people', 'staff', 'who', 'meet', 'practice', 'practitioner', 'therapist', 'our-', 'the-']
        
        self.openai_api_key = os.environ.get("OPENAI_API_KEY", "")
        self.cost_tracker = OpenAICostTracker()
        self.linkedin_attempts = 0
        self.linkedin_max_attempts = LINKEDIN_MAX_ATTEMPTS_PER_SESSION
    
    def enrich(self, lead: BusinessLead, skip_if_complete: bool = True) -> BusinessLead:
        if skip_if_complete and lead.contact_name and lead.email:
            lead.enrichment_status = "complete"
            return lead
        
        lead.ai_enriched = ""
        
        if not lead.website or 'find-and-update.company-information' in lead.website:
            if not lead.contact_name:
                lead = self._try_companies_house(lead)
            lead.enrichment_status = self._determine_status(lead)
            lead.enrichment_source = lead.enrichment_source or "not_found"
            return lead
        
        sources_tried = []
        website_text = ""
        
        try:
            print(f"  Enriching: {lead.company_name}")
            
            if self.companies_house_api_key and not lead.contact_name:
                ch_contact = self._get_director_from_companies_house(lead.company_name)
                if ch_contact:
                    lead.contact_name = ch_contact
                    sources_tried.append("companies_house")
                    print(f"    [Companies House] Found director: {ch_contact}")
            
            if lead.contact_name and lead.email:
                if sources_tried:
                    lead.enrichment_source = sources_tried[0]
                lead.enrichment_status = "complete"
                return lead
            
            found_email, found_contact, source, text = self._enrich_from_website(lead)
            website_text = text
            if found_email:
                lead.email = found_email
                print(f"    [Website] Found email: {found_email}")
            if found_contact and not lead.contact_name:
                lead.contact_name = found_contact
                print(f"    [Website] Found contact: {found_contact}")
            if source and source not in sources_tried:
                sources_tried.append(source)
            
            if lead.contact_name and lead.email:
                if sources_tried:
                    lead.enrichment_source = sources_tried[0]
                lead.enrichment_status = "complete"
                return lead
            
            if not lead.contact_name and self.linkedin_attempts < self.linkedin_max_attempts:
                linkedin_contact = self._search_linkedin_for_contact(lead)
                if linkedin_contact:
                    lead.contact_name = linkedin_contact
                    sources_tried.append("linkedin")
                    print(f"    [LinkedIn] Found contact: {linkedin_contact}")
            
            if not lead.email and lead.contact_name and lead.website:
                domain = extract_domain(lead.website)
                guessed = guess_email(lead.company_name, lead.contact_name, domain)
                if guessed:
                    lead.email = guessed
                    print(f"    [Guessed] Email: {guessed}")
                    if "website" not in sources_tried:
                        sources_tried.append("website")
            
            if lead.contact_name and lead.email:
                if sources_tried:
                    lead.enrichment_source = sources_tried[0]
                lead.enrichment_status = "complete"
                return lead
            
            if (not lead.contact_name or not lead.email) and self.openai_api_key and website_text:
                if self.cost_tracker.can_make_call():
                    ai_contact, ai_email = self._openai_extract(lead, website_text)
                    if ai_contact and not lead.contact_name:
                        lead.contact_name = ai_contact
                        lead.ai_enriched = "true"
                        sources_tried.append("openai")
                        print(f"    [OpenAI] Extracted contact: {ai_contact}")
                    if ai_email and not lead.email:
                        lead.email = ai_email
                        lead.ai_enriched = "true"
                        if "openai" not in sources_tried:
                            sources_tried.append("openai")
                        print(f"    [OpenAI] Extracted email: {ai_email}")
                else:
                    print(f"    [OpenAI] Daily budget exhausted (${DAILY_OPENAI_COST_LIMIT})")
            
        except Exception as e:
            print(f"  Error enriching {lead.company_name}: {e}")
        
        if sources_tried:
            lead.enrichment_source = sources_tried[0]
        else:
            lead.enrichment_source = "not_found"
        
        lead.enrichment_status = self._determine_status(lead)
        
        return lead
    
    def _determine_status(self, lead: BusinessLead) -> str:
        has_contact = bool(lead.contact_name and len(lead.contact_name.strip()) > 2)
        has_email = bool(lead.email and '@' in lead.email)
        
        if has_contact and has_email:
            return "complete"
        elif has_contact and not has_email:
            return "missing_email"
        elif has_email and not has_contact:
            return "missing_name"
        else:
            return "incomplete"
    
    def _is_generic_email(self, email: str) -> bool:
        if not email:
            return False
        local_part = email.split('@')[0].lower()
        return any(local_part.startswith(prefix) for prefix in self.generic_email_prefixes)
    
    def _is_personal_email(self, email: str) -> bool:
        if not email:
            return False
        personal_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com', 'me.com', 'live.com', 'btinternet.com', 'sky.com', 'virginmedia.com', 'talktalk.net']
        domain = email.split('@')[-1].lower() if '@' in email else ''
        return domain in personal_domains
    
    def _enrich_from_website(self, lead: BusinessLead) -> Tuple[str, str, str, str]:
        personal_email = ""
        generic_email = ""
        personal_domain_email = ""
        contact = ""
        source = ""
        all_text = ""
        
        homepage_soup = None
        homepage_text = ""
        
        try:
            response = make_request(lead.website)
            if response:
                homepage_soup = BeautifulSoup(response.text, 'lxml')
                homepage_text = homepage_soup.get_text()
                all_text = homepage_text
                
                found = self._find_email(homepage_soup, homepage_text)
                if found:
                    if self._is_personal_email(found):
                        personal_domain_email = found
                    elif self._is_generic_email(found):
                        generic_email = found
                    else:
                        personal_email = found
                    source = "website"
                
                found = self._find_contact_name(homepage_soup)
                if found:
                    contact = found
                    source = "website"
                
                if not lead.linkedin:
                    lead.linkedin = self._find_linkedin(homepage_soup)
                
                if not lead.employee_count:
                    lead.employee_count = self._estimate_employee_count(homepage_soup, homepage_text)
                
                if not lead.sector or len(lead.sector) < 10:
                    lead.sector = self._extract_sector(homepage_soup) or lead.sector
        except Exception as e:
            log_verbose(f"Error checking homepage {lead.website}: {e}")
        
        if personal_email and contact:
            return personal_email, contact, source, all_text
        
        discovered_pages = self._discover_nav_pages(homepage_soup, lead.website) if homepage_soup else []
        
        for page_url in discovered_pages[:4]:
            if personal_email and contact:
                break
            
            try:
                rate_limit(0.2, 0.5)
                response = make_request(page_url)
                if not response:
                    continue
                
                soup = BeautifulSoup(response.text, 'lxml')
                page_text = soup.get_text()
                all_text += "\n" + page_text
                
                if not personal_email:
                    found = self._find_email(soup, page_text)
                    if found:
                        if self._is_personal_email(found):
                            if not personal_domain_email:
                                personal_domain_email = found
                        elif self._is_generic_email(found):
                            if not generic_email:
                                generic_email = found
                        else:
                            personal_email = found
                        source = "website"
                
                if not contact:
                    found = self._find_contact_name(soup)
                    if found:
                        contact = found
                        source = "website"
                
                if not lead.linkedin:
                    lead.linkedin = self._find_linkedin(soup)
                
            except Exception as e:
                log_verbose(f"Error checking {page_url}: {e}")
                continue
        
        final_email = personal_email or personal_domain_email or generic_email
        return final_email, contact, source, all_text[:5000]
    
    def _discover_nav_pages(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        if not soup:
            return []
        
        discovered = []
        base_domain = urlparse(base_url).netloc
        
        nav_elements = soup.find_all(['nav', 'header'])
        if not nav_elements:
            nav_elements = [soup]
        
        for nav in nav_elements:
            links = nav.find_all('a', href=True)
            for link in links:
                href = str(link.get('href', '') or '')
                text = link.get_text().lower().strip()
                
                if any(kw in href.lower() or kw in text for kw in self.nav_keywords):
                    full_url = urljoin(base_url, href)
                    parsed = urlparse(full_url)
                    
                    if parsed.netloc == base_domain or not parsed.netloc:
                        if full_url not in discovered and full_url != base_url:
                            if not any(x in href.lower() for x in ['#', 'javascript:', 'mailto:', 'tel:', '.pdf', '.jpg', '.png']):
                                discovered.append(full_url)
        
        return discovered[:6]
    
    def _find_email(self, soup: BeautifulSoup, page_text: str) -> str:
        mailto_links = soup.find_all('a', href=re.compile(r'^mailto:', re.I))
        for link in mailto_links:
            href = str(link.get('href', ''))
            email = href.replace('mailto:', '').split('?')[0].strip()
            if email and '@' in email:
                if not any(x in email.lower() for x in ['example', 'test', 'domain', 'email@', 'noreply', 'no-reply', 'unsubscribe']):
                    return email
        
        return extract_email_from_text(page_text)
    
    def _find_contact_name(self, soup: BeautifulSoup) -> str:
        meta_author = soup.find('meta', attrs={'name': 'author'})
        if meta_author:
            author = meta_author.get('content', '')
            if author and isinstance(author, str) and self._looks_like_name(author):
                return clean_text(str(author))
        
        schema_scripts = soup.find_all('script', type='application/ld+json')
        for script in schema_scripts:
            try:
                script_content = script.string or ""
                data = json.loads(script_content)
                if isinstance(data, dict):
                    for field in ['founder', 'author', 'employee', 'member', 'name']:
                        if field in data:
                            person = data[field]
                            if isinstance(person, dict) and 'name' in person:
                                name = person['name']
                                if self._looks_like_name(name):
                                    return clean_text(name)
                            elif isinstance(person, str) and self._looks_like_name(person):
                                return clean_text(person)
            except:
                pass
        
        for pattern in self.director_patterns:
            elements = soup.find_all(string=re.compile(pattern, re.I))
            for elem in elements:
                parent = elem.parent
                if parent:
                    text = clean_text(parent.get_text())
                    name = self._extract_name_from_text(text)
                    if name:
                        return name
                    
                    next_elem = parent.find_next(['h2', 'h3', 'h4', 'strong', 'b', 'p'])
                    if next_elem:
                        next_text = clean_text(next_elem.get_text())
                        if self._looks_like_name(next_text):
                            return next_text
        
        about_sections = soup.find_all(['section', 'div', 'article'], 
                                       class_=re.compile(r'about|team|management|staff|bio|profile|founder|owner', re.I))
        for section in about_sections[:3]:
            name_candidates = section.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'span'])
            for candidate in name_candidates:
                text = clean_text(candidate.get_text())
                if self._looks_like_name(text):
                    return text
        
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text()
            name_match = re.search(r'^([A-Z][a-z]+\s+[A-Z][a-z]+)', title)
            if name_match:
                potential = name_match.group(1)
                if self._looks_like_name(potential):
                    return potential
        
        return ""
    
    def _extract_name_from_text(self, text: str) -> str:
        patterns = [
            r'(?:founder|owner|director|ceo|principal|proprietor)[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)',
            r'([A-Z][a-z]+\s+[A-Z][a-z]+)[,\s]+(?:founder|owner|director|ceo|principal)',
            r'(?:by|with|from)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)',
            r'Dr\.?\s+([A-Z][a-z]+\s+[A-Z][a-z]+)',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                name = match.group(1).strip()
                if self._looks_like_name(name):
                    return name
        
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
                        'ltd', 'limited', 'inc', 'clinic', 'practice', 'services',
                        'physiotherapy', 'osteopathy', 'chiropractic', 'dental', 'therapy',
                        'health', 'wellness', 'clinic', 'centre', 'center', 'studio',
                        'home', 'page', 'privacy', 'policy', 'terms', 'conditions',
                        'read', 'more', 'view', 'all', 'latest', 'news']
        if any(w.lower() in exclude_words for w in words):
            return False
        first_names_uk = ['james', 'john', 'david', 'michael', 'robert', 'william', 'richard', 'thomas', 
                          'sarah', 'emma', 'hannah', 'helen', 'claire', 'kate', 'anna', 'mary', 'jane',
                          'peter', 'paul', 'mark', 'chris', 'stephen', 'andrew', 'ian', 'simon', 'alex',
                          'lucy', 'rachel', 'rebecca', 'sophie', 'laura', 'lisa', 'amy', 'victoria',
                          'daniel', 'matthew', 'adam', 'ben', 'tom', 'nick', 'sam', 'joe', 'jack']
        first_word = words[0].lower()
        if first_word in first_names_uk:
            return True
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
    
    def _search_linkedin_for_contact(self, lead: BusinessLead) -> str:
        if self.linkedin_attempts >= self.linkedin_max_attempts:
            return ""
        
        self.linkedin_attempts += 1
        
        try:
            town = lead.search_town or lead.location.split(',')[0] if lead.location else ""
            search_query = f'site:linkedin.com/in "{lead.company_name}" {town} owner founder director'
            
            search_url = f"https://www.bing.com/search?q={quote_plus(search_query)}"
            
            response = requests.get(
                search_url,
                headers=get_headers(),
                timeout=10
            )
            
            if response.status_code != 200:
                return ""
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            results = soup.find_all(['li', 'div'], class_=re.compile(r'b_algo|result', re.I))
            if not results:
                results = soup.find_all('li')
            
            for result in results[:5]:
                text = result.get_text()
                
                if 'linkedin.com/in/' in text.lower():
                    title_patterns = [
                        r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s*[-–|]\s*(?:founder|owner|director|ceo|managing|principal)',
                        r'(?:founder|owner|director|ceo|managing|principal)[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                        r'^([A-Z][a-z]+\s+[A-Z][a-z]+)\s+[-|]',
                    ]
                    for pattern in title_patterns:
                        match = re.search(pattern, text, re.I)
                        if match:
                            name = match.group(1).strip()
                            if self._looks_like_name(name):
                                log_verbose(f"Found LinkedIn contact: {name}")
                                return name
            
            rate_limit(0.5, 1.0)
            
        except Exception as e:
            log_verbose(f"LinkedIn search error: {e}")
        
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
            
            active_officers = [o for o in officers if not o.get("resigned_on")]
            active_officers.sort(key=lambda x: x.get("appointed_on", ""), reverse=True)
            
            for officer in active_officers:
                role = officer.get("officer_role", "").lower()
                if role in ["director", "managing-director", "corporate-director"]:
                    name = officer.get("name", "")
                    if name:
                        formatted = self._format_companies_house_name(name)
                        if formatted:
                            return formatted
            
            psc_response = requests.get(
                f"{self.ch_base_url}/company/{company_number}/persons-with-significant-control",
                params={"items_per_page": 5},
                auth=(self.companies_house_api_key, ""),
                timeout=10
            )
            
            if psc_response.status_code == 200:
                psc_data = psc_response.json()
                pscs = psc_data.get("items", [])
                for psc in pscs:
                    if not psc.get("ceased_on"):
                        name = psc.get("name", "")
                        if name:
                            formatted = self._format_companies_house_name(name)
                            if formatted:
                                return formatted
            
            for officer in active_officers:
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
    
    def _openai_extract(self, lead: BusinessLead, website_text: str) -> Tuple[str, str]:
        if not self.openai_api_key:
            return "", ""
        
        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_api_key)
            
            truncated_text = website_text[:3000]
            
            prompt = f"""Extract contact information from this website text for: {lead.company_name}

Look for:
1. Owner, Director, Founder, Principal, CEO, or main person's name
2. Email address (prefer personal over info@/contact@)

Website text:
{truncated_text}

Return JSON only: {{"name": "First Last" or null, "email": "email@domain.com" or null}}
Only include if clearly found in the text. Return null for missing data."""

            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are extracting contact details from website text. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=100,
                temperature=0.1
            )
            
            tokens_used = response.usage.total_tokens if response.usage else 150
            self.cost_tracker.record_usage(tokens_used)
            
            result_text = response.choices[0].message.content.strip()
            if result_text.startswith("```"):
                result_text = result_text.split("```")[1]
                if result_text.startswith("json"):
                    result_text = result_text[4:]
            
            result = json.loads(result_text)
            
            name = result.get("name", "") or ""
            email = result.get("email", "") or ""
            
            if name and not self._looks_like_name(name):
                name = ""
            if email and '@' not in email:
                email = ""
            
            return name, email
            
        except Exception as e:
            log_verbose(f"OpenAI extraction error: {e}")
            return "", ""


def batch_enrich_leads(leads: List[BusinessLead], skip_complete: bool = True) -> Tuple[List[BusinessLead], dict]:
    enricher = LeadEnricher()
    stats = {
        "total": len(leads),
        "skipped": 0,
        "enriched": 0,
        "complete": 0,
        "missing_email": 0,
        "missing_name": 0,
        "incomplete": 0,
        "ai_enriched": 0,
        "sources": {"website": 0, "linkedin": 0, "companies_house": 0, "openai": 0, "not_found": 0}
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
    print(f"  OpenAI daily budget: ${enricher.cost_tracker.get_remaining_budget():.2f} remaining")
    print(f"  LinkedIn attempts limit: {enricher.linkedin_max_attempts} per session")
    
    for i, lead in enumerate(needs_enrichment):
        enricher.enrich(lead, skip_if_complete=False)
        stats["enriched"] += 1
        
        status = lead.enrichment_status
        if status in stats:
            stats[status] += 1
        
        if lead.ai_enriched == "true":
            stats["ai_enriched"] += 1
        
        source = lead.enrichment_source or "not_found"
        if source in stats["sources"]:
            stats["sources"][source] += 1
        
        if (i + 1) % 10 == 0:
            print(f"  Progress: {i + 1}/{len(needs_enrichment)} leads processed")
            print(f"    Complete: {stats['complete']}, Missing email: {stats['missing_email']}, Missing name: {stats['missing_name']}")
            print(f"    AI enriched: {stats['ai_enriched']}, OpenAI budget left: ${enricher.cost_tracker.get_remaining_budget():.2f}")
    
    return leads, stats
