import re
import os
import json
from datetime import datetime, date
from typing import Optional, Tuple, List
from urllib.parse import urljoin, urlparse, quote_plus
from bs4 import BeautifulSoup
import requests

from .models import BusinessLead
from .utils import make_request, rate_limit, extract_email_from_text, guess_email, extract_domain, clean_text, log_verbose, get_headers, clean_email, normalize_name

DAILY_OPENAI_COST_LIMIT = 2.00
COST_PER_1K_TOKENS = 0.00015  # gpt-4o-mini pricing
LINKEDIN_MAX_ATTEMPTS_PER_SESSION = 50
OPENAI_MAX_CALLS_PER_RECORD = 3
OPENAI_TARGET_TOKENS_PER_CALL = 1500
HEADCOUNT_SKIP_THRESHOLD = 50

class OpenAICostTracker:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_tracker()
        return cls._instance
    
    def _init_tracker(self):
        self.cost_file = "/tmp/openai_enrichment_cost.json"
        self.session_tokens = 0
        self.session_calls = 0
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
    
    def get_stats(self) -> dict:
        return {
            'daily_cost': round(self.daily_cost, 4),
            'daily_limit': DAILY_OPENAI_COST_LIMIT,
            'calls_today': self.call_count,
            'remaining_budget': round(self.get_remaining_budget(), 4)
        }
    
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
        self.session_tokens += tokens_used
        self.session_calls += 1
        self._save_costs()
        print(f"    [OpenAI] Used {tokens_used} tokens (${cost:.4f}). Daily total: ${self.daily_cost:.4f}/{DAILY_OPENAI_COST_LIMIT}")
    
    def get_session_stats(self) -> dict:
        return {
            'session_tokens': self.session_tokens,
            'session_calls': self.session_calls
        }
    
    def reset_session(self):
        self.session_tokens = 0
        self.session_calls = 0
        print(f"    [OpenAI] Session stats reset")
    
    def get_remaining_budget(self) -> float:
        return max(0, DAILY_OPENAI_COST_LIMIT - self.daily_cost)


class LeadEnricher:
    def __init__(self):
        self.director_patterns = [
            r'director[s]?', r'managing director', r'md', r'ceo', 
            r'founder', r'owner', r'partner', r'principal',
            r'proprietor', r'clinical director', r'practice owner',
            r'lead\s+therapist', r'head\s+therapist', r'senior\s+therapist',
            r'meet\s+the\s+team', r'about\s+me', r'your\s+therapist',
            r'dr\.?\s+', r'physiotherapist', r'osteopath', r'therapist'
        ]
        self.companies_house_api_key = os.environ.get("COMPANIES_HOUSE_API_KEY", "")
        self.ch_base_url = "https://api.company-information.service.gov.uk"
        self.generic_email_prefixes = ['info', 'contact', 'enquiries', 'hello', 'admin', 'reception', 'office', 'mail', 'enquiry', 'general', 'support', 'help', 'sales']
        self.nav_keywords = ['about', 'team', 'contact', 'people', 'staff', 'who', 'meet', 'practice', 'practitioner', 'therapist', 'our-', 'the-', 'book', 'booking', 'appointment']
        
        self.sector_categories = [
            'Physiotherapy', 'Mental Health', 'Massage Therapy', 'Chiropractic',
            'Osteopathy', 'Aesthetics/Beauty', 'Yoga/Pilates', 'Nutrition', 'General Wellness'
        ]
        self.sector_keywords = {
            'Physiotherapy': ['physiotherapy', 'physiotherapist', 'physio', 'physical therapy', 'rehabilitation', 'rehab', 'sports injury'],
            'Mental Health': ['psychotherapy', 'counselling', 'counseling', 'therapy', 'mental health', 'psychology', 'psychologist', 'hypnotherapy', 'hypnotherapist', 'cbt', 'cognitive', 'anxiety', 'depression', 'mindfulness'],
            'Massage Therapy': ['massage', 'sports massage', 'deep tissue', 'remedial massage', 'thai massage', 'swedish massage'],
            'Chiropractic': ['chiropractic', 'chiropractor', 'spinal', 'spine'],
            'Osteopathy': ['osteopath', 'osteopathy', 'cranial osteopathy'],
            'Aesthetics/Beauty': ['aesthetics', 'aesthetic', 'beauty', 'facial', 'skin', 'botox', 'dermal', 'cosmetic', 'spa', 'laser', 'anti-aging'],
            'Yoga/Pilates': ['yoga', 'pilates', 'reformer', 'mat pilates', 'studio', 'stretch', 'flexibility'],
            'Nutrition': ['nutrition', 'nutritionist', 'dietitian', 'diet', 'weight loss', 'eating', 'food'],
            'General Wellness': ['wellness', 'holistic', 'health', 'wellbeing', 'acupuncture', 'reflexology', 'reiki', 'healing', 'naturopath', 'clinic', 'dental', 'dentist', 'podiatry', 'podiatrist', 'gp', 'doctor', 'medical']
        }
        
        self.openai_api_key = os.environ.get("OPENAI_API_KEY", "")
        self.cost_tracker = OpenAICostTracker()
        self.linkedin_attempts = 0
        self.linkedin_max_attempts = LINKEDIN_MAX_ATTEMPTS_PER_SESSION
        self.linkedin_attempted = set()
        self.openai_calls_per_record = {}
        
        self.invalid_names = {'new title', 'title', 'untitled', 'unknown', 'n/a', 'na', 
                              'none', 'test', 'admin', 'contact', 'info', 'owner', 'director',
                              'manager', 'team', 'staff', 'enquiries', 'hello', 'general'}
    
    def enrich(self, lead: BusinessLead, skip_if_complete: bool = True) -> BusinessLead:
        if skip_if_complete and lead.contact_name and lead.email:
            lead.enrichment_status = "complete"
            return lead
        
        sources_tried = []
        website_text = ""
        ch_attempted = False
        website_attempted = False
        openai_used_this_call = False
        
        try:
            print(f"  Enriching: {lead.company_name}")
            
            ch_attempted = True
            if self.companies_house_api_key and not lead.contact_name:
                print(f"    [Companies House] Searching for director...")
                ch_contact = self._get_director_from_companies_house(lead.company_name)
                if ch_contact and self._is_valid_contact_name(ch_contact):
                    lead.contact_name = normalize_name(ch_contact)
                    lead.contact_verified = "true"
                    sources_tried.append("companies_house")
                    print(f"    [Companies House] Found director: {lead.contact_name}")
                    
                    if not lead.email and lead.website:
                        domain = extract_domain(lead.website)
                        guessed = guess_email(lead.company_name, lead.contact_name, domain)
                        if guessed:
                            lead.email = clean_email(guessed)
                            lead.email_guessed = "true"
                            lead.enrichment_source = "companies_house"
                            print(f"    [Guessed] Email: {lead.email}")
                else:
                    print(f"    [Companies House] No director found")
            elif not self.companies_house_api_key:
                print(f"    [Companies House] Skipped - no API key")
            elif lead.contact_name:
                print(f"    [Companies House] Skipped - already has contact: {lead.contact_name}")
            
            if self._is_complete(lead):
                lead.enrichment_source = "companies_house"
                lead.enrichment_status = "complete"
                return lead
            
            website_attempted = True
            if lead.website and 'find-and-update.company-information' not in lead.website:
                print(f"    [Website] Checking {lead.website[:50]}...")
                found_email, found_contact, source, text = self._enrich_from_website(lead)
                website_text = text
                if found_email and not lead.email:
                    lead.email = clean_email(found_email)
                    lead.email_guessed = "false"
                    if "website" not in sources_tried:
                        sources_tried.append("website")
                    print(f"    [Website] Found email: {lead.email}")
                if found_contact and self._is_valid_contact_name(found_contact) and not lead.contact_name:
                    lead.contact_name = normalize_name(found_contact)
                    lead.contact_verified = "true"
                    if "website" not in sources_tried:
                        sources_tried.append("website")
                    print(f"    [Website] Found contact: {lead.contact_name}")
                    
                    if not lead.email and lead.website:
                        domain = extract_domain(lead.website)
                        guessed = guess_email(lead.company_name, lead.contact_name, domain)
                        if guessed:
                            lead.email = clean_email(guessed)
                            lead.email_guessed = "true"
                            print(f"    [Guessed] Email: {lead.email}")
                if not found_email and not found_contact:
                    print(f"    [Website] No contact/email found")
            elif not lead.website:
                print(f"    [Website] Skipped - no website URL")
            
            if self._is_complete(lead):
                lead.enrichment_source = sources_tried[0] if sources_tried else "website"
                lead.enrichment_status = "complete"
                return lead
            
            ch_yielded_data = "companies_house" in sources_tried
            website_yielded_data = "website" in sources_tried
            
            if ch_attempted and website_attempted and not ch_yielded_data and not website_yielded_data and not lead.contact_name and self.linkedin_attempts < self.linkedin_max_attempts:
                if lead.place_id not in self.linkedin_attempted:
                    self.linkedin_attempted.add(lead.place_id or lead.company_name)
                    linkedin_contact = self._search_linkedin_for_contact(lead)
                    if linkedin_contact and self._is_valid_contact_name(linkedin_contact):
                        lead.contact_name = normalize_name(linkedin_contact)
                        lead.contact_verified = "false"
                        sources_tried.append("linkedin")
                        print(f"    [LinkedIn] Found contact: {lead.contact_name}")
                        
                        if not lead.email and lead.website:
                            domain = extract_domain(lead.website)
                            guessed = guess_email(lead.company_name, lead.contact_name, domain)
                            if guessed:
                                lead.email = clean_email(guessed)
                                lead.email_guessed = "true"
                                print(f"    [Guessed] Email: {lead.email}")
            
            if self._is_complete(lead):
                lead.enrichment_source = sources_tried[0] if sources_tried else "linkedin"
                lead.enrichment_status = "complete"
                return lead
            
            record_key = lead.place_id or lead.company_name
            calls_for_record = self.openai_calls_per_record.get(record_key, 0)
            
            should_use_openai = (
                self.openai_api_key and
                (not lead.contact_name or not lead.email) and
                lead.ai_enriched != "true" and
                calls_for_record < OPENAI_MAX_CALLS_PER_RECORD and
                self.cost_tracker.can_make_call()
            )
            
            if lead.contact_name and lead.email:
                print(f"    [OpenAI] Skipped - already has email and contact_name")
            elif not self.openai_api_key:
                pass
            elif lead.ai_enriched == "true":
                print(f"    [OpenAI] Skipped - already enriched this record")
            elif calls_for_record >= OPENAI_MAX_CALLS_PER_RECORD:
                print(f"    [OpenAI] Skipped - max {OPENAI_MAX_CALLS_PER_RECORD} calls reached for this record")
            elif not self.cost_tracker.can_make_call():
                print(f"    [OpenAI] Skipped - daily budget exhausted (${DAILY_OPENAI_COST_LIMIT})")
            elif should_use_openai:
                if website_text or lead.website:
                    if not website_text and lead.website:
                        _, _, _, website_text = self._enrich_from_website(lead)
                    if website_text:
                        ai_contact, ai_email = self._openai_extract(lead, website_text)
                        self.openai_calls_per_record[record_key] = calls_for_record + 1
                        
                        if ai_contact and self._is_valid_contact_name(ai_contact) and not lead.contact_name:
                            lead.contact_name = normalize_name(ai_contact)
                            lead.ai_enriched = "true"
                            openai_used_this_call = True
                            sources_tried.append("openai")
                            print(f"    [OpenAI] Extracted contact: {lead.contact_name}")
                            
                            if not lead.email and lead.website:
                                domain = extract_domain(lead.website)
                                guessed = guess_email(lead.company_name, lead.contact_name, domain)
                                if guessed:
                                    lead.email = clean_email(guessed)
                                    lead.email_guessed = "true"
                                    print(f"    [Guessed] Email: {lead.email}")
                        if ai_email and not lead.email:
                            lead.email = clean_email(ai_email)
                            lead.ai_enriched = "true"
                            openai_used_this_call = True
                            if "openai" not in sources_tried:
                                sources_tried.append("openai")
                            print(f"    [OpenAI] Extracted email: {lead.email}")
            
        except Exception as e:
            print(f"  Error enriching {lead.company_name}: {e}")
        
        if sources_tried:
            lead.enrichment_source = sources_tried[0]
        elif not lead.enrichment_source:
            lead.enrichment_source = "not_found"
        
        lead.enrichment_status = self._determine_status(lead)
        object.__setattr__(lead, '_openai_used_this_call', openai_used_this_call)
        
        return lead
    
    def _determine_status(self, lead: BusinessLead) -> str:
        has_contact = self._is_valid_contact_name(lead.contact_name)
        has_email = bool(lead.email and '@' in lead.email)
        
        if has_contact and has_email:
            return "complete"
        elif has_contact and not has_email:
            return "missing_email"
        elif has_email and not has_contact:
            return "missing_name"
        else:
            return "incomplete"
    
    def _is_complete(self, lead: BusinessLead) -> bool:
        return self._is_valid_contact_name(lead.contact_name) and bool(lead.email and '@' in lead.email)
    
    def _is_valid_contact_name(self, name: str) -> bool:
        if not name or len(name.strip()) < 3:
            return False
        name_lower = name.strip().lower()
        if name_lower in self.invalid_names:
            return False
        words = name_lower.split()
        if len(words) < 2:
            return False
        if any(word in self.invalid_names for word in words):
            return False
        return True
    
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
        final_url = lead.website
        
        try:
            response = make_request(lead.website)
            if response:
                final_url = response.url
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
                
                if not lead.sector or lead.sector not in self.sector_categories:
                    lead.sector = self._extract_sector(homepage_soup, homepage_text)
        except Exception as e:
            log_verbose(f"Error checking homepage {lead.website}: {e}")
        
        if personal_email and contact:
            return personal_email, contact, source, all_text
        
        parsed_url = urlparse(final_url)
        root_url = f"{parsed_url.scheme}://{parsed_url.netloc}/"
        discovered_pages = self._discover_nav_pages(homepage_soup, root_url) if homepage_soup else []
        
        if discovered_pages:
            print(f"      Visiting {len(discovered_pages[:6])} subpages: {[p.split('/')[-2] if p.endswith('/') else p.split('/')[-1] for p in discovered_pages[:6]]}")
        else:
            has_soup = homepage_soup is not None
            nav_count = len(homepage_soup.find_all(['nav', 'header'])) if homepage_soup else 0
            print(f"      No subpages discovered (soup={has_soup}, navs={nav_count})")
        
        for page_url in discovered_pages[:6]:
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
    
    def _normalize_domain(self, domain: str) -> str:
        if domain.startswith('www.'):
            return domain[4:]
        return domain
    
    def _discover_nav_pages(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        if not soup:
            return []
        
        discovered = []
        base_domain = self._normalize_domain(urlparse(base_url).netloc)
        
        nav_elements = soup.find_all(['nav', 'header'])
        if not nav_elements:
            nav_elements = [soup]
        
        base_path = urlparse(base_url).path.rstrip('/')
        
        all_links = soup.find_all('a', href=True)
        matched = 0
        for link in all_links:
            href = str(link.get('href', '') or '')
            text = link.get_text().lower().strip()
            
            if any(kw in href.lower() or kw in text for kw in self.nav_keywords):
                matched += 1
                full_url = urljoin(base_url, href)
                parsed = urlparse(full_url)
                link_domain = self._normalize_domain(parsed.netloc)
                link_path = parsed.path.rstrip('/')
                
                if link_domain == base_domain or not parsed.netloc:
                    is_root = link_path == '' or link_path == base_path
                    if full_url not in discovered and not is_root:
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
        
        schema_scripts = soup.find_all('script', type='application/ld+json')
        for script in schema_scripts:
            try:
                script_content = script.string or ""
                data = json.loads(script_content)
                
                items_to_check = []
                if isinstance(data, dict):
                    items_to_check.append(data)
                    if '@graph' in data and isinstance(data['@graph'], list):
                        items_to_check.extend(data['@graph'])
                elif isinstance(data, list):
                    items_to_check.extend(data)
                
                for item in items_to_check:
                    if not isinstance(item, dict):
                        continue
                    email = item.get('email', '')
                    if email and '@' in email:
                        if not any(x in email.lower() for x in ['example', 'test', 'noreply', 'no-reply']):
                            return email
                    if 'contactPoint' in item:
                        cp = item['contactPoint']
                        if isinstance(cp, dict):
                            email = cp.get('email', '')
                            if email and '@' in email:
                                return email
                        elif isinstance(cp, list):
                            for point in cp:
                                if isinstance(point, dict):
                                    email = point.get('email', '')
                                    if email and '@' in email:
                                        return email
            except (json.JSONDecodeError, TypeError):
                pass
        
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
        
        page_text = soup.get_text()
        names_found = self._extract_names_from_page_text(page_text)
        if names_found:
            return names_found[0]
        
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text()
            name_match = re.search(r'^([A-Z][a-z]+\s+[A-Z][a-z]+)', title)
            if name_match:
                potential = name_match.group(1)
                if self._looks_like_name(potential):
                    return potential
        
        return ""
    
    def _extract_names_from_page_text(self, text: str) -> List[str]:
        names = []
        excluded_words = {
            'book', 'online', 'privacy', 'policy', 'terms', 'conditions', 'cookie',
            'consent', 'contact', 'about', 'home', 'services', 'read', 'more',
            'learn', 'click', 'here', 'view', 'see', 'our', 'the', 'meet', 'team',
            'dropdown', 'menu', 'custom', 'scroll', 'items', 'align', 'new', 'change',
            'search', 'engine', 'rank', 'math', 'internet', 'explorer', 'comments',
            'feed', 'physiotherapy', 'osteopathy', 'chiropractic', 'massage', 'therapy',
            'paediatric', 'respiratory', 'musculo', 'skeletal', 'administrative', 'cardio'
        }
        
        words = text.split()
        for i in range(len(words) - 1):
            word1 = words[i].strip()
            word2 = words[i + 1].strip()
            
            word1_clean = re.sub(r'[^a-zA-Z]', '', word1)
            word2_clean = re.sub(r'[^a-zA-Z]', '', word2)
            
            if (len(word1_clean) >= 3 and len(word2_clean) >= 3 and
                word1_clean[0].isupper() and word2_clean[0].isupper() and
                word1_clean[1:].islower() and word2_clean[1:].islower() and
                word1_clean.lower() not in excluded_words and 
                word2_clean.lower() not in excluded_words):
                
                potential_name = f"{word1_clean} {word2_clean}"
                if self._looks_like_name(potential_name):
                    names.append(potential_name)
        
        return names[:5]
    
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
        if not text or len(text) < 5 or len(text) > 50:
            return False
        words = text.split()
        if len(words) < 2 or len(words) > 4:
            return False
        
        for word in words:
            clean_word = word.replace("'", "").replace("-", "")
            if len(clean_word) < 2:
                return False
            if not any(c.lower() in 'aeiou' for c in clean_word):
                return False
            if not clean_word[0].isupper():
                return False
            if not clean_word.isalpha():
                return False
        
        exclude_words = ['the', 'and', 'our', 'team', 'about', 'contact', 'director', 
                        'ceo', 'founder', 'welcome', 'meet', 'staff', 'practitioner',
                        'ltd', 'limited', 'inc', 'clinic', 'practice', 'services',
                        'physiotherapy', 'osteopathy', 'chiropractic', 'dental', 'therapy',
                        'health', 'wellness', 'clinic', 'centre', 'center', 'studio',
                        'home', 'page', 'privacy', 'policy', 'terms', 'conditions',
                        'read', 'more', 'view', 'all', 'latest', 'news', 'book', 'online',
                        'new', 'title', 'menu', 'dropdown', 'items', 'custom', 'scroll']
        if any(w.lower() in exclude_words for w in words):
            return False
        
        first_names_uk = {
            'james', 'john', 'david', 'michael', 'robert', 'william', 'richard', 'thomas', 
            'sarah', 'emma', 'hannah', 'helen', 'claire', 'kate', 'anna', 'mary', 'jane',
            'peter', 'paul', 'mark', 'chris', 'stephen', 'andrew', 'ian', 'simon', 'alex',
            'lucy', 'rachel', 'rebecca', 'sophie', 'laura', 'lisa', 'amy', 'victoria',
            'daniel', 'matthew', 'adam', 'ben', 'tom', 'nick', 'sam', 'joe', 'jack',
            'jess', 'jessica', 'gemma', 'natalie', 'nicole', 'charlotte', 'olivia', 'emily',
            'catherine', 'elizabeth', 'georgina', 'camila', 'michele', 'waqaar', 'ivaylo',
            'bevan', 'wilson', 'donna', 'morgan', 'kim', 'joanne', 'sian', 'suneetha',
            'limei', 'tricia', 'vanessa', 'ellen', 'nicola', 'donald', 'diana', 'julie',
            'karen', 'susan', 'sharon', 'deborah', 'tracy', 'amanda', 'jennifer', 'alison',
            'martin', 'gary', 'kevin', 'neil', 'stuart', 'alan', 'graham', 'philip', 'colin',
            'caroline', 'fiona', 'paula', 'wendy', 'andrea', 'jacqueline', 'lesley', 'dawn',
            'anthony', 'brian', 'barry', 'derek', 'tony', 'roger', 'keith', 'kenneth',
            'sandra', 'linda', 'christine', 'margaret', 'janet', 'angela', 'gillian', 'denise',
            'edward', 'carl', 'gordon', 'roy', 'trevor', 'wayne', 'jeffrey', 'russell',
            'kerry', 'tara', 'zoe', 'holly', 'chloe', 'jade', 'megan', 'bethany', 'ellie',
            'luke', 'ryan', 'jamie', 'lee', 'scott', 'craig', 'darren', 'sean', 'dean',
            'abigail', 'molly', 'grace', 'lily', 'ella', 'daisy', 'freya', 'millie', 'poppy',
            'george', 'harry', 'charlie', 'oscar', 'leo', 'theo', 'noah', 'jacob', 'alfie'
        }
        first_word = words[0].lower()
        return first_word in first_names_uk
    
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
    
    def _normalize_sector(self, text: str) -> str:
        if not text:
            return "General Wellness"
        
        text_lower = text.lower()
        scores: dict[str, int] = {}
        
        for category, keywords in self.sector_keywords.items():
            score = 0
            for kw in keywords:
                if kw in text_lower:
                    score += 2 if len(kw) > 6 else 1
            scores[category] = score
        
        if scores and max(scores.values()) > 0:
            best_category = max(scores.keys(), key=lambda k: scores[k])
            return best_category
        
        return "General Wellness"
    
    def _extract_sector(self, soup: BeautifulSoup, page_text: str = "") -> str:
        all_text = page_text
        
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            content = meta_desc.get('content', '')
            if content and isinstance(content, str):
                all_text += " " + content
        
        og_desc = soup.find('meta', attrs={'property': 'og:description'})
        if og_desc:
            content = og_desc.get('content', '')
            if content and isinstance(content, str):
                all_text += " " + content
        
        h1_tags = soup.find_all('h1')
        for h1 in h1_tags[:2]:
            all_text += " " + h1.get_text()
        
        return self._normalize_sector(all_text)
    
    def _find_linkedin(self, soup: BeautifulSoup) -> str:
        linkedin_links = soup.find_all('a', href=re.compile(r'linkedin\.com', re.I))
        for link in linkedin_links:
            href = str(link.get('href', ''))
            if 'linkedin.com/company' in href or 'linkedin.com/in/' in href:
                return href
        return ""
    
    def _search_linkedin_for_contact(self, lead: BusinessLead) -> str:
        if self.linkedin_attempts >= self.linkedin_max_attempts:
            print(f"    [LinkedIn] Skipped - max {self.linkedin_max_attempts} attempts reached this session")
            return ""
        
        if lead.employee_count:
            try:
                count = int(str(lead.employee_count).replace('+', '').replace('-', '').split()[0])
                if count >= HEADCOUNT_SKIP_THRESHOLD:
                    print(f"    [LinkedIn] Skipped - large org ({count}+ employees)")
                    return ""
            except (ValueError, TypeError):
                pass
        
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
            
            truncated_text = website_text[:2000]
            
            prompt = f"""Extract contact info for: {lead.company_name}

Find: 1) Owner/Director/Founder name 2) Email (personal preferred)
Text: {truncated_text}
Return JSON: {{"name": "First Last" or null, "email": "x@y.com" or null}}"""

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
            
            result_text = (response.choices[0].message.content or "").strip()
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


def batch_enrich_leads(leads: List[BusinessLead], skip_complete: bool = True, filepath: str = "", save_interval: int = 1) -> Tuple[List[BusinessLead], dict]:
    from src.utils import save_leads_to_csv
    
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
        "ai_enriched_this_session": 0,
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
    if filepath:
        print(f"  Incremental save: Every {save_interval} lead(s) to {filepath}")
    
    for i, lead in enumerate(needs_enrichment):
        enricher.enrich(lead, skip_if_complete=False)
        stats["enriched"] += 1
        
        status = lead.enrichment_status
        if status in stats:
            stats[status] += 1
        
        if lead.ai_enriched == "true":
            stats["ai_enriched"] += 1
        
        if getattr(lead, '_openai_used_this_call', False):
            stats["ai_enriched_this_session"] += 1
        
        source = lead.enrichment_source or "not_found"
        if source in stats["sources"]:
            stats["sources"][source] += 1
        
        if filepath and (i + 1) % save_interval == 0:
            save_leads_to_csv(leads, filepath, mode='w')
            print(f"    [Saved] Progress saved to {filepath}")
        
        if (i + 1) % 10 == 0:
            session = enricher.cost_tracker.get_session_stats()
            print(f"  Progress: {i + 1}/{len(needs_enrichment)} leads processed")
            print(f"    Complete: {stats['complete']}, Missing email: {stats['missing_email']}, Missing name: {stats['missing_name']}")
            print(f"    AI enriched (total): {stats['ai_enriched']}, AI enriched (session): {stats['ai_enriched_this_session']}")
            print(f"    OpenAI session: {session['session_calls']} calls, {session['session_tokens']} tokens, budget left: ${enricher.cost_tracker.get_remaining_budget():.2f}")
    
    if filepath:
        save_leads_to_csv(leads, filepath, mode='w')
        print(f"    [Final Save] All data saved to {filepath}")
    
    return leads, stats
