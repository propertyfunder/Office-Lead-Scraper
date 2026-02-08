import re
import os
import json
from datetime import datetime, date
from typing import Optional, Tuple, List
from urllib.parse import urljoin, urlparse, quote_plus
from bs4 import BeautifulSoup
import requests

from .models import BusinessLead
from .utils import make_request, rate_limit, extract_email_from_text, guess_email, generate_email_guesses, extract_domain, clean_text, log_verbose, get_headers, clean_email, normalize_name, log_failed_url

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


def _is_empty(value) -> bool:
    """Check if value is empty (None, NaN, or empty string)."""
    if value is None:
        return True
    if isinstance(value, float):
        import math
        try:
            if math.isnan(value):
                return True
        except (TypeError, ValueError):
            pass
    if isinstance(value, str) and value.strip() == '':
        return True
    return False

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
        self.nav_keywords = ['about', 'team', 'contact', 'people', 'staff', 'who', 'meet',
                              'meet-the-team', 'meet_the_team', 'meettheteam',
                              'our-team', 'our_team', 'the-team', 'the_team',
                              'practice', 'practitioner', 'therapist', 'our-', 'the-',
                              'book', 'booking', 'appointment', 'clinician', 'doctor',
                              'leadership', 'consultant', 'specialist', 'coach',
                              'instructor', 'director', 'associates', 'partners',
                              'our-people', 'our_people', 'who-we-are', 'who_we_are',
                              'about-us', 'about_us', 'our-story', 'our_story',
                              'meet-us', 'meet_us', 'our-experts', 'our_experts']
        self.team_page_keywords = [
            'team', 'our team', 'meet the team', 'meet our team',
            'clinicians', 'practitioners',
            'about us', 'leadership', 'doctors', 'therapists', 'consultants',
            'staff', 'directors', 'specialists', 'coaches', 'instructors',
            'our people', 'our experts', 'meet us', 'who we are', 'the team',
            'our associates', 'our partners', 'our practitioners',
            'our clinicians', 'our therapists', 'our consultants',
            'our story', 'about me', 'your therapist', 'your practitioner'
        ]
        self.fallback_page_paths = [
            '/team', '/our-team', '/the-team', '/meet-the-team',
            '/about', '/about-us', '/about-me',
            '/staff', '/our-staff',
            '/clinicians', '/our-clinicians',
            '/practitioners', '/our-practitioners',
            '/therapists', '/our-therapists',
            '/our-story', '/leadership',
            '/people', '/our-people',
            '/contact', '/contact-us',
        ]
        
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
                              'manager', 'team', 'staff', 'enquiries', 'hello', 'general',
                              'massage', 'therapy', 'clinic', 'practice', 'studio', 'centre',
                              'center', 'dental', 'yoga', 'pilates', 'health', 'wellness',
                              'the', 'and', 'for', 'your'}
        self.invalid_name_phrases = [
            'counselling', 'hypnotherapy', 'physiotherapy', 'osteopathy', 'chiropractic',
            'therapy', 'clinic', 'practice', 'studio', 'dental', 'yoga', 'pilates',
            'massage', 'acupuncture', 'reflexology', 'wellness', 'fitness',
            'gardens', 'oxfordshire', 'surrey', 'hampshire', 'berkshire', 'sussex',
            'london', 'guildford', 'farnham', 'godalming', 'woking', 'stafford'
        ]
        self.social_media_domains = ['facebook.com', 'fb.com', 'instagram.com', 'twitter.com',
                                      'tiktok.com', 'linkedin.com', 'youtube.com']
    
    def _is_fully_enriched(self, lead: BusinessLead) -> bool:
        has_primary = bool(lead.contact_name and lead.email)
        has_contacts = bool(lead.contact_names)
        has_generic = bool(lead.generic_email)
        has_guesses = bool(lead.personal_email_guesses)
        return has_primary and has_contacts and (has_generic or has_guesses)
    
    def _classify_email_type(self, lead: BusinessLead) -> str:
        has_generic = bool(lead.generic_email and '@' in lead.generic_email)
        has_guesses = bool(lead.personal_email_guesses)
        
        if lead.email and '@' in lead.email and self._is_generic_email(lead.email):
            has_generic = True
        
        has_personal = False
        if lead.email and '@' in lead.email and not self._is_generic_email(lead.email):
            email_local = lead.email.split('@')[0].lower()
            contact_lower = (lead.contact_name or '').lower().split()
            principal_lower = (lead.principal_name or '').lower().split()
            name_parts = contact_lower + principal_lower
            if any(part in email_local for part in name_parts if len(part) >= 3):
                has_personal = True
            elif lead.email_guessed != "true":
                has_personal = True
            else:
                has_personal = False
        
        if has_personal and has_generic:
            return "both"
        elif has_personal:
            return "personal"
        elif has_generic:
            return "generic"
        elif has_guesses or (lead.email and lead.email_guessed == "true"):
            return "guessed"
        else:
            return "none"
    
    def _calculate_confidence_score(self, lead: BusinessLead) -> str:
        score = 0
        
        if lead.contact_name and self._is_valid_contact_name(lead.contact_name):
            score += 1
        if lead.email and '@' in lead.email and not self._is_generic_email(lead.email):
            score += 1
        elif lead.generic_email:
            score += 0.5
        if lead.website and lead.website_verified == "true":
            score += 1
        elif lead.website:
            score += 0.5
        if lead.contact_names and ';' in lead.contact_names:
            score += 0.5
        if lead.principal_name:
            score += 0.5
        if lead.personal_email_guesses:
            score += 0.5
        
        final = min(5, max(1, round(score)))
        return str(final)
    
    def enrich(self, lead: BusinessLead, skip_if_complete: bool = True) -> BusinessLead:
        if skip_if_complete and self._is_fully_enriched(lead):
            lead.enrichment_status = "complete"
            lead.email_type = self._classify_email_type(lead)
            lead.confidence_score = self._calculate_confidence_score(lead)
            return lead
        
        current_attempts = int(lead.enrichment_attempts) if lead.enrichment_attempts and lead.enrichment_attempts.isdigit() else 0
        lead.enrichment_attempts = str(current_attempts + 1)
        
        sources_tried = []
        website_text = ""
        ch_attempted = False
        website_attempted = False
        openai_used_this_call = False
        notes = []
        
        no_website = not lead.website
        no_facebook = not lead.facebook_url
        if no_website and no_facebook:
            print(f"  Skipping: {lead.company_name} - no website and no Facebook presence")
            notes.append("excluded:no_web_no_fb")
            lead.enrichment_status = "excluded"
            lead.email_type = self._classify_email_type(lead)
            lead.confidence_score = self._calculate_confidence_score(lead)
            existing_notes = lead.refinement_notes
            new_notes = "; ".join(notes)
            lead.refinement_notes = f"{existing_notes}; {new_notes}".strip("; ") if existing_notes else new_notes
            return lead
        
        try:
            print(f"  Enriching: {lead.company_name} (attempt {lead.enrichment_attempts})")
            
            website_attempted = True
            is_social = lead.website and any(d in lead.website.lower() for d in self.social_media_domains)
            if is_social:
                print(f"    [Website] Social media URL: {lead.website[:50]} - flagged for manual review")
                lead.tag = (lead.tag + ",facebook-only" if lead.tag else "facebook-only") if 'facebook' in lead.website.lower() or 'fb.com' in lead.website.lower() else lead.tag
                log_failed_url(lead.website, lead.company_name, "Social media URL - cannot scrape")
                notes.append("social_media_url")
            elif lead.website and 'find-and-update.company-information' not in lead.website:
                print(f"    [Website] Checking {lead.website[:50]}...")
                web_result = self._enrich_from_website(lead)
                found_email = web_result['email']
                found_contact = web_result['contact']
                website_text = web_result['text']
                web_contacts = web_result['contacts']
                web_generic_email = web_result['generic_email']
                known_format = web_result['known_email_format']
                
                if web_generic_email and _is_empty(lead.generic_email):
                    lead.generic_email = clean_email(web_generic_email)
                    if lead.generic_email:
                        print(f"    [Website] Generic email: {lead.generic_email}")
                
                if found_email and _is_empty(lead.email):
                    lead.email = clean_email(found_email)
                    lead.email_guessed = "false"
                    if "website" not in sources_tried:
                        sources_tried.append("website")
                    if lead.email:
                        print(f"    [Website] Found email: {lead.email}")
                if found_contact and _is_empty(lead.contact_name):
                    if self._is_valid_contact_name(found_contact):
                        if self._is_suspicious_name(found_contact) and web_contacts:
                            better = next((c['name'] for c in web_contacts if self._is_valid_contact_name(c['name']) and not self._is_suspicious_name(c['name'])), None)
                            if better:
                                lead.contact_name = normalize_name(better)
                                notes.append(f"suspicious_name_replaced:{found_contact}")
                                print(f"    [Website] Suspicious name '{found_contact}' replaced with team member: {lead.contact_name}")
                            else:
                                lead.contact_name = normalize_name(found_contact)
                                notes.append(f"possible_placeholder_name:{found_contact}")
                                print(f"    [Website] Found contact (flagged suspicious): {lead.contact_name}")
                        else:
                            lead.contact_name = normalize_name(found_contact)
                            print(f"    [Website] Found contact: {lead.contact_name}")
                        lead.contact_verified = "true"
                        if "website" not in sources_tried:
                            sources_tried.append("website")
                        
                        if _is_empty(lead.email) and lead.website:
                            domain = extract_domain(lead.website)
                            guessed = guess_email(lead.company_name, lead.contact_name, domain)
                            if guessed:
                                lead.email = clean_email(guessed)
                                lead.email_guessed = "true"
                                print(f"    [Guessed] Email: {lead.email}")
                    else:
                        notes.append(f"invalid_name_rejected:{found_contact[:40]}")
                
                if web_contacts:
                    domain = extract_domain(lead.website)
                    contact_names_list = []
                    contact_titles_list = []
                    all_email_guesses = []
                    
                    for c in web_contacts[:8]:
                        contact_names_list.append(c['name'])
                        contact_titles_list.append(c.get('title', ''))
                        if domain:
                            guesses = generate_email_guesses(c['name'], domain, known_format)
                            all_email_guesses.extend(guesses)
                    
                    if _is_empty(lead.contact_names) or len(contact_names_list) > len(lead.contact_names.split(';')):
                        lead.contact_names = "; ".join(contact_names_list)
                        lead.contact_titles = "; ".join(contact_titles_list)
                    lead.multiple_contacts = "TRUE" if len(contact_names_list) > 1 else "FALSE"
                    
                    seen_guesses = []
                    for g in all_email_guesses:
                        if g not in seen_guesses:
                            seen_guesses.append(g)
                    if _is_empty(lead.personal_email_guesses) or len(seen_guesses) > len(lead.personal_email_guesses.split(';')):
                        lead.personal_email_guesses = "; ".join(seen_guesses)
                    
                    if contact_names_list:
                        print(f"    [Website] Found {len(contact_names_list)} contacts: {', '.join(contact_names_list)}")
                    if seen_guesses:
                        print(f"    [Website] Generated {len(seen_guesses)} email guesses")
                elif lead.contact_name and _is_empty(lead.contact_names):
                    lead.contact_names = lead.contact_name
                    lead.multiple_contacts = "FALSE"
                    domain = extract_domain(lead.website) if lead.website else ""
                    if domain:
                        guesses = generate_email_guesses(lead.contact_name, domain)
                        lead.personal_email_guesses = "; ".join(guesses)
                
                if found_email and not found_contact and not web_contacts:
                    notes.append("website_email_only_no_names")
                elif not found_email and found_contact:
                    notes.append("website_name_only_no_email")
                elif not found_email and not found_contact and not web_contacts:
                    print(f"    [Website] No contact/email found")
                    log_failed_url(lead.website, lead.company_name, "No email or contact found on website")
                    notes.append("website_no_data")
            elif not lead.website:
                print(f"    [Website] Skipped - no website URL")
                notes.append("no_website")
            
            if self._is_complete(lead):
                lead.enrichment_source = sources_tried[0] if sources_tried else "website"
                lead.enrichment_status = "complete"
                lead.email_type = self._classify_email_type(lead)
                lead.confidence_score = self._calculate_confidence_score(lead)
                if notes:
                    lead.refinement_notes = "; ".join(notes)
                return lead
            
            ch_attempted = True
            if self.companies_house_api_key:
                if _is_empty(lead.principal_name):
                    print(f"    [Companies House] Searching for director...")
                    ch_contact = self._get_director_from_companies_house(lead.company_name)
                    if ch_contact and self._is_valid_contact_name(ch_contact):
                        ch_name = normalize_name(ch_contact)
                        lead.principal_name = ch_name
                        if lead.website:
                            domain = extract_domain(lead.website)
                            if domain:
                                guessed = guess_email(lead.company_name, ch_name, domain)
                                if guessed:
                                    lead.principal_email_guess = clean_email(guessed)
                        sources_tried.append("companies_house")
                        print(f"    [Companies House] Found director: {lead.principal_name}")
                        if lead.principal_email_guess:
                            print(f"    [Companies House] Director email guess: {lead.principal_email_guess}")
                        
                        if _is_empty(lead.contact_name):
                            lead.contact_name = ch_name
                            lead.contact_verified = "true"
                            lead.enrichment_source = "companies_house"
                            
                            if _is_empty(lead.email) and lead.principal_email_guess:
                                lead.email = lead.principal_email_guess
                                lead.email_guessed = "true"
                            if lead.website and _is_empty(lead.contact_names):
                                lead.contact_names = ch_name
                                lead.multiple_contacts = "FALSE"
                                domain = extract_domain(lead.website)
                                if domain:
                                    guesses = generate_email_guesses(ch_name, domain)
                                    lead.personal_email_guesses = "; ".join(guesses)
                    else:
                        print(f"    [Companies House] No director found")
                else:
                    print(f"    [Companies House] Skipped - principal_name already set: {lead.principal_name}")
            elif not self.companies_house_api_key:
                print(f"    [Companies House] Skipped - no API key")
            
            if lead.principal_name and _is_empty(lead.principal_email_guess) and lead.website:
                domain = extract_domain(lead.website)
                if domain:
                    guessed = guess_email(lead.company_name, lead.principal_name, domain)
                    if guessed:
                        lead.principal_email_guess = clean_email(guessed)
                        print(f"    [Principal] Guessed email for principal: {lead.principal_email_guess}")
                        notes.append("principal_email_guessed")
            
            if lead.principal_name and _is_empty(lead.contact_name):
                lead.contact_name = lead.principal_name
                lead.contact_verified = "false"
                notes.append("contact_backfilled_from_principal")
                print(f"    [Backfill] contact_name set from principal: {lead.principal_name}")
                if _is_empty(lead.email) and lead.principal_email_guess:
                    lead.email = lead.principal_email_guess
                    lead.email_guessed = "true"
                if lead.website and _is_empty(lead.contact_names):
                    lead.contact_names = lead.principal_name
                    lead.multiple_contacts = "FALSE"
                    domain = extract_domain(lead.website)
                    if domain:
                        guesses = generate_email_guesses(lead.principal_name, domain)
                        if _is_empty(lead.personal_email_guesses) or len(guesses) > len(lead.personal_email_guesses.split(';')):
                            lead.personal_email_guesses = "; ".join(guesses)
            
            if self._is_complete(lead):
                lead.enrichment_source = sources_tried[0] if sources_tried else "companies_house"
                lead.enrichment_status = "complete"
                lead.email_type = self._classify_email_type(lead)
                lead.confidence_score = self._calculate_confidence_score(lead)
                if notes:
                    lead.refinement_notes = "; ".join(notes)
                return lead
            
            ch_yielded_data = "companies_house" in sources_tried
            website_yielded_data = "website" in sources_tried
            
            if ch_attempted and website_attempted and not ch_yielded_data and not website_yielded_data and _is_empty(lead.contact_name) and self.linkedin_attempts < self.linkedin_max_attempts:
                if lead.place_id not in self.linkedin_attempted:
                    self.linkedin_attempted.add(lead.place_id or lead.company_name)
                    linkedin_contact = self._search_linkedin_for_contact(lead)
                    if linkedin_contact and self._is_valid_contact_name(linkedin_contact):
                        lead.contact_name = normalize_name(linkedin_contact)
                        lead.contact_verified = "false"
                        sources_tried.append("linkedin")
                        print(f"    [LinkedIn] Found contact: {lead.contact_name}")
                        
                        if _is_empty(lead.email) and lead.website:
                            domain = extract_domain(lead.website)
                            guessed = guess_email(lead.company_name, lead.contact_name, domain)
                            if guessed:
                                lead.email = clean_email(guessed)
                                lead.email_guessed = "true"
                                print(f"    [Guessed] Email: {lead.email}")
                        if lead.website and _is_empty(lead.contact_names):
                            lead.contact_names = lead.contact_name
                            lead.multiple_contacts = "FALSE"
                            domain = extract_domain(lead.website)
                            if domain:
                                guesses = generate_email_guesses(lead.contact_name, domain)
                                lead.personal_email_guesses = "; ".join(guesses)
            
            if self._is_complete(lead):
                lead.enrichment_source = sources_tried[0] if sources_tried else "linkedin"
                lead.enrichment_status = "complete"
                lead.email_type = self._classify_email_type(lead)
                lead.confidence_score = self._calculate_confidence_score(lead)
                if notes:
                    lead.refinement_notes = "; ".join(notes)
                return lead
            
            record_key = lead.place_id or lead.company_name
            calls_for_record = self.openai_calls_per_record.get(record_key, 0)
            
            openai_reason = ""
            should_use_openai = (
                self.openai_api_key and
                (_is_empty(lead.contact_name) or _is_empty(lead.email)) and
                lead.ai_enriched != "true" and
                calls_for_record < OPENAI_MAX_CALLS_PER_RECORD and
                self.cost_tracker.can_make_call()
            )
            
            if should_use_openai:
                if _is_empty(lead.contact_name) and _is_empty(lead.email):
                    openai_reason = "scraper_failed_no_contact_no_email"
                elif _is_empty(lead.contact_name):
                    openai_reason = "scraper_failed_no_contact"
                elif _is_empty(lead.email):
                    openai_reason = "scraper_failed_no_email"
            
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
                        web_fallback = self._enrich_from_website(lead)
                        website_text = web_fallback.get('text', '')
                    if website_text:
                        print(f"    [OpenAI] Triggered: {openai_reason}")
                        notes.append(f"openai_triggered:{openai_reason}")
                        ai_contact, ai_email = self._openai_extract(lead, website_text)
                        self.openai_calls_per_record[record_key] = calls_for_record + 1
                        
                        if ai_contact and self._is_valid_contact_name(ai_contact) and _is_empty(lead.contact_name):
                            lead.contact_name = normalize_name(ai_contact)
                            lead.ai_enriched = "true"
                            openai_used_this_call = True
                            sources_tried.append("openai")
                            print(f"    [OpenAI] Extracted contact: {lead.contact_name}")
                            
                            if _is_empty(lead.email) and lead.website:
                                domain = extract_domain(lead.website)
                                guessed = guess_email(lead.company_name, lead.contact_name, domain)
                                if guessed:
                                    lead.email = clean_email(guessed)
                                    lead.email_guessed = "true"
                                    print(f"    [Guessed] Email: {lead.email}")
                        if ai_email and _is_empty(lead.email):
                            lead.email = clean_email(ai_email)
                            lead.ai_enriched = "true"
                            openai_used_this_call = True
                            if "openai" not in sources_tried:
                                sources_tried.append("openai")
                            print(f"    [OpenAI] Extracted email: {lead.email}")
            
        except Exception as e:
            print(f"  Error enriching {lead.company_name}: {e}")
            notes.append(f"error:{str(e)[:80]}")
        
        if sources_tried:
            lead.enrichment_source = sources_tried[0]
        elif not lead.enrichment_source:
            lead.enrichment_source = "not_found"
        
        lead.enrichment_status = self._determine_status(lead)
        lead.email_type = self._classify_email_type(lead)
        lead.confidence_score = self._calculate_confidence_score(lead)
        if notes:
            existing_notes = lead.refinement_notes
            new_notes = "; ".join(notes)
            lead.refinement_notes = f"{existing_notes}; {new_notes}".strip("; ") if existing_notes else new_notes
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
        
        title_prefixes = {'dr', 'dr.', 'mr', 'mr.', 'mrs', 'mrs.', 'ms', 'ms.', 'miss',
                          'prof', 'prof.', 'professor', 'rev', 'rev.', 'sir', 'dame'}
        qualification_suffixes = {'bsc', 'msc', 'phd', 'dphil', 'frcs', 'mbbs', 'mrcgp',
                                   'mcsp', 'hcpc', 'mbacp', 'ukcp', 'babcp', 'pgdip',
                                   'diphe', 'ba', 'ma', 'hons', 'fhea', 'pgcert',
                                   'mphil', 'mchiro', 'dosth', 'dip', 'cert', 'accred',
                                   'registered', 'chartered', 'fellow'}
        
        name_words = [w for w in words
                      if w.rstrip('.') not in title_prefixes
                      and w.strip('().,') not in qualification_suffixes]
        
        if len(name_words) < 2:
            return False
        if any(word in self.invalid_names for word in name_words):
            return False
        if any(phrase in ' '.join(name_words) for phrase in self.invalid_name_phrases):
            return False
        if name_words[0] == name_words[1]:
            return False
        if any(c.isdigit() for c in ' '.join(name_words)):
            return False
        business_words = {'clinic', 'clinics', 'surgery', 'surgeries', 'medical', 'dental',
                         'practice', 'practices', 'centre', 'centers', 'center', 'centres',
                         'house', 'links', 'suite', 'treatment', 'treatments', 'therapy',
                         'therapist', 'therapists', 'hygienist', 'university', 'care',
                         'hospital', 'hospitals', 'pharmacy', 'assurance', 'holistic',
                         'directory', 'services', 'service', 'village', 'community',
                         'street', 'road', 'podiatry', 'physiotherapist', 'osteopath',
                         'chiropractic', 'acupuncture', 'hypnotherapy', 'counselling',
                         'nutrition', 'pilates', 'yoga', 'massage', 'dentistry',
                         'aesthetics', 'beauty', 'limited', 'ltd', 'group', 'associates',
                         'solutions', 'consulting', 'consultancy', 'foundation', 'trust',
                         'partnership', 'potential', 'limitless', 'wellness', 'fitness',
                         'studio', 'studios', 'academy', 'institute', 'school', 'college',
                         'nursery'}
        if any(w.rstrip("'s") in business_words for w in name_words):
            return False
        if len(name_words) > 4:
            return False
        if re.search(r"'s\s+\w", ' '.join(name_words)):
            return False
        for word in name_words:
            alpha = re.sub(r'[^a-z]', '', word.replace("'", "").replace("-", ""))
            if len(alpha) >= 3:
                vowels = sum(1 for c in alpha if c in 'aeiouy')
                if vowels == 0:
                    return False
        alpha_only = re.sub(r'[^a-z]', '', ' '.join(name_words).replace("'", "").replace("-", ""))
        if re.search(r'[^aeiouy]{6,}', alpha_only):
            return False
        return True
    
    def _is_suspicious_name(self, name: str) -> bool:
        if not name:
            return True
        name_lower = name.strip().lower()
        words = name_lower.split()
        name_words = [w for w in words if w.rstrip('.') not in {'dr', 'mr', 'mrs', 'ms', 'miss', 'prof', 'rev', 'sir', 'dame'}]
        if not name_words:
            return True
        first = name_words[0].replace("'", "").replace("-", "")
        if first not in self.COMMON_UK_FIRST_NAMES:
            alpha = re.sub(r'[^a-z]', '', first)
            if len(alpha) >= 3:
                vowels = sum(1 for c in alpha if c in 'aeiouy')
                ratio = vowels / len(alpha)
                if ratio < 0.2:
                    return True
            if len(alpha) >= 4:
                unusual_bigrams = ['xq', 'zx', 'qz', 'jx', 'vx', 'xz', 'bx', 'fq', 'qx']
                for bg in unusual_bigrams:
                    if bg in alpha:
                        return True
        for w in name_words:
            clean = re.sub(r'[^a-z]', '', w)
            if len(clean) >= 2 and len(set(clean)) == 1:
                return True
        if len(name_words) >= 2 and name_words[0] == name_words[1]:
            return True
        return False
    
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
    
    def _enrich_from_website(self, lead: BusinessLead) -> dict:
        result = {
            'email': '',
            'generic_email': '',
            'contact': '',
            'contacts': [],
            'source': '',
            'text': '',
            'known_email_format': ''
        }
        personal_email = ""
        generic_email = ""
        personal_domain_email = ""
        contact = ""
        source = ""
        all_text = ""
        all_contacts = []
        all_emails_found = []
        
        homepage_soup = None
        homepage_text = ""
        final_url = lead.website
        site_domain = extract_domain(lead.website) if lead.website else ""
        
        if not lead.website:
            return result
        
        try:
            response = make_request(lead.website, timeout=10)
            if not response:
                log_verbose(f"Website unresponsive: {lead.website}")
                log_failed_url(lead.website, lead.company_name, "Unresponsive - no response")
                return result
            if response.status_code >= 400:
                log_verbose(f"Website returned {response.status_code}: {lead.website}")
                log_failed_url(lead.website, lead.company_name, f"HTTP {response.status_code}")
                return result
            if response:
                final_url = response.url
                homepage_soup = BeautifulSoup(response.text, 'lxml')
                homepage_text = homepage_soup.get_text(separator=' ')
                all_text = homepage_text
                
                found = self._find_email(homepage_soup, homepage_text, site_domain)
                if found:
                    all_emails_found.append(found)
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
                
                multi = self._find_multiple_contacts(homepage_soup)
                if multi:
                    all_contacts.extend(multi)
                
                if not lead.linkedin:
                    lead.linkedin = self._find_linkedin(homepage_soup)
                
                if not lead.employee_count:
                    lead.employee_count = self._estimate_employee_count(homepage_soup, homepage_text)
                
                if not lead.sector or lead.sector not in self.sector_categories:
                    lead.sector = self._extract_sector(homepage_soup, homepage_text)
            else:
                log_failed_url(lead.website, lead.company_name, "Homepage request failed")
        except Exception as e:
            log_verbose(f"Error checking homepage {lead.website}: {e}")
            log_failed_url(lead.website, lead.company_name, f"Exception: {str(e)[:100]}")
        
        parsed_url = urlparse(final_url)
        root_url = f"{parsed_url.scheme}://{parsed_url.netloc}/"
        discovered_pages = self._discover_nav_pages(homepage_soup, root_url) if homepage_soup else []
        
        max_subpages = 8
        if discovered_pages:
            print(f"      Visiting {len(discovered_pages[:max_subpages])} subpages: {[p.split('/')[-2] if p.endswith('/') else p.split('/')[-1] for p in discovered_pages[:max_subpages]]}")
        else:
            has_soup = homepage_soup is not None
            nav_count = len(homepage_soup.find_all(['nav', 'header'])) if homepage_soup else 0
            print(f"      No subpages discovered (soup={has_soup}, navs={nav_count})")
        
        subpage_soups = []
        for page_url in discovered_pages[:max_subpages]:
            try:
                rate_limit(0.2, 0.5)
                response = make_request(page_url)
                if not response:
                    continue
                if response.status_code != 200:
                    continue
                
                soup = BeautifulSoup(response.text, 'lxml')
                subpage_soups.append(soup)
                page_text = soup.get_text(separator=' ')
                all_text += "\n" + page_text
                
                if not personal_email:
                    found = self._find_email(soup, page_text, site_domain)
                    if found:
                        log_verbose(f"Subpage email found: {found}")
                        all_emails_found.append(found)
                        if self._is_personal_email(found):
                            if not personal_domain_email:
                                personal_domain_email = found
                        elif self._is_generic_email(found):
                            if not generic_email:
                                generic_email = found
                                log_verbose(f"Stored as generic_email: {generic_email}")
                        else:
                            personal_email = found
                        source = "website"
                
                if not contact:
                    found = self._find_contact_name(soup)
                    if found:
                        contact = found
                        source = "website"
                
                if len(all_contacts) < 8:
                    multi = self._find_multiple_contacts(soup, max_contacts=8)
                    for c in multi:
                        if c['name'].lower() not in {x['name'].lower() for x in all_contacts}:
                            all_contacts.append(c)
                
                if not lead.linkedin:
                    lead.linkedin = self._find_linkedin(soup)
                
            except Exception as e:
                log_verbose(f"Error checking {page_url}: {e}")
                continue
        
        if not contact or (contact and not all_contacts) or len(all_contacts) < 2:
            stage2_reason = "no_contact" if not contact else "single_contact_only"
            print(f"      [Stage 2] Deep DOM scan for names ({stage2_reason})...")
            all_deep_contacts = []
            soups_to_scan = []
            if homepage_soup:
                soups_to_scan.append(homepage_soup)
            soups_to_scan.extend(subpage_soups)
            
            for scan_soup in soups_to_scan:
                deep_contacts = self._deep_dom_scan_for_names(scan_soup)
                for c in deep_contacts:
                    if c['name'].lower() not in {x['name'].lower() for x in all_deep_contacts}:
                        all_deep_contacts.append(c)
                if len(all_deep_contacts) >= 8:
                    break
            
            if all_deep_contacts:
                all_deep_contacts = self._sort_contacts_by_role(all_deep_contacts)
                for c in all_deep_contacts:
                    if c['name'].lower() not in {x['name'].lower() for x in all_contacts}:
                        all_contacts.append(c)
                if not contact:
                    contact = all_deep_contacts[0]['name']
                    source = "website"
                    print(f"      [Stage 2] Found contact: {contact}")
        
        if contact and contact.lower() not in {c['name'].lower() for c in all_contacts}:
            all_contacts.insert(0, {'name': normalize_name(contact), 'title': ''})
        
        if len(all_contacts) > 1:
            primary = all_contacts[0] if contact else None
            rest = all_contacts[1:] if contact else all_contacts
            rest = self._sort_contacts_by_role(rest)
            if primary:
                all_contacts = [primary] + rest
            else:
                all_contacts = rest
        
        known_format = ""
        domain = extract_domain(lead.website)
        if domain:
            for e in all_emails_found:
                if '@' in e and not self._is_generic_email(e) and not self._is_personal_email(e):
                    email_domain = e.split('@')[1].lower()
                    norm_domain = domain.replace('www.', '').lower()
                    if email_domain == norm_domain:
                        known_format = e.lower()
                        break
        
        result['email'] = personal_email or personal_domain_email or generic_email
        result['generic_email'] = generic_email
        result['contact'] = contact
        result['contacts'] = all_contacts[:8]
        result['source'] = source
        result['text'] = all_text[:5000]
        result['known_email_format'] = known_format
        return result
    
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
        
        if len(discovered) < 3:
            for path in self.fallback_page_paths:
                fallback_url = urljoin(base_url, path)
                if fallback_url not in discovered:
                    discovered.append(fallback_url)
                if len(discovered) >= 10:
                    break
        
        return discovered[:8]
    
    def _find_email(self, soup: BeautifulSoup, page_text: str, site_domain: str = "") -> str:
        junk_domains = ['example', 'test', 'domain', 'email@', 'noreply', 'no-reply',
                        'unsubscribe', 'sentry', 'wixpress', 'godaddy', 'squarespace',
                        'wordpress', 'mailchimp', 'googleapis', 'gstatic', 'cloudflare', 'filler@',
                        'wpengine', 'schema.org', 'w3.org', 'jquery', 'bootstrap', 'fontawesome']

        mailto_links = soup.find_all('a', href=re.compile(r'^mailto:', re.I))
        for link in mailto_links:
            href = str(link.get('href', ''))
            email = href.replace('mailto:', '').replace('Mailto:', '').split('?')[0].strip()
            email = email.rstrip('.,;:!?)>]')
            if email and '@' in email:
                if not any(x in email.lower() for x in junk_domains):
                    return email

        all_elements = soup.find_all(True, href=True)
        for elem in all_elements:
            href = str(elem.get('href', ''))
            if 'mailto:' in href.lower() and '@' in href:
                email = re.sub(r'^.*mailto:', '', href, flags=re.I).split('?')[0].strip()
                email = email.rstrip('.,;:!?)>]')
                if email and '@' in email:
                    if not any(x in email.lower() for x in junk_domains):
                        return email

        for elem in soup.find_all('a', href=True):
            href = str(elem.get('href', ''))
            if 'mailto:' in href.lower():
                email_match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,7}', href)
                if email_match:
                    email = email_match.group(0)
                    if not any(x in email.lower() for x in junk_domains):
                        return email

        email_regex = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,7}'
        
        for elem in soup.find_all(True):
            for attr_name, attr_val in elem.attrs.items():
                if attr_name in ('href',):
                    continue
                if isinstance(attr_val, str) and '@' in attr_val:
                    if attr_name.startswith('data-') or attr_name.startswith('aria-') or attr_name == 'alt' or attr_name == 'title' or attr_name == 'content':
                        email_match = re.search(email_regex, attr_val)
                        if email_match:
                            email = email_match.group(0)
                            if not any(x in email.lower() for x in junk_domains):
                                if not site_domain or site_domain.replace('www.', '').lower() in email.lower().split('@')[1]:
                                    log_verbose(f"Email found in {attr_name} attribute: {email}")
                                    return email
        
        raw_html = str(soup)
        raw_mailto = re.findall(r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,7})', raw_html, re.I)
        for email in raw_mailto:
            if not any(x in email.lower() for x in junk_domains):
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
                        if not any(x in email.lower() for x in junk_domains):
                            return email
                    if 'contactPoint' in item:
                        cp = item['contactPoint']
                        if isinstance(cp, dict):
                            email = cp.get('email', '')
                            if email and '@' in email:
                                if not any(x in email.lower() for x in junk_domains):
                                    return email
                        elif isinstance(cp, list):
                            for point in cp:
                                if isinstance(point, dict):
                                    email = point.get('email', '')
                                    if email and '@' in email:
                                        if not any(x in email.lower() for x in junk_domains):
                                            return email
            except (json.JSONDecodeError, TypeError):
                pass
        
        js_scripts = soup.find_all('script')
        for script in js_scripts:
            if script.string:
                js_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,7}', script.string)
                for email in js_emails:
                    if not any(x in email.lower() for x in junk_domains):
                        if site_domain and site_domain.replace('www.', '') in email.lower():
                            return email
        
        text_email = extract_email_from_text(page_text)
        if text_email:
            if site_domain:
                email_domain = text_email.split('@')[1].lower() if '@' in text_email else ''
                site_clean = site_domain.replace('www.', '').lower()
                if email_domain == site_clean:
                    return text_email
            return text_email
        
        if site_domain:
            site_clean = site_domain.replace('www.', '').lower()
            all_text_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,7}', page_text)
            for email in all_text_emails:
                if not any(x in email.lower() for x in junk_domains):
                    email_domain = email.split('@')[1].lower() if '@' in email else ''
                    if email_domain == site_clean:
                        return email
        
        return ""
    
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
    
    def _find_multiple_contacts(self, soup: BeautifulSoup, max_contacts: int = 8) -> List[dict]:
        contacts = []
        seen_names = set()
        
        role_patterns = [
            r'(?:founder|co-founder|owner|director|managing director|ceo|principal|proprietor|partner)',
            r'(?:clinical director|practice owner|lead therapist|head therapist|senior therapist)',
            r'(?:physiotherapist|osteopath|chiropractor|therapist|practitioner|clinician)',
            r'(?:consultant|specialist|coach|instructor|doctor|dentist|dr\.?)',
            r'(?:pilates instructor|yoga teacher|massage therapist|psychotherapist|counsellor)',
            r'(?:nutritionist|dietitian|acupuncturist|reflexologist|homeopath)',
        ]
        combined_role_pattern = '|'.join(role_patterns)
        
        team_sections = soup.find_all(['section', 'div', 'article', 'main'],
            class_=re.compile(r'team|staff|people|about|bio|profile|clinician|practitioner|therapist|expert|member|leadership|instructor|coach|specialist|doctor', re.I))
        
        if not team_sections:
            team_sections = []
            for heading in soup.find_all(['h1', 'h2', 'h3']):
                text = heading.get_text().lower()
                if any(kw in text for kw in self.team_page_keywords):
                    parent = heading.parent
                    if parent:
                        team_sections.append(parent)
        
        if not team_sections:
            team_sections = [soup]
        
        for section in team_sections:
            name_elements = section.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b'])
            for elem in name_elements:
                if len(contacts) >= max_contacts:
                    break
                name_text = clean_text(elem.get_text())
                if not self._looks_like_name(name_text):
                    continue
                name_lower = name_text.lower()
                if name_lower in seen_names:
                    continue
                if not self._is_valid_contact_name(name_text):
                    continue
                
                title = ""
                parent = elem.parent
                if parent:
                    siblings = parent.find_all(['p', 'span', 'div', 'em', 'small', 'h4', 'h5', 'h6'])
                    for sib in siblings:
                        sib_text = clean_text(sib.get_text())
                        if sib_text and sib_text != name_text and len(sib_text) < 100:
                            if re.search(combined_role_pattern, sib_text, re.I):
                                title = sib_text[:80]
                                break
                
                if not title:
                    next_elem = elem.find_next(['p', 'span', 'div', 'em', 'small'])
                    if next_elem:
                        next_text = clean_text(next_elem.get_text())
                        if next_text and len(next_text) < 100:
                            if re.search(combined_role_pattern, next_text, re.I):
                                title = next_text[:80]
                
                seen_names.add(name_lower)
                contacts.append({
                    'name': normalize_name(name_text),
                    'title': title
                })
            
            if len(contacts) >= max_contacts:
                break
        
        if len(contacts) < max_contacts:
            schema_scripts = soup.find_all('script', type='application/ld+json')
            for script in schema_scripts:
                if len(contacts) >= max_contacts:
                    break
                try:
                    data = json.loads(script.string or "")
                    items = []
                    if isinstance(data, dict):
                        items.append(data)
                        if '@graph' in data:
                            items.extend(data['@graph'] if isinstance(data['@graph'], list) else [data['@graph']])
                    elif isinstance(data, list):
                        items.extend(data)
                    
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        for field in ['employee', 'member', 'founder', 'author']:
                            people = item.get(field, [])
                            if isinstance(people, dict):
                                people = [people]
                            if not isinstance(people, list):
                                continue
                            for person in people:
                                if len(contacts) >= max_contacts:
                                    break
                                if not isinstance(person, dict):
                                    continue
                                name = person.get('name', '')
                                if name and self._looks_like_name(name) and name.lower() not in seen_names and self._is_valid_contact_name(name):
                                    title = person.get('jobTitle', '') or person.get('roleName', '')
                                    seen_names.add(name.lower())
                                    contacts.append({
                                        'name': normalize_name(name),
                                        'title': str(title)[:80] if title else ''
                                    })
                except (json.JSONDecodeError, TypeError, AttributeError):
                    pass
        
        if len(contacts) > 1:
            contacts = self._sort_contacts_by_role(contacts)
        
        return contacts[:max_contacts]
    
    def _role_priority(self, title: str) -> int:
        if not title:
            return 99
        title_lower = title.lower()
        priority_tiers = [
            (1, ['founder', 'co-founder', 'owner', 'managing director', 'ceo', 'principal', 'proprietor']),
            (2, ['director', 'partner', 'clinical director', 'practice owner', 'practice lead', 'practice manager']),
            (3, ['lead therapist', 'head therapist', 'senior therapist', 'lead physiotherapist', 'head of']),
            (4, ['consultant', 'specialist', 'senior']),
            (5, ['physiotherapist', 'osteopath', 'chiropractor', 'therapist', 'practitioner', 'clinician']),
            (6, ['coach', 'instructor', 'teacher']),
        ]
        for priority, keywords in priority_tiers:
            if any(kw in title_lower for kw in keywords):
                return priority
        return 99
    
    def _sort_contacts_by_role(self, contacts: List[dict]) -> List[dict]:
        return sorted(contacts, key=lambda c: self._role_priority(c.get('title', '')))
    
    def _deep_dom_scan_for_names(self, soup: BeautifulSoup) -> List[dict]:
        contacts = []
        seen_names = set()
        
        for img in soup.find_all('img', alt=True):
            alt = str(img.get('alt', ''))
            if self._looks_like_name(alt) and self._is_valid_contact_name(alt):
                name_lower = alt.lower()
                if name_lower not in seen_names:
                    seen_names.add(name_lower)
                    contacts.append({'name': normalize_name(alt), 'title': ''})
        
        for elem in soup.find_all(['p', 'span', 'li', 'td']):
            text = clean_text(elem.get_text())
            if not text or len(text) > 200:
                continue
            patterns = [
                r'(?:founded|run|owned|led|managed|created)\s+by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',
                r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s+(?:is|are)\s+(?:the|our|a)\s+(?:founder|owner|director|principal)',
                r'(?:I\'m|I am|My name is|Hi,?\s+I\'m)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',
                r'(?:meet|introducing|welcome)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',
            ]
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    name = match.group(1).strip()
                    if self._looks_like_name(name) and self._is_valid_contact_name(name):
                        name_lower = name.lower()
                        if name_lower not in seen_names:
                            seen_names.add(name_lower)
                            contacts.append({'name': normalize_name(name), 'title': ''})
        
        for card in soup.find_all(['div', 'article', 'li'], class_=re.compile(r'card|member|person|author|bio|profile|avatar', re.I)):
            headings = card.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'span'])
            for heading in headings:
                text = clean_text(heading.get_text())
                if self._looks_like_name(text) and self._is_valid_contact_name(text):
                    name_lower = text.lower()
                    if name_lower not in seen_names:
                        seen_names.add(name_lower)
                        contacts.append({'name': normalize_name(text), 'title': ''})
                        break
        
        return contacts[:8]
    
    def _detect_email_format(self, known_email: str, domain: str) -> str:
        if not known_email or not domain or '@' not in known_email:
            return ""
        local_part = known_email.split('@')[0].lower()
        return known_email.lower()
    
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
    
    COMMON_UK_FIRST_NAMES = {
        'adam', 'alan', 'alex', 'alice', 'amanda', 'amy', 'andrew', 'angela', 'anna', 'anne',
        'anthony', 'beth', 'brian', 'bruce', 'carl', 'carol', 'caroline', 'catherine', 'charlotte',
        'chris', 'christine', 'claire', 'clare', 'colin', 'craig', 'daniel', 'david', 'dawn',
        'deborah', 'diane', 'donna', 'dorothy', 'edward', 'elizabeth', 'emma', 'emily', 'eric',
        'fiona', 'frances', 'frank', 'gareth', 'gary', 'gemma', 'george', 'gill', 'glen', 'glenn',
        'gordon', 'grace', 'graham', 'grant', 'greg', 'hannah', 'harry', 'hayley', 'helen',
        'holly', 'ian', 'jack', 'james', 'jane', 'janet', 'jason', 'jean', 'jennifer', 'jenny',
        'jessica', 'jill', 'joan', 'joanne', 'joe', 'john', 'jonathan', 'joseph', 'julie', 'june',
        'karen', 'kate', 'katherine', 'kathleen', 'kathryn', 'katy', 'keith', 'kelly', 'ken',
        'kevin', 'kim', 'kirsty', 'laura', 'lauren', 'lee', 'leigh', 'linda', 'lisa', 'louise',
        'lucy', 'lynn', 'malcolm', 'margaret', 'maria', 'marie', 'mark', 'martin', 'mary',
        'matthew', 'max', 'megan', 'michael', 'michelle', 'mike', 'natalie', 'neil', 'nicholas',
        'nick', 'nicola', 'nigel', 'oliver', 'olivia', 'pamela', 'patricia', 'patrick', 'paul',
        'paula', 'penny', 'peter', 'philip', 'rachel', 'rebecca', 'richard', 'robert', 'robin',
        'roger', 'rosemary', 'ross', 'ruth', 'sally', 'sam', 'samantha', 'sandra', 'sarah',
        'scott', 'sean', 'sharon', 'simon', 'sophie', 'stephen', 'stuart', 'sue', 'susan',
        'teresa', 'thomas', 'tim', 'timothy', 'tom', 'tony', 'tracy', 'victoria', 'wayne',
        'wendy', 'william', 'zoe',
        'amir', 'anita', 'asha', 'deepak', 'fatima', 'hassan', 'indira', 'jasmine', 'kamal',
        'kumar', 'lakshmi', 'meera', 'mohammad', 'nadia', 'priya', 'raj', 'ravi', 'sanjay',
        'tara', 'vikram', 'yusuf', 'zara',
    }
    
    def _looks_like_name(self, text: str) -> bool:
        if not text or len(text) < 4 or len(text) > 60:
            return False
        words = text.split()
        if len(words) < 2 or len(words) > 5:
            return False
        
        title_prefixes = {'dr', 'dr.', 'mr', 'mr.', 'mrs', 'mrs.', 'ms', 'ms.', 'miss',
                          'prof', 'prof.', 'professor', 'rev', 'rev.', 'sir', 'dame'}
        qualification_suffixes = {'bsc', 'msc', 'phd', 'dphil', 'frcs', 'mbbs', 'mrcgp',
                                   'mcsp', 'hcpc', 'mbacp', 'ukcp', 'babcp', 'pgdip',
                                   'diphe', 'ba', 'ma', 'hons', 'fhea', 'pgcert',
                                   'mphil', 'mchiro', 'dosth', 'dip', 'cert', 'accred',
                                   'registered', 'chartered', 'fellow'}
        
        name_words = []
        for w in words:
            w_clean = w.strip('.,()').lower()
            if w_clean in title_prefixes:
                continue
            if w_clean in qualification_suffixes:
                continue
            if w_clean.startswith('(') or w_clean.endswith(')'):
                continue
            name_words.append(w)
        
        if len(name_words) < 2:
            return False
        
        for word in name_words:
            clean_word = word.replace("'", "").replace("-", "").replace(".", "")
            if len(clean_word) < 2:
                continue
            if not clean_word[0].isupper():
                return False
            remaining = clean_word[1:]
            if not all(c.isalpha() for c in remaining):
                return False
        
        exclude_words = {'the', 'and', 'our', 'team', 'about', 'contact',
                        'welcome', 'meet', 'staff',
                        'ltd', 'limited', 'inc', 'clinic', 'practice', 'services',
                        'physiotherapy', 'osteopathy', 'chiropractic', 'dental', 'therapy',
                        'health', 'wellness', 'centre', 'center', 'studio',
                        'home', 'page', 'privacy', 'policy', 'terms', 'conditions',
                        'read', 'more', 'view', 'all', 'latest', 'news', 'book', 'online',
                        'new', 'title', 'menu', 'dropdown', 'items', 'custom', 'scroll',
                        'surgery', 'hospital', 'pharmacy', 'university', 'college'}
        if any(w.lower() in exclude_words for w in name_words):
            return False
        
        first_name_lower = name_words[0].replace("'", "").replace("-", "").lower()
        if first_name_lower in self.COMMON_UK_FIRST_NAMES:
            return True
        
        for word in name_words:
            clean_word = word.replace("'", "").replace("-", "").replace(".", "")
            if len(clean_word) >= 3:
                vowels = sum(1 for c in clean_word.lower() if c in 'aeiouy')
                if vowels == 0:
                    return False
        
        first_word = name_words[0].replace("'", "").replace("-", "")
        if first_word[0].isupper() and first_word[1:].islower():
            return True
        
        return False
    
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
        
        search_queries = [
            f'site:linkedin.com/in "{lead.company_name}" owner founder director',
        ]
        
        town = lead.search_town or (lead.location.split(',')[0] if lead.location else "")
        if town:
            search_queries.append(f'site:linkedin.com/in "{lead.company_name}" {town}')
        
        sector = (lead.sector or '').lower()
        if any(kw in sector for kw in ['physio', 'osteo', 'chiro', 'mental', 'massage', 'yoga', 'pilates', 'nutrition', 'wellness', 'aesthetics', 'dental']):
            search_queries.append(f'site:linkedin.com/in "{lead.company_name}" practitioner therapist lead')
        
        for search_query in search_queries[:2]:
            try:
                search_url = f"https://www.bing.com/search?q={quote_plus(search_query)}"
                
                response = requests.get(
                    search_url,
                    headers=get_headers(),
                    timeout=10
                )
                
                if response.status_code != 200:
                    continue
                
                soup = BeautifulSoup(response.text, 'lxml')
                
                results = soup.find_all(['li', 'div'], class_=re.compile(r'b_algo|result', re.I))
                if not results:
                    results = soup.find_all('li')
                
                for result in results[:5]:
                    text = result.get_text()
                    
                    if 'linkedin.com/in/' in text.lower():
                        title_patterns = [
                            r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s*[-–|]\s*(?:founder|owner|director|ceo|managing|principal|lead|head|senior)',
                            r'(?:founder|owner|director|ceo|managing|principal|lead|head)[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                            r'^([A-Z][a-z]+\s+[A-Z][a-z]+)\s+[-|]',
                            r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s*[-–|]\s*(?:physiotherapist|osteopath|chiropractor|therapist|practitioner|clinician|consultant)',
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
        if skip_complete and enricher._is_fully_enriched(lead):
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
