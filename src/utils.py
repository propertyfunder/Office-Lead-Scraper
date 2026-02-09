import time
import random
import re
import os
import csv
from typing import List, Set, Optional, Tuple
from fake_useragent import UserAgent
import requests

from .models import BusinessLead

ua = UserAgent()

VERBOSE = False

def set_verbose(verbose: bool):
    global VERBOSE
    VERBOSE = verbose

def log_verbose(message: str):
    if VERBOSE:
        print(f"  [DEBUG] {message}")

def get_headers(referer: str = "") -> dict:
    headers = {
        "User-Agent": ua.random,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
        "DNT": "1",
    }
    if referer:
        headers["Referer"] = referer
        headers["Sec-Fetch-Site"] = "same-origin"
    return headers

def rate_limit(min_seconds: float = 1.0, max_seconds: float = 3.0):
    delay = random.uniform(min_seconds, max_seconds)
    log_verbose(f"Rate limiting: waiting {delay:.1f}s")
    time.sleep(delay)

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def extract_email_from_text(text: str) -> str:
    if not text:
        return ""
    tld_list = r'(?:com|co\.uk|org\.uk|nhs\.net|nhs\.uk|ac\.uk|gov\.uk|org|net|uk|io|info|biz|me|health|therapy|yoga|clinic|dental|physio|care|education|studio|space|pro|solutions|services|tech|online|live|cloud|app|dev|design|consulting|london|wales|scot)'
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.' + tld_list + r'(?=[^a-zA-Z.]|$)'
    matches = re.findall(email_pattern, text, re.I)
    if not matches:
        fallback_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}(?=[^a-zA-Z]|$)'
        matches = re.findall(fallback_pattern, text)
    junk = ['example.com', 'test.com', 'domain.com', '.png', '.jpg', '.gif',
            'sentry', 'wixpress', 'godaddy', 'squarespace', 'wordpress',
            'mailchimp', 'googleapis', 'gstatic', 'cloudflare']
    for email in matches:
        if not any(x in email.lower() for x in junk):
            return email
    return ""

def guess_email(company_name: str, contact_name: str, domain: str = "") -> str:
    if not contact_name or not domain:
        return ""
    name_parts = contact_name.lower().split()
    if len(name_parts) < 2:
        return ""
    first_name = re.sub(r'[^a-z]', '', name_parts[0])
    last_name = re.sub(r'[^a-z]', '', name_parts[-1])
    domain = domain.replace("www.", "").replace("http://", "").replace("https://", "").split("/")[0]
    return f"{first_name}.{last_name}@{domain}"

def generate_email_guesses(contact_name: str, domain: str, known_format: str = "") -> List[str]:
    if not contact_name or not domain:
        return []
    name_parts = contact_name.strip().lower().split()
    if len(name_parts) < 2:
        return []
    first_name = re.sub(r'[^a-z]', '', name_parts[0])
    last_name = re.sub(r'[^a-z]', '', name_parts[-1])
    domain = domain.replace("www.", "").replace("http://", "").replace("https://", "").split("/")[0].strip().lower()
    if not first_name or not last_name or not domain or '.' not in domain:
        return []
    guesses = [
        f"{first_name}.{last_name}@{domain}",
        f"{first_name[0]}.{last_name}@{domain}",
        f"{first_name}@{domain}",
        f"{last_name}@{domain}",
        f"{first_name}_{last_name}@{domain}",
        f"{first_name}{last_name}@{domain}",
        f"{first_name}{last_name[0]}@{domain}",
        f"{first_name[0]}{last_name[0]}@{domain}",
    ]
    if known_format:
        known_clean = known_format.strip().lower()
        if known_clean not in guesses:
            guesses.insert(0, known_clean)
        else:
            guesses.remove(known_clean)
            guesses.insert(0, known_clean)
    valid_email_re = re.compile(r'^[a-z][a-z0-9._%+-]*@[a-z0-9.-]+\.[a-z]{2,7}$')
    seen = []
    for g in guesses:
        g = g.strip().lower().rstrip('.,;:!?')
        if g not in seen and valid_email_re.match(g):
            seen.append(g)
    return seen

def extract_domain(url: str) -> str:
    if not url:
        return ""
    url = url.replace("http://", "").replace("https://", "").replace("www.", "")
    return url.split("/")[0]

def clean_email(email: str) -> str:
    if not email:
        return ""
    email = re.sub(r'<[^>]+>', '', email)
    email = re.sub(r'[<>"\'\[\](){}]', '', email)
    email_pattern = r'[a-zA-Z][a-zA-Z0-9._%+-]*@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,7}\b'
    match = re.search(email_pattern, email)
    if match:
        email = match.group(0)
    else:
        return ""
    tld_pattern = r'(\.(com|co\.uk|org|org\.uk|net|uk|io|info|biz|me|health|therapy|yoga|clinic|dental|physio|care|education|studio|space|nhs\.net|nhs\.uk|ac\.uk|gov\.uk))'
    tld_match = re.search(tld_pattern, email, re.I)
    if tld_match:
        email = email[:tld_match.end()]
    junk = ['sentry', 'wixpress', 'godaddy', 'squarespace', 'wordpress',
            'mailchimp', 'googleapis', 'gstatic', 'cloudflare', 'filler@']
    if any(x in email.lower() for x in junk):
        return ""
    email = email.strip().lower()
    if '@' in email and '.' in email.split('@')[-1]:
        return email
    return ""

def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = re.sub(r'<[^>]+>', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    words = name.split()
    return ' '.join(word.capitalize() for word in words)

def get_all_fieldnames():
    from dataclasses import fields as dc_fields
    return [f.name for f in dc_fields(BusinessLead)]

def save_leads_to_csv(leads: List[BusinessLead], filepath: str, mode: str = 'a'):
    file_exists = os.path.exists(filepath) and os.path.getsize(filepath) > 0
    fieldnames = get_all_fieldnames()
    with open(filepath, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        if not file_exists or mode == 'w':
            writer.writeheader()
        for lead in leads:
            writer.writerow(lead.to_dict())

def load_existing_keys(filepath: str) -> Set[str]:
    keys = set()
    if not os.path.exists(filepath):
        return keys
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get('company_name', '').lower().strip()
            email = row.get('email', '').lower().strip()
            keys.add(f"{name}|{email}")
    return keys

def load_existing_leads_for_dedup(filepath: str) -> dict:
    existing = {
        'name_location': set(),
        'websites': set(),
        'place_ids': set()
    }
    if not os.path.exists(filepath):
        return existing
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get('company_name', '').lower().strip()
            location = row.get('location', '').lower().strip()
            website = row.get('website', '').lower().replace("http://", "").replace("https://", "").replace("www.", "").rstrip("/")
            place_id = row.get('place_id', '').strip()
            
            if name and location:
                existing['name_location'].add(f"{name}|{location}")
            if website:
                existing['websites'].add(website)
            if place_id:
                existing['place_ids'].add(place_id)
    return existing

def is_duplicate_lead(lead, existing_data: dict) -> bool:
    if lead.place_id and lead.place_id in existing_data['place_ids']:
        return True
    
    website_key = lead.get_website_key()
    if website_key and website_key in existing_data['websites']:
        return True
    
    name_loc_key = lead.get_name_location_key()
    if name_loc_key and name_loc_key in existing_data['name_location']:
        return True
    
    return False

def add_lead_to_existing(lead, existing_data: dict):
    name_loc_key = lead.get_name_location_key()
    if name_loc_key:
        existing_data['name_location'].add(name_loc_key)
    
    website_key = lead.get_website_key()
    if website_key:
        existing_data['websites'].add(website_key)
    
    if lead.place_id:
        existing_data['place_ids'].add(lead.place_id)

def is_target_sector(description: str) -> bool:
    target_keywords = [
        'accountant', 'accounting', 'lawyer', 'legal', 'solicitor', 'law firm',
        'recruiter', 'recruitment', 'staffing', 'hr ', 'human resources',
        'consultant', 'consulting', 'advisory', 'advisor',
        'software', 'technology', 'tech', 'developer', 'development', 'it services',
        'engineering', 'engineer', 'r&d', 'research', 'design',
        'digital', 'marketing', 'media', 'creative', 'agency', 'advertising',
        'clean energy', 'renewable', 'environmental', 'sustainability', 'green',
        'professional services', 'business services', 'management'
    ]
    exclude_keywords = [
        'retail', 'shop', 'store', 'logistics', 'warehouse', 'transport',
        'plumber', 'electrician', 'builder', 'construction', 'trades',
        'industrial', 'manufacturing', 'factory', 'restaurant', 'cafe',
        'hairdresser', 'salon', 'beauty', 'takeaway', 'food'
    ]
    desc_lower = description.lower()
    if any(keyword in desc_lower for keyword in exclude_keywords):
        return False
    return any(keyword in desc_lower for keyword in target_keywords)

def detect_block_or_captcha(response: requests.Response) -> Tuple[bool, str]:
    if response.status_code == 403:
        return True, "403 Forbidden - Access blocked"
    if response.status_code == 429:
        return True, "429 Too Many Requests - Rate limited"
    
    content_lower = response.text.lower()
    
    captcha_indicators = [
        'captcha', 'recaptcha', 'hcaptcha', 'challenge-form',
        'verify you are human', 'robot', 'automated access',
        'unusual traffic', 'security check', 'access denied',
        'blocked', 'forbidden'
    ]
    
    for indicator in captcha_indicators:
        if indicator in content_lower:
            return True, f"Possible CAPTCHA/block detected: '{indicator}' found in response"
    
    if len(response.text) < 1000 and 'javascript' in content_lower:
        return True, "Possible JavaScript challenge page (minimal content)"
    
    return False, ""

FAILED_URLS_LOG = "failed_urls.log"

def log_failed_url(url: str, company_name: str, reason: str):
    try:
        with open(FAILED_URLS_LOG, 'a', encoding='utf-8') as f:
            f.write(f"{company_name}\t{url}\t{reason}\n")
    except:
        pass

def make_request_with_retry(
    url: str, 
    max_retries: int = 3, 
    timeout: int = 15,
    referer: str = ""
) -> Tuple[Optional[requests.Response], str]:
    backoff_times = [2, 5, 10]
    last_error = ""
    
    for attempt in range(max_retries):
        try:
            log_verbose(f"Request attempt {attempt + 1}/{max_retries}: {url[:80]}...")
            
            headers = get_headers(referer)
            if attempt > 0:
                headers["User-Agent"] = ua.random
            if attempt >= 1:
                from urllib.parse import urlparse
                parsed = urlparse(url)
                headers["Referer"] = f"{parsed.scheme}://{parsed.netloc}/"
                headers["Sec-Fetch-Site"] = "same-origin"
            
            session = requests.Session()
            response = session.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            
            if response.status_code == 200:
                is_blocked, block_reason = detect_block_or_captcha(response)
                if is_blocked:
                    log_verbose(f"Block detected: {block_reason}")
                    last_error = block_reason
                    if attempt < max_retries - 1:
                        wait_time = backoff_times[min(attempt, len(backoff_times) - 1)]
                        log_verbose(f"Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                else:
                    log_verbose(f"Success: {len(response.text)} bytes received")
                    return response, ""
            
            response.raise_for_status()
            return response, ""
            
        except requests.exceptions.HTTPError as e:
            status_code = getattr(e.response, 'status_code', 'unknown') if hasattr(e, 'response') else 'unknown'
            last_error = f"HTTP {status_code}: {str(e)}"
            log_verbose(f"HTTP error: {last_error}")
        except requests.exceptions.ConnectionError as e:
            last_error = f"Connection error: {str(e)}"
            log_verbose(f"Connection error: {last_error}")
        except requests.exceptions.Timeout as e:
            last_error = f"Timeout: {str(e)}"
            log_verbose(f"Timeout: {last_error}")
        except requests.RequestException as e:
            last_error = str(e)
            log_verbose(f"Request error: {last_error}")
        
        if attempt < max_retries - 1:
            wait_time = backoff_times[min(attempt, len(backoff_times) - 1)]
            log_verbose(f"Retrying in {wait_time}s...")
            time.sleep(wait_time)
    
    return None, last_error

def make_request(url: str, timeout: int = 15, referer: str = "") -> Optional[requests.Response]:
    response, error = make_request_with_retry(url, max_retries=3, timeout=timeout, referer=referer)
    if error:
        print(f"Request failed for {url[:60]}...: {error}")
    return response
