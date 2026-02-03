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
        "Accept-Encoding": "gzip, deflate, br",
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
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    matches = re.findall(email_pattern, text)
    for email in matches:
        if not any(x in email.lower() for x in ['example.com', 'test.com', 'domain.com', '.png', '.jpg', '.gif']):
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

def extract_domain(url: str) -> str:
    if not url:
        return ""
    url = url.replace("http://", "").replace("https://", "").replace("www.", "")
    return url.split("/")[0]

def save_leads_to_csv(leads: List[BusinessLead], filepath: str, mode: str = 'a'):
    file_exists = os.path.exists(filepath) and os.path.getsize(filepath) > 0
    fieldnames = [
        'company_name', 'website', 'sector', 'contact_name', 
        'email', 'linkedin', 'location', 'employee_count', 'source'
    ]
    with open(filepath, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
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
            response = requests.get(url, headers=headers, timeout=timeout)
            
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
