import time
import random
import re
import os
import csv
from typing import List, Set, Optional
from fake_useragent import UserAgent
import requests

from .models import BusinessLead

ua = UserAgent()

def get_headers() -> dict:
    return {
        "User-Agent": ua.random,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

def rate_limit(min_seconds: float = 1.0, max_seconds: float = 3.0):
    time.sleep(random.uniform(min_seconds, max_seconds))

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

def make_request(url: str, timeout: int = 15) -> Optional[requests.Response]:
    try:
        response = requests.get(url, headers=get_headers(), timeout=timeout)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        print(f"Request error for {url}: {e}")
        return None
