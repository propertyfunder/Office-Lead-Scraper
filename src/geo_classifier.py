import re
from typing import Tuple
from bs4 import BeautifulSoup
from .utils import make_request, extract_domain, log_verbose

GU_POSTCODES_RE = re.compile(r'\bGU\d{1,2}\s?\d[A-Z]{2}\b', re.I)
SURREY_LOCATIONS = [
    "godalming", "guildford", "farnham", "woking", "haslemere",
    "cranleigh", "milford", "shalford", "compton", "bramley",
    "hindhead", "elstead", "witley", "chiddingfold", "dunsfold",
    "alfold", "busbridge", "hascombe", "shackleford", "puttenham",
    "thursley", "farncombe", "eashing", "hurtmore", "peperharow",
    "hambledon", "surrey", "waverley", "weybridge", "camberley",
    "aldershot", "bordon", "liphook", "petersfield",
]
SURREY_RE = re.compile(
    r'\b(?:' + '|'.join(re.escape(t) for t in SURREY_LOCATIONS) + r')\b', re.I
)

LOCAL_SECTORS = {
    "architecture", "accounting", "legal", "property management",
    "training coaching", "training & coaching",
}

EXCLUDE_KEYWORDS = [
    "immigration", "visa application", "tier 2", "tier 4",
    "sponsor licence", "right to work",
    "us law", "united states law", "attorney at law", "bar association",
    "new york", "california", "washington dc", "llc formation",
    "student recruitment", "international student", "overseas agent",
    "study abroad", "education agent",
    "offshore", "cayman", "bvi company", "tax haven",
]
EXCLUDE_RE = re.compile(
    r'\b(?:' + '|'.join(re.escape(k) for k in EXCLUDE_KEYWORDS) + r')\b', re.I
)

REVIEW_KEYWORDS = [
    "nationwide", "national coverage", "uk-wide", "across the uk",
    "global offices", "international offices", "offices worldwide",
    "clients worldwide", "global reach",
]
REVIEW_RE = re.compile(
    r'\b(?:' + '|'.join(re.escape(k) for k in REVIEW_KEYWORDS) + r')\b', re.I
)

DORMANT_KEYWORDS = [
    "coming soon", "under construction", "site is being updated",
    "website is under development", "launching soon", "watch this space",
    "parked domain", "this domain", "domain for sale", "buy this domain",
    "website coming", "page is under construction",
]
DORMANT_RE = re.compile(
    r'\b(?:' + '|'.join(re.escape(k) for k in DORMANT_KEYWORDS) + r')\b', re.I
)

SHELL_KEYWORDS = [
    "holding company", "dormant company", "non-trading",
    "no trading activity", "shell company", "nominee",
    "registered office only", "registered address only",
]
SHELL_RE = re.compile(
    r'\b(?:' + '|'.join(re.escape(k) for k in SHELL_KEYWORDS) + r')\b', re.I
)


def classify_geo_relevance(
    website: str = "",
    location: str = "",
    sector: str = "",
    generic_email: str = "",
    company_name: str = "",
    page_text: str = "",
    soup: BeautifulSoup = None,
) -> Tuple[str, str]:
    reasons = []
    has_local_signal = False
    has_exclude_signal = False
    has_review_signal = False

    text = page_text.lower() if page_text else ""
    loc_lower = (location or "").lower()

    if GU_POSTCODES_RE.search(loc_lower):
        has_local_signal = True
        reasons.append("gu_postcode_in_address")

    if SURREY_RE.search(loc_lower):
        has_local_signal = True

    if text:
        if GU_POSTCODES_RE.search(text):
            has_local_signal = True
            reasons.append("gu_postcode_on_website")

        if SURREY_RE.search(text):
            has_local_signal = True
            reasons.append("surrey_location_on_website")

        if EXCLUDE_RE.search(text):
            has_exclude_signal = True
            matches = EXCLUDE_RE.findall(text)
            reasons.append(f"exclude_keywords:{','.join(set(m.lower() for m in matches[:3]))}")

        if SHELL_RE.search(text):
            has_exclude_signal = True
            reasons.append("shell_or_holding_company")

        if DORMANT_RE.search(text):
            has_review_signal = True
            reasons.append("dormant_or_parked_website")

        if REVIEW_RE.search(text):
            if not has_local_signal:
                has_review_signal = True
                reasons.append("national_or_international_scope")

        word_count = len(text.split())
        if word_count < 50 and not has_local_signal:
            has_review_signal = True
            reasons.append("very_thin_website_content")
    elif website:
        has_review_signal = True
        reasons.append("no_website_content_available")

    if generic_email and website:
        email_domain = generic_email.split("@")[-1].lower() if "@" in generic_email else ""
        site_domain = extract_domain(website).lower()
        if email_domain and site_domain and email_domain != site_domain:
            if not email_domain.startswith(site_domain.split(".")[0]):
                has_review_signal = True
                reasons.append(f"email_domain_mismatch:{email_domain}!={site_domain}")

    if sector:
        sector_lower = sector.lower().replace("_", " ")
        if sector_lower in LOCAL_SECTORS or any(s in sector_lower for s in LOCAL_SECTORS):
            if has_local_signal or GU_POSTCODES_RE.search(loc_lower):
                has_local_signal = True

    cn_lower = (company_name or "").lower()
    shell_in_name = any(w in cn_lower for w in (
        "holdings", "holding", "nominees", "trustees", "pension",
        "dormant", "property investments",
    ))
    if shell_in_name and not text:
        has_exclude_signal = True
        reasons.append("shell_name_no_website")
    elif shell_in_name and not has_local_signal:
        has_review_signal = True
        reasons.append("possible_shell_name")

    if not website and not text and not GU_POSTCODES_RE.search(loc_lower):
        has_review_signal = True
        if "no_website_content_available" not in reasons:
            reasons.append("no_website_no_location_signal")

    reason_str = "; ".join(reasons) if reasons else ""

    if has_exclude_signal:
        return "exclude", reason_str

    strong_review = any(r in reason_str for r in (
        "national_or_international_scope",
        "shell_or_holding_company",
        "possible_shell_name",
        "very_thin_website_content",
        "no_website_content_available",
        "no_website_no_location_signal",
    ))
    if has_review_signal and (not has_local_signal or strong_review):
        return "review", reason_str
    if has_local_signal:
        return "local", reason_str
    return "review", reason_str


def classify_from_website(
    website: str,
    location: str = "",
    sector: str = "",
    generic_email: str = "",
    company_name: str = "",
) -> Tuple[str, str]:
    page_text = ""
    soup = None

    if website:
        is_social = any(d in website.lower() for d in (
            "facebook.com", "fb.com", "instagram.com", "twitter.com",
            "linkedin.com", "tiktok.com",
        ))
        if not is_social:
            try:
                response = make_request(website, timeout=10)
                if response and response.status_code < 400:
                    soup = BeautifulSoup(response.text, "lxml")
                    page_text = soup.get_text(separator=" ")
            except Exception as e:
                log_verbose(f"Geo classifier fetch error for {website}: {e}")

    return classify_geo_relevance(
        website=website,
        location=location,
        sector=sector,
        generic_email=generic_email,
        company_name=company_name,
        page_text=page_text,
        soup=soup,
    )
