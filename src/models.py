from dataclasses import dataclass, field, asdict
from typing import Optional

@dataclass
class BusinessLead:
    company_name: str
    website: str = ""
    sector: str = ""
    contact_name: str = ""
    email: str = ""
    linkedin: str = ""
    location: str = ""
    employee_count: str = ""
    source: str = ""
    ai_score: str = ""
    ai_reason: str = ""
    tag: str = ""
    phone: str = ""
    google_rating: str = ""
    place_id: str = ""
    search_town: str = ""
    category: str = ""  # 'unit8' for wellness/clinical, 'office' for general office
    enrichment_source: str = ""  # 'website', 'linkedin', 'companies_house', 'openai', 'guessed_email', 'not_found'
    enrichment_status: str = ""  # 'complete', 'missing_email', 'missing_name', 'incomplete'
    ai_enriched: str = ""  # 'true' if OpenAI was used for enrichment
    email_guessed: str = ""  # 'true' if email was guessed from name+domain pattern
    contact_verified: str = ""  # 'true' if contact found from authoritative source (CH, website)
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    def get_key(self) -> str:
        return f"{self.company_name.lower().strip()}|{self.email.lower().strip()}"
    
    def get_website_key(self) -> str:
        if not self.website:
            return ""
        website = self.website.lower().replace("http://", "").replace("https://", "").replace("www.", "").rstrip("/")
        return website
    
    def get_name_location_key(self) -> str:
        name = self.company_name.lower().strip()
        location = self.location.lower().strip() if self.location else ""
        return f"{name}|{location}"
