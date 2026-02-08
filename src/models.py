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
    category: str = ""
    enrichment_source: str = ""
    enrichment_status: str = ""
    ai_enriched: str = ""
    email_guessed: str = ""
    contact_verified: str = ""
    generic_email: str = ""
    contact_names: str = ""
    personal_email_guesses: str = ""
    contact_titles: str = ""
    multiple_contacts: str = ""
    principal_name: str = ""
    principal_email_guess: str = ""
    facebook_url: str = ""
    contact_email: str = ""
    team_email_guesses: str = ""
    email_type: str = ""
    name_review_needed: str = ""
    missing_email: str = ""
    website_verified: str = ""
    data_score: str = ""
    enrichment_attempts: str = ""
    confidence_score: str = ""
    refinement_notes: str = ""
    contact_source: str = ""
    last_enriched_date: str = ""
    mailshot_category: str = ""
    
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
