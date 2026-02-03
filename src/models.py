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
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    def get_key(self) -> str:
        return f"{self.company_name.lower().strip()}|{self.email.lower().strip()}"
