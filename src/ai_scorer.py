import os
import json
from typing import Optional
from openai import OpenAI

from .models import BusinessLead

class AILeadScorer:
    def __init__(self):
        api_key = os.environ.get('OPENAI_API_KEY')
        self.client = OpenAI(api_key=api_key) if api_key else None
        self.enabled = self.client is not None
        
    def score_lead(self, lead: BusinessLead) -> BusinessLead:
        if not self.enabled:
            return lead
            
        try:
            context = self._build_context(lead)
            
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": """You are a lead qualification expert for commercial office leasing in Surrey, UK.
Analyze businesses and score their likelihood of needing flexible office space.

Consider:
- Professional services firms (accounting, legal, consulting) = HIGH potential
- Tech/software companies with 5-50 employees = HIGH potential  
- Marketing/creative agencies = HIGH potential
- Engineering/architecture firms = MEDIUM-HIGH potential
- Established corporates (100+ employees) = LOW potential (already have offices)
- Retail/trades/logistics = EXCLUDE (not target market)

Return JSON only: {"score": 1-10, "reason": "brief reason", "sector": "refined sector name"}"""},
                    {"role": "user", "content": context}
                ],
                max_tokens=150,
                temperature=0.3
            )
            
            result = json.loads(response.choices[0].message.content)
            
            if result.get('score'):
                lead.ai_score = str(result['score'])
            if result.get('reason'):
                lead.ai_reason = result['reason']
            if result.get('sector') and len(result['sector']) > 3:
                lead.sector = result['sector']
                
        except Exception as e:
            pass
            
        return lead
    
    def _build_context(self, lead: BusinessLead) -> str:
        parts = [f"Company: {lead.company_name}"]
        if lead.sector:
            parts.append(f"Description: {lead.sector[:300]}")
        if lead.location:
            parts.append(f"Location: {lead.location}")
        if lead.employee_count:
            parts.append(f"Employees: {lead.employee_count}")
        if lead.website:
            parts.append(f"Website: {lead.website}")
        return "\n".join(parts)
