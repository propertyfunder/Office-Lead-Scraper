import os
import json
from typing import Optional
from openai import OpenAI

from .models import BusinessLead

class AILeadScorer:
    def __init__(self, wellness_mode: bool = False):
        api_key = os.environ.get('OPENAI_API_KEY')
        self.client = OpenAI(api_key=api_key) if api_key else None
        self.enabled = self.client is not None
        self.wellness_mode = wellness_mode
        
    def score_lead(self, lead: BusinessLead) -> BusinessLead:
        if not self.enabled:
            return lead
            
        try:
            context = self._build_context(lead)
            system_prompt = self._get_system_prompt()
            
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": context}
                ],
                max_tokens=200,
                temperature=0.3
            )
            
            result = json.loads(response.choices[0].message.content)
            
            if result.get('score'):
                lead.ai_score = str(result['score'])
            if result.get('reason'):
                lead.ai_reason = result['reason']
            if result.get('sector') and len(result['sector']) > 3:
                lead.sector = result['sector']
            if self.wellness_mode and result.get('tag'):
                lead.tag = result['tag']
                
        except Exception:
            pass
            
        return lead
    
    def _get_system_prompt(self) -> str:
        if self.wellness_mode:
            return """You are a lead qualification expert for Unit 8 at Godalming Business Centre in Surrey, UK.
Unit 8 is a small professional space ideal for clinical, therapeutic, and wellness businesses.

Features of Unit 8:
- Small, professional, quiet space
- Parking available
- Reception access
- Suitable for client-facing health/wellness practices

Score businesses on their suitability for Unit 8 (1-10):

HIGH SUITABILITY (8-10):
- Physiotherapists, osteopaths, chiropractors (need clinical space)
- Private GPs or small health clinics
- Psychotherapists, counsellors, mental health professionals
- Small pilates/yoga studios (group classes)
- Massage therapists, acupuncture practitioners
- Cosmetic dentists or private dental practices
- Holistic health practitioners

MEDIUM SUITABILITY (5-7):
- Larger practices that might outgrow the space
- Businesses currently home-based looking to upgrade
- Nutritionists or wellness coaches

LOW SUITABILITY (1-4):
- Large gym chains or fitness franchises
- NHS or public sector clinics
- Businesses already in purpose-built premises
- Retail-focused businesses

Return JSON only: {"score": 1-10, "reason": "brief reason for suitability", "sector": "refined sector name", "tag": "wellness or clinic-target"}"""
        else:
            return """You are a lead qualification expert for commercial office leasing in Surrey, UK.
Analyze businesses and score their likelihood of needing flexible office space.

Consider:
- Professional services firms (accounting, legal, consulting) = HIGH potential
- Tech/software companies with 5-50 employees = HIGH potential  
- Marketing/creative agencies = HIGH potential
- Engineering/architecture firms = MEDIUM-HIGH potential
- Established corporates (100+ employees) = LOW potential (already have offices)
- Retail/trades/logistics = EXCLUDE (not target market)

Return JSON only: {"score": 1-10, "reason": "brief reason", "sector": "refined sector name"}"""
    
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
