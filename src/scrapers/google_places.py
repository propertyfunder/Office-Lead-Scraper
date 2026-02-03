import os
from typing import Generator, Optional
import requests

from .base_scraper import BaseScraper
from ..models import BusinessLead
from ..utils import rate_limit, clean_text, log_verbose

class GooglePlacesScraper(BaseScraper):
    def __init__(self, town: str, sector: str = "", api_key: str = ""):
        super().__init__(town, sector)
        self.source_name = "Google Places"
        self.api_key = api_key or os.environ.get("GOOGLE_MAPS_API_KEY", "")
        self.base_url = "https://places.googleapis.com/v1/places:searchText"
        
        self.town_coords = {
            "Guildford": {"lat": 51.2362, "lng": -0.5704},
            "Godalming": {"lat": 51.1859, "lng": -0.6174},
            "Farnham": {"lat": 51.2146, "lng": -0.7995},
            "Woking": {"lat": 51.3162, "lng": -0.5600},
        }
        
        self.search_types = [
            "accountants",
            "lawyers",
            "solicitors", 
            "recruitment agencies",
            "IT companies",
            "software companies",
            "marketing agencies",
            "consulting firms",
            "engineering companies",
            "architects",
        ]
    
    def is_available(self) -> bool:
        return bool(self.api_key)
    
    @property
    def api_failed(self) -> bool:
        return getattr(self, '_api_failed', False)
    
    @api_failed.setter
    def api_failed(self, value: bool):
        self._api_failed = value
    
    def scrape(self, max_pages: int = 3) -> Generator[BusinessLead, None, None]:
        if not self.api_key:
            print(f"  [Google Places] No API key found. Set GOOGLE_MAPS_API_KEY env var.")
            self.api_failed = True
            return
        
        if not self._test_api():
            print(f"  [Google Places] API test failed - check your API key and Places API is enabled")
            self.api_failed = True
            return
        
        coords = self.town_coords.get(self.town) or self.town_coords["Guildford"]
        search_terms = [self.sector] if self.sector else self.search_types
        total_found = 0
        
        for term in search_terms:
            query = f"{term} in {self.town} UK"
            print(f"  [Google Places] Searching: {query}")
            
            try:
                headers = {
                    "Content-Type": "application/json",
                    "X-Goog-Api-Key": self.api_key,
                    "X-Goog-FieldMask": "places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.internationalPhoneNumber,places.websiteUri,places.rating,places.userRatingCount,places.types,places.businessStatus"
                }
                
                data = {
                    "textQuery": query,
                    "locationBias": {
                        "circle": {
                            "center": {"latitude": coords["lat"], "longitude": coords["lng"]},
                            "radius": 15000.0
                        }
                    },
                    "maxResultCount": min(max_pages * 20, 60)
                }
                
                response = requests.post(self.base_url, json=data, headers=headers, timeout=15)
                
                if response.status_code == 403:
                    print(f"  [Google Places] API access denied - check Places API is enabled in Google Cloud Console")
                    self.api_failed = True
                    return
                
                if response.status_code == 400:
                    error_msg = response.json().get("error", {}).get("message", "Unknown error")
                    print(f"  [Google Places] API error: {error_msg}")
                    continue
                
                if response.status_code != 200:
                    log_verbose(f"API returned status {response.status_code}")
                    continue
                
                places = response.json().get("places", [])
                
                for place in places:
                    lead = self._parse_place(place)
                    if lead and self._is_professional_service(place):
                        total_found += 1
                        yield lead
                
                rate_limit(0.3, 0.5)
                
            except requests.exceptions.Timeout:
                print(f"  [Google Places] Request timeout")
                continue
            except requests.exceptions.RequestException as e:
                print(f"  [Google Places] Network error: {str(e)[:50]}")
                continue
            except Exception as e:
                log_verbose(f"Places API error: {e}")
                continue
        
        print(f"  [Google Places] Total found: {total_found} leads")
    
    def _test_api(self) -> bool:
        try:
            headers = {
                "Content-Type": "application/json",
                "X-Goog-Api-Key": self.api_key,
                "X-Goog-FieldMask": "places.displayName"
            }
            data = {
                "textQuery": "test business",
                "maxResultCount": 1
            }
            response = requests.post(self.base_url, json=data, headers=headers, timeout=10)
            
            if response.status_code != 200:
                try:
                    error = response.json().get("error", {})
                    print(f"  [Google Places] API error ({response.status_code}): {error.get('message', 'Unknown error')}")
                except:
                    print(f"  [Google Places] API returned status {response.status_code}")
                return False
            
            return True
        except Exception as e:
            print(f"  [Google Places] Connection error: {str(e)[:60]}")
            return False
    
    def _parse_place(self, place: dict) -> Optional[BusinessLead]:
        try:
            display_name = place.get("displayName", {})
            name = display_name.get("text", "") if isinstance(display_name, dict) else str(display_name)
            
            if not name:
                return None
            
            address = place.get("formattedAddress", "")
            website = place.get("websiteUri", "")
            phone = place.get("nationalPhoneNumber") or place.get("internationalPhoneNumber", "")
            rating = place.get("rating")
            review_count = place.get("userRatingCount", 0)
            types = place.get("types", [])
            
            location = self._extract_location(address)
            sector = self._types_to_sector(types)
            
            if rating:
                sector = f"{sector} (Rating: {rating}/5, {review_count} reviews)"
            
            return BusinessLead(
                company_name=clean_text(name),
                website=website,
                sector=sector[:200],
                email=phone,
                location=location,
                source=self.source_name
            )
        except Exception as e:
            log_verbose(f"Error parsing place: {e}")
            return None
    
    def _extract_location(self, address: str) -> str:
        if not address:
            return self.town
        
        parts = [p.strip() for p in address.split(",")]
        
        for i, part in enumerate(parts):
            if any(town.lower() in part.lower() for town in ["Guildford", "Godalming", "Farnham", "Woking", "Surrey"]):
                relevant = parts[max(0, i-1):i+2]
                return ", ".join(relevant)
        
        if len(parts) >= 2:
            return ", ".join(parts[-3:])
        
        return address
    
    def _types_to_sector(self, types: list) -> str:
        type_mapping = {
            "accounting": "Accountancy",
            "lawyer": "Legal Services",
            "attorney": "Legal Services",
            "employment_agency": "Recruitment",
            "real_estate_agency": "Property Services",
            "insurance_agency": "Insurance",
            "marketing": "Marketing",
            "consultant": "Consulting",
            "architect": "Architecture",
            "engineering": "Engineering",
        }
        
        for place_type in types:
            for key, sector in type_mapping.items():
                if key in place_type.lower():
                    return sector
        
        return "Professional Services"
    
    def _is_professional_service(self, place: dict) -> bool:
        excluded_types = [
            "restaurant", "food", "cafe", "bar", "store", "shop",
            "lodging", "hotel", "gym", "spa", "salon", "beauty",
            "car_repair", "car_dealer", "gas_station", "parking",
            "hospital", "doctor", "dentist", "pharmacy", "health",
            "school", "university", "church", "mosque", "temple",
            "museum", "park", "zoo", "stadium", "movie_theater"
        ]
        
        types = place.get("types", [])
        types_lower = [t.lower() for t in types]
        
        for excluded in excluded_types:
            if any(excluded in t for t in types_lower):
                return False
        
        status = place.get("businessStatus", "")
        if status and status != "OPERATIONAL":
            return False
        
        return True
