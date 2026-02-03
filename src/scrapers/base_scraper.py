from abc import ABC, abstractmethod
from typing import List, Generator
from ..models import BusinessLead

class BaseScraper(ABC):
    def __init__(self, town: str, sector: str = ""):
        self.town = town
        self.sector = sector
        self.source_name = "Unknown"
    
    @abstractmethod
    def scrape(self, max_pages: int = 3) -> Generator[BusinessLead, None, None]:
        pass
    
    def get_source_name(self) -> str:
        return self.source_name
