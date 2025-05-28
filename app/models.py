from pydantic import BaseModel
from typing import Optional, List
from enum import Enum
from datetime import datetime

class ScraperStatus(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class BusinessData(BaseModel):
    business_name: Optional[str] = None
    category: Optional[str] = None
    location: Optional[str] = None
    mobile: Optional[str] = None
    whatsapp: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    source_url: Optional[str] = None
    source_site: Optional[str] = None

class ScraperTask(BaseModel):
    task_id: str
    scraper_name: str
    status: ScraperStatus
    progress: int = 0
    total_records: int = 0
    message: str = ""
    filename: Optional[str] = None
    created_at: str
    completed_at: Optional[str] = None

class ScrapeRequest(BaseModel):
    search_term: Optional[str] = None
    location: Optional[str] = None
    category: Optional[str] = None
    max_pages: int = 5

class ScrapeResponse(BaseModel):
    task_id: str
    message: str
    status: ScraperStatus

# New model for data tracking
class ScrapeStats(BaseModel):
    scraper_name: str
    display_name: str
    total_records: int
    date: str
    timestamp: str
    search_term: Optional[str] = None
    location: Optional[str] = None