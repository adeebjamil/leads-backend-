"""
UAE Business Directory Scraper - Backend Application

This package contains the main FastAPI application and all scraping modules
for extracting business data from various UAE directory websites.

Author: UAE Business Scraper Team
Version: 1.0.0
"""

__version__ = "1.0.0"
__author__ = "UAE Business Scraper Team"
__description__ = "Backend API for scraping UAE business directories"

# Import main components for easy access
from .models import (
    ScraperStatus,
    BusinessData,
    ScraperTask,
    ScrapeRequest,
    ScrapeResponse
)

from .scraper_manager import scraper_manager
from .utils import (
    create_timestamp,
    export_to_csv,
    export_to_excel,
    clean_text,
    clean_phone,
    get_headers
)

__all__ = [
    "ScraperStatus",
    "BusinessData", 
    "ScraperTask",
    "ScrapeRequest",
    "ScrapeResponse",
    "scraper_manager",
    "create_timestamp",
    "export_to_csv",
    "export_to_excel",
    "clean_text",
    "clean_phone",
    "get_headers"
]