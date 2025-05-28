"""
UAE Business Directory Scrapers Package - Google Maps Edition

High-quality verified business leads from Google Maps with:
- Business Name
- Category & Rating
- Location/Address  
- Phone Number
- Website
- Verified Data Quality

Author: UAE Business Scraper Team
Version: 2.0.0 - Google Maps Edition
"""

__version__ = "2.0.0"
__author__ = "UAE Business Scraper Team"

# Import Google Maps scraper
from . import googlemaps

# Define available scrapers
AVAILABLE_SCRAPERS = {
    'googlemaps': {
        'name': 'Google Maps UAE',
        'module': googlemaps,
        'function': googlemaps.scrape_googlemaps,
        'description': 'High-quality verified business leads from Google Maps',
        'method': 'Selenium + Google Maps API',
        'url': 'https://maps.google.com'
    }
}

def get_scraper_info(scraper_name: str = None):
    """Get information about available scrapers"""
    if scraper_name:
        return AVAILABLE_SCRAPERS.get(scraper_name)
    return AVAILABLE_SCRAPERS

def get_scraper_function(scraper_name: str):
    """Get scraper function by name"""
    scraper_info = AVAILABLE_SCRAPERS.get(scraper_name)
    if scraper_info:
        return scraper_info['function']
    raise ValueError(f"Unknown scraper: {scraper_name}")

def list_scrapers():
    """List all available scrapers"""
    return list(AVAILABLE_SCRAPERS.keys())

__all__ = [
    'googlemaps',
    'AVAILABLE_SCRAPERS',
    'get_scraper_info',
    'get_scraper_function', 
    'list_scrapers'
]