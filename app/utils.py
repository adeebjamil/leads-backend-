import pandas as pd
import os
from datetime import datetime
import time
import random
from typing import List, Dict

def create_timestamp():
    """Create a timestamp string for file naming"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def export_to_csv(data: List[Dict], filename: str, output_dir: str = "exports") -> str:
    """Export data to CSV file"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    df = pd.DataFrame(data)
    filepath = os.path.join(output_dir, f"{filename}.csv")
    df.to_csv(filepath, index=False, encoding='utf-8-sig')
    return filepath

def export_to_excel(data: List[Dict], filename: str, output_dir: str = "exports") -> str:
    """Export data to Excel file"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    df = pd.DataFrame(data)
    filepath = os.path.join(output_dir, f"{filename}.xlsx")
    df.to_excel(filepath, index=False, engine='openpyxl')
    return filepath

def clean_text(text: str) -> str:
    """Clean and normalize text data"""
    if not text:
        return ""
    return text.strip().replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')

def clean_phone(phone: str) -> str:
    """Clean and format phone numbers"""
    if not phone:
        return ""
    # Remove common phone formatting characters
    cleaned = phone.replace('(', '').replace(')', '').replace('-', '').replace(' ', '').replace('+', '')
    return cleaned.strip()

def random_delay(min_seconds: float = 1.0, max_seconds: float = 3.0):
    """Add random delay to avoid being blocked"""
    delay = random.uniform(min_seconds, max_seconds)
    time.sleep(delay)

def get_user_agents():
    """Return list of user agents for rotation"""
    return [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0'
    ]

def get_headers():
    """Get random headers for requests"""
    user_agents = get_user_agents()
    return {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }