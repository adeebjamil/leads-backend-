import asyncio
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from typing import List, Dict, Callable
from ..models import ScrapeRequest, BusinessData
from ..utils import clean_text, clean_phone, export_to_csv, export_to_excel, create_timestamp
import time
import re
import random

# NEW: Email extraction from websites
def extract_email_from_website(website_url):
    """Extract email from business website"""
    try:
        if not website_url or not website_url.startswith('http'):
            return ""
            
        print(f"[DEBUG] 📧 Checking website for email: {website_url}")
        
        response = requests.get(website_url, timeout=8, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        if response.status_code == 200:
            # Look for email patterns
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            emails = re.findall(email_pattern, response.text, re.IGNORECASE)
            
            # Filter business emails (exclude social media, generic emails)
            business_emails = []
            for email in emails:
                email_lower = email.lower()
                if not any(domain in email_lower for domain in [
                    'google.com', 'facebook.com', 'instagram.com', 'twitter.com', 
                    'linkedin.com', 'youtube.com', 'tiktok.com', 'pinterest.com',
                    'example.com', 'test.com', 'mailto.com', 'email.com'
                ]):
                    # Prefer info@, contact@, admin@ emails
                    if any(prefix in email_lower for prefix in ['info@', 'contact@', 'admin@', 'sales@']):
                        business_emails.insert(0, email)  # Priority emails first
                    else:
                        business_emails.append(email)
            
            if business_emails:
                found_email = business_emails[0]
                print(f"[DEBUG] ✅ Found email: {found_email}")
                return found_email
            else:
                print(f"[DEBUG] ❌ No business emails found on website")
                
    except Exception as e:
        print(f"[DEBUG] Email extraction failed for {website_url}: {e}")
    
    return ""

def generate_common_emails(website_url, business_name):
    """Generate common email patterns for business"""
    try:
        if not website_url:
            return ""
            
        # Extract domain from website
        domain = website_url.replace('http://', '').replace('https://', '').replace('www.', '').split('/')[0]
        
        # Common email prefixes
        common_patterns = [
            f"info@{domain}",
            f"contact@{domain}",
            f"sales@{domain}",
            f"admin@{domain}",
            f"hello@{domain}",
            f"support@{domain}"
        ]
        
        # Business name based email
        if business_name:
            clean_name = re.sub(r'[^\w]', '', business_name.lower())[:10]
            if clean_name:
                common_patterns.append(f"{clean_name}@{domain}")
        
        print(f"[DEBUG] 💡 Suggested emails for {domain}: {common_patterns[:3]}")
        return common_patterns[0]  # Return most likely email
        
    except:
        return ""

async def scrape_googlemaps(request: ScrapeRequest, progress_callback: Callable, task_id: str) -> Dict:
    """Google Maps Business Scraper - ENHANCED CLOUD VERSION"""
    
    results = []
    seen_businesses = set()
    
    progress_callback(task_id, 10, "Setting up Google Maps scraper...")
    print(f"[DEBUG] Starting Google Maps scraping for task {task_id}")
    
    # ENHANCED Chrome options with better anti-detection
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # Use new headless mode
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    
    # CRITICAL: Better anti-detection
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Memory optimization for cloud
    chrome_options.add_argument("--memory-pressure-off")
    chrome_options.add_argument("--max_old_space_size=4096")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument("--no-zygote")
    
    # Window settings
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--start-maximized")
    
    # Try to find Chrome binary
    import shutil
    chrome_paths = [
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable", 
        "/usr/bin/chromium-browser",
        "/usr/bin/chromium",
        "/opt/google/chrome/chrome"
    ]
    
    chrome_binary = None
    for path in chrome_paths:
        if shutil.which(path):
            chrome_binary = path
            break
    
    if chrome_binary:
        chrome_options.binary_location = chrome_binary
        print(f"[DEBUG] ✅ Found Chrome at: {chrome_binary}")
    else:
        print("[ERROR] ❌ Chrome binary not found")
        progress_callback(task_id, 0, "Error: Chrome not available")
        return {
            'filename': f"failed_{task_id}",
            'total_records': 0,
            'csv_path': None,
            'excel_path': None,
            'status': 'failed',
            'error': 'Chrome binary not found'
        }
    
    # Performance preferences
    prefs = {
        "profile.default_content_setting_values": {
            "images": 2,  # Block images
            "plugins": 2, 
            "popups": 2, 
            "geolocation": 1,
            "notifications": 2, 
            "media_stream": 2,
        }
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    driver = None
    
    try:
        progress_callback(task_id, 15, "Initializing browser...")
        print("[DEBUG] 🚀 Initializing Chrome driver...")
        
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            print(f"[DEBUG] ChromeDriverManager failed: {e}")
            driver = webdriver.Chrome(options=chrome_options)
        
        # Enhanced anti-detection
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
        driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")
        
        progress_callback(task_id, 25, "Loading Google Maps...")
        print("[DEBUG] 🗺️ Loading Google Maps...")
        
        # Navigate to Google Maps
        driver.get("https://www.google.com/maps")
        time.sleep(8)  # Longer wait
        
        # ENHANCED: Check if page loaded properly
        page_title = driver.title
        print(f"[DEBUG] Page title: {page_title}")
        
        if "Google Maps" not in page_title:
            print("[ERROR] ❌ Google Maps did not load properly")
            progress_callback(task_id, 30, "Error: Could not access Google Maps")
            raise Exception("Google Maps access blocked")
        
        # Accept cookies
        try:
            accept_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Accept') or contains(text(), 'I agree') or contains(text(), 'Acepto') or contains(text(), 'Akzeptieren')]")
            if accept_buttons:
                accept_buttons[0].click()
                time.sleep(3)
                print("[DEBUG] ✅ Cookies accepted")
        except Exception as e:
            print(f"[DEBUG] Cookie handling: {e}")
        
        progress_callback(task_id, 35, "Performing search...")
        
        # Perform search with better error handling
        search_success = perform_enhanced_search(driver, request, progress_callback, task_id)
        
        if search_success:
            progress_callback(task_id, 50, "Extracting business data...")
            results = extract_google_maps_results_enhanced(driver, progress_callback, task_id, seen_businesses, request.max_pages or 3)
            
            print(f"[DEBUG] 📊 Final results: {len(results)} businesses found")
            
        else:
            print("[ERROR] ❌ Search completely failed")
            progress_callback(task_id, 45, "Search failed - no results found")
            
    except Exception as e:
        print(f"[ERROR] ❌ Scraping failed: {e}")
        import traceback
        traceback.print_exc()
        progress_callback(task_id, 80, f"Error: {str(e)}")
        
        return {
            'filename': f"failed_{task_id}",
            'total_records': 0,
            'csv_path': None,
            'excel_path': None,
            'status': 'failed',
            'error': str(e)
        }
        
    finally:
        if driver:
            try:
                driver.quit()
                print("[DEBUG] 🔒 Browser closed")
            except:
                pass
    
    # Export results
    progress_callback(task_id, 90, "Exporting results...")
    timestamp = create_timestamp()
    filename = f"googlemaps_uae_{timestamp}"
    
    if results and len(results) > 0:
        csv_path = export_to_csv([r.__dict__ for r in results], filename)
        excel_path = export_to_excel([r.__dict__ for r in results], filename)
        print(f"[DEBUG] ✅ Exported {len(results)} records")
    else:
        csv_path = None
        excel_path = None
        print("[DEBUG] ⚠️ No results to export")
    
    progress_callback(task_id, 100, f"Completed! Found {len(results)} businesses")
    
    return {
        'filename': filename,
        'total_records': len(results),
        'csv_path': csv_path,
        'excel_path': excel_path,
        'status': 'completed' if len(results) > 0 else 'completed_no_results'
    }

def perform_enhanced_search(driver, request, progress_callback, task_id):
    """Enhanced search with better error handling"""
    try:
        progress_callback(task_id, 40, "Finding search box...")
        
        # Multiple search box selectors
        search_selectors = [
            "#searchboxinput",
            "input[aria-label*='Search']",
            "input[placeholder*='Search']",
            "input[data-value='Search']"
        ]
        
        search_box = None
        for selector in search_selectors:
            try:
                search_box = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                print(f"[DEBUG] ✅ Found search box with selector: {selector}")
                break
            except:
                continue
        
        if not search_box:
            print("[ERROR] ❌ Could not find search box")
            return False
        
        # Create search query
        search_term = request.search_term or "restaurants"
        location = request.location or "Dubai, UAE"
        search_query = f"{search_term} in {location}"
        
        print(f"[DEBUG] 🔍 Searching for: {search_query}")
        progress_callback(task_id, 42, f"Searching: {search_query}")
        
        # Perform search
        search_box.clear()
        time.sleep(1)
        search_box.send_keys(search_query)
        time.sleep(2)
        search_box.send_keys(Keys.RETURN)
        
        # Wait for results
        progress_callback(task_id, 45, "Waiting for results...")
        time.sleep(10)  # Longer wait for cloud
        
        # Check if results loaded
        result_indicators = [
            "[role='main']",
            ".Nv2PK",
            "[data-result-index]",
            ".hfpxzc"
        ]
        
        results_found = False
        for indicator in result_indicators:
            try:
                results_container = driver.find_elements(By.CSS_SELECTOR, indicator)
                if results_container and len(results_container) > 0:
                    print(f"[DEBUG] ✅ Results found with selector: {indicator}")
                    results_found = True
                    break
            except:
                continue
        
        if not results_found:
            print("[ERROR] ❌ No search results found")
            # Take screenshot for debugging
            try:
                driver.save_screenshot("/tmp/failed_search.png")
                print("[DEBUG] 📸 Screenshot saved for debugging")
            except:
                pass
            return False
        
        print("[DEBUG] ✅ Search completed successfully")
        return True
        
    except Exception as e:
        print(f"[ERROR] ❌ Search failed: {e}")
        return False

def extract_google_maps_results_enhanced(driver, progress_callback, task_id, seen_businesses, max_pages):
    """Enhanced extraction with better error handling"""
    results = []
    
    try:
        progress_callback(task_id, 55, "Loading business listings...")
        time.sleep(5)
        
        # Enhanced element selectors
        business_selectors = [
            "[data-result-index]",
            ".hfpxzc",
            ".Nv2PK .qBF1Pd",
            "[jsaction*='mouseover']",
            ".bfdHYd"
        ]
        
        all_elements = []
        for selector in business_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                all_elements.extend(elements)
                print(f"[DEBUG] Found {len(elements)} elements with selector: {selector}")
            except Exception as e:
                print(f"[DEBUG] Selector failed {selector}: {e}")
                continue
        
        print(f"[DEBUG] 📋 Total elements found: {len(all_elements)}")
        
        if len(all_elements) == 0:
            print("[ERROR] ❌ No business elements found")
            # Take debug screenshot
            try:
                driver.save_screenshot("/tmp/no_results.png")
                current_url = driver.current_url
                page_source_snippet = driver.page_source[:500]
                print(f"[DEBUG] Current URL: {current_url}")
                print(f"[DEBUG] Page source snippet: {page_source_snippet}")
            except:
                pass
            return results
        
        # Filter unique elements
        unique_elements = filter_unique_map_elements(all_elements)
        print(f"[DEBUG] 🎯 Unique business elements: {len(unique_elements)}")
        
        # Process each business
        for i, element in enumerate(unique_elements[:20]):  # Limit to 20 for cloud
            try:
                progress_val = 60 + (i * 25) // len(unique_elements)
                progress_callback(task_id, progress_val, f"Processing business {i+1}/{len(unique_elements)}")
                
                business_data = extract_google_maps_business_data(element, driver)
                
                if business_data and business_data.business_name:
                    business_key = create_business_key(business_data)
                    
                    if business_key not in seen_businesses:
                        seen_businesses.add(business_key)
                        results.append(business_data)
                        print(f"[DEBUG] ✅ #{len(results)}: {business_data.business_name}")
                        print(f"[DEBUG]    📞 {business_data.mobile} | 🌐 {business_data.website}")
                    else:
                        print(f"[DEBUG] ❌ DUPLICATE: {business_data.business_name}")
                
            except Exception as e:
                print(f"[DEBUG] Failed to process business {i+1}: {e}")
                continue
                
    except Exception as e:
        print(f"[ERROR] ❌ Results extraction failed: {e}")
    
    print(f"[DEBUG] 🎉 Extraction completed: {len(results)} unique businesses")
    return results

def scroll_results_panel(driver):
    """Scroll the results panel to load more businesses"""
    try:
        # Find the scrollable results panel
        results_panel = driver.find_element(By.CSS_SELECTOR, "[role='main']")
        
        # Scroll down multiple times
        for _ in range(3):
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", results_panel)
            time.sleep(1)
            
    except Exception as e:
        print(f"[DEBUG] Scrolling failed: {e}")

def filter_unique_map_elements(elements):
    """Filter elements to get unique business listings"""
    unique_elements = []
    seen_texts = set()
    
    for element in elements:
        try:
            element_text = element.text.strip()
            
            # Basic filtering
            if not element_text or len(element_text) < 10:
                continue
                
            # Check for business-like content
            if not any(indicator in element_text.lower() for indicator in 
                      ['open', 'closed', '★', 'rating', 'reviews', 'phone', 'website', 'directions']):
                continue
            
            # Create signature to avoid duplicates
            text_signature = element_text[:50].lower()
            if text_signature not in seen_texts:
                seen_texts.add(text_signature)
                unique_elements.append(element)
                
        except Exception as e:
            continue
            
    return unique_elements

def extract_google_maps_business_data(element, driver):
    """Extract business data from Google Maps listing - ENHANCED WITH EMAIL EXTRACTION"""
    try:
        # Click on the business to get details
        try:
            element.click()
            time.sleep(2)
        except:
            pass
        
        # Get business name
        business_name = extract_maps_business_name(driver)
        if not business_name:
            return None
            
        # Get other details
        category = extract_maps_category(driver)
        rating = extract_maps_rating(driver)
        address = extract_maps_address(driver)
        phone = extract_maps_phone(driver)
        website = extract_maps_website(driver)
        hours = extract_maps_hours(driver)
        
        # ENHANCED: Extract email from website
        email = ""
        if website:
            email = extract_email_from_website(website)
            
        # If no email found from website, try generating common patterns
        if not email and website:
            email = generate_common_emails(website, business_name)
        
        # Combine rating and category for category field
        if rating:
            category = f"{category} (★{rating})" if category else f"Business (★{rating})"
        
        return BusinessData(
            business_name=business_name,
            category=category,
            location=address,
            mobile=phone,
            whatsapp=phone,  # Assume mobile can be WhatsApp
            email=email,  # NOW EXTRACTS EMAILS FROM WEBSITES!
            website=website,
            source_url=driver.current_url,
            source_site="Google Maps"
        )
        
    except Exception as e:
        print(f"[DEBUG] Business data extraction failed: {e}")
        return None

def extract_maps_business_name(driver):
    """Extract business name from Google Maps"""
    selectors = [
        "h1[data-attrid='title']",
        "h1.DUwDvf",
        "[data-attrid='title'] h1",
        ".SPZz6b h1",
        "h1"
    ]
    
    for selector in selectors:
        try:
            element = driver.find_element(By.CSS_SELECTOR, selector)
            name = clean_text(element.text)
            if name and len(name) > 3:
                return name
        except:
            continue
    
    return None

def extract_maps_category(driver):
    """Extract business category from Google Maps"""
    selectors = [
        "[data-attrid='kc:/collection/knowledge_panels/local_searchresults:business_type']",
        ".YhemCb",
        ".DkEaL",
        "button[jsaction*='category']"
    ]
    
    for selector in selectors:
        try:
            element = driver.find_element(By.CSS_SELECTOR, selector)
            category = clean_text(element.text)
            if category and len(category) > 2:
                return category
        except:
            continue
    
    return "Business"

def extract_maps_rating(driver):
    """Extract rating from Google Maps"""
    selectors = [
        "[data-attrid='kc:/collection/knowledge_panels/local_searchresults:star_score']",
        ".MW4etd",
        "span.yi40Hd.YrbPuc"
    ]
    
    for selector in selectors:
        try:
            element = driver.find_element(By.CSS_SELECTOR, selector)
            rating_text = element.text.strip()
            # Extract number from rating text
            rating_match = re.search(r'(\d+\.?\d*)', rating_text)
            if rating_match:
                return rating_match.group(1)
        except:
            continue
    
    return None

def extract_maps_address(driver):
    """Extract address from Google Maps"""
    selectors = [
        "[data-attrid='kc:/collection/knowledge_panels/local_searchresults:address']",
        ".AaVjTc .rogA2c",
        "[data-item-id='address']",
        ".Io6YTe"
    ]
    
    for selector in selectors:
        try:
            element = driver.find_element(By.CSS_SELECTOR, selector)
            address = clean_text(element.text)
            if address and len(address) > 5:
                return address
        except:
            continue
    
    return "UAE"

def extract_maps_phone(driver):
    """Extract phone number from Google Maps"""
    selectors = [
        "[data-attrid='kc:/collection/knowledge_panels/local_searchresults:phone']",
        "span[data-attrid*='phone']",
        ".AaVjTc .rogA2c[data-item-id='phone']",
        "a[href^='tel:']"
    ]
    
    for selector in selectors:
        try:
            element = driver.find_element(By.CSS_SELECTOR, selector)
            if selector == "a[href^='tel:']":
                phone = element.get_attribute("href").replace("tel:", "")
            else:
                phone = element.text
            
            cleaned_phone = clean_phone(phone)
            if cleaned_phone:
                return cleaned_phone
        except:
            continue
    
    return ""

def extract_maps_website(driver):
    """Extract website from Google Maps"""
    selectors = [
        "[data-attrid='kc:/collection/knowledge_panels/local_searchresults:website']",
        "a[data-attrid*='website']",
        ".AaVjTc .rogA2c[data-item-id='authority']",
        "a[href^='http']:not([href*='google.com']):not([href*='maps.google'])"
    ]
    
    for selector in selectors:
        try:
            element = driver.find_element(By.CSS_SELECTOR, selector)
            website = element.get_attribute("href") if element.tag_name == "a" else element.text
            
            if website and website.startswith("http") and "google" not in website:
                return website
        except:
            continue
    
    return ""

def extract_maps_hours(driver):
    """Extract opening hours from Google Maps"""
    try:
        hours_button = driver.find_element(By.CSS_SELECTOR, "[data-attrid*='hours'] button, .OqCZI button")
        return clean_text(hours_button.text)
    except:
        return ""

def create_business_key(business_data):
    """Create unique key for duplicate detection"""
    # Normalize business name
    name_key = re.sub(r'[^\w\s]', '', business_data.business_name.lower().strip())
    name_key = re.sub(r'\s+', ' ', name_key)
    
    # Normalize phone
    phone_key = re.sub(r'[^\d]', '', business_data.mobile or '')
    
    # Create composite key
    if phone_key and len(phone_key) > 7:
        return f"{name_key}_{phone_key}"
    else:
        return name_key