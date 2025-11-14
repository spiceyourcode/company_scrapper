import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re
from datetime import datetime
import logging
import cloudscraper # Import for Cloudflare bypass

# --- CONFIGURATION ---
INPUT_FILENAME = "company_list.csv"
OUTPUT_FILENAME = "company_data_filled.xlsx"

# Speed optimization - GOV.UK 
MIN_DELAY_GOV = 1
MAX_DELAY_GOV = 3

# Speed optimization - Endole
MIN_DELAY_ENDOLE = 5
MAX_DELAY_ENDOLE = 10

MAX_RETRIES = 3
ENDOLE_SEARCH_RETRIES = 1 

# Base URLs
SEARCH_URL_ENDOLE = "https://open.endole.co.uk/search/?q="
SEARCH_URL_GOV = "https://find-and-update.company-information.service.gov.uk/search?q="
GOV_BASE_URL = "https://find-and-update.company-information.service.gov.uk"
ENDOLE_DETAIL_BASE_URL = "https://open.endole.co.uk/insight/company"

# --- POSTCODE TO CITY MAPPING (EXPANDED AND REFINED) ---
# This dictionary maps the outward code (postcode prefix/district) or the
# Postcode Area (1-2 letters) to the canonical City from the user's list.

POSTCODE_TO_CITY_MAP = {
    # 1. Specific Prefixes (More granular, usually 3-4 characters)
    'BB5': 'Preston', 'BL9': 'Manchester', 'BR2': 'London', 'BR3': 'London', 
    'BS8': 'Bristol', 'BT92': 'Belfast', 'CR8': 'London', 'CT5': 'Canterbury', 
    'E17': 'London', 'HA1': 'London', 'HP16': 'Oxford', 'HR4': 'Worcester', 
    'IG8': 'London', 'IP1': 'Colchester', 'LE7': 'Leicester', 'LL11': 'Wrexham', 
    'N18': 'London', 'NR4': 'Norwich', 'PE3': 'Peterborough', 'PE30': 'Norwich', 
    'PO18': 'Chichester', 'PR9': 'Liverpool', 'RG19': 'Oxford', 'RM8': 'London', 
    'SW17': 'London', 'WC1B': 'London', 'WC2A': 'London', 'WC2H': 'London',
    
    # 2. General Postcode Area Mappings (1-2 characters)
    # These map the entire area to the most prominent city in your list.
    
    # ENGLAND 
    'AL': 'St albans', 'B': 'Birmingham', 'BA': 'Bath', 'BD': 'Bradford', 
    'BN': 'Brighton and hove', 'BS': 'Bristol', 'CA': 'Carlisle', 'CB': 'Cambridge', 
    'CM': 'Chelmsford', 'CO': 'Colchester', 'CV': 'Coventry', 'DE': 'Derby', 
    'DH': 'Durham', 'DN': 'Doncaster', 'EX': 'Exeter', 'GL': 'Gloucester', 
    'HR': 'Hereford', 'HU': 'Kingston upon hull', 'L': 'Liverpool', 'LA': 'Lancaster', 
    'LE': 'Leicester', 'LN': 'Lincoln', 'LS': 'Leeds', 'M': 'Manchester', 
    'MK': 'Milton keynes', 'NE': 'Newcastle upon tyne', 'NG': 'Nottingham', 
    'NR': 'Norwich', 'OX': 'Oxford', 'PE': 'Peterborough', 'PL': 'Plymouth', 
    'PO': 'Portsmouth', 'PR': 'Preston', 'S': 'Sheffield', 'SO': 'Southampton', 
    'SS': 'Southend on sea', 'ST': 'Stoke on trent', 'SR': 'Sunderland', 'TR': 'Truro', 
    'WF': 'Wakefield', 'WR': 'Worcester', 'WV': 'Wolverhampton', 'YO': 'York',
    
    # LONDON
    'E': 'London', 'EC': 'London', 'N': 'London', 'NW': 'London',
    'SE': 'London', 'SW': 'London', 'W': 'London', 'WC': 'London',
    
    # WALES
    'CF': 'Cardiff', 'LL': 'Wrexham', 'NP': 'Newport', 
    
    # SCOTLAND
    'AB': 'Aberdeen', 'DD': 'Dundee', 'EH': 'Edinburgh', 'FK': 'Stirling', 
    'G': 'Glasgow', 'IV': 'Inverness', 'KY': 'Dunfermline',
    
    # NORTHERN IRELAND (BT covers all NI cities in the list)
    'BT': 'Belfast', 
}

# The user's canonical list of cities is kept for other validation purposes 
UK_CITIES = [
    'Bath', 'Birmingham', 'Bradford', 'Brighton and hove', 'Bristol', 'Cambridge',
    'Canterbury', 'Carlisle', 'Chelmsford', 'Chester', 'Chichester', 'Colchester',
    'Coventry', 'Derby', 'Doncaster', 'Durham', 'Ely', 'Exeter', 'Gloucester',
    'Hereford', 'Kingston upon hull', 'Lancaster', 'Leeds', 'Leicester', 'Lichfield',
    'Lincoln', 'Liverpool', 'London', 'Manchester', 'Milton keynes', 'Newcastle upon tyne',
    'Norwich', 'Nottingham', 'Oxford', 'Peterborough', 'Plymouth', 'Portsmouth',
    'Preston', 'Ripon', 'Salford', 'Salisbury', 'Sheffield', 'Southampton',
    'Southend on sea', 'St albans', 'Stoke on trent', 'Sunderland', 'Truro',
    'Wakefield', 'Wells', 'Westminster', 'Winchester', 'Wolverhampton', 'Worcester',
    'York', 'Aberdeen', 'Dundee', 'Dunfermline', 'Edinburgh', 'Glasgow', 'Inverness',
    'Stirling', 'Bangor', 'Cardiff', 'Newport', 'St asaph', 'St davids', 'Wrexham',
    'Armagh', 'Belfast', 'Derry', 'Lisburn', 'Newry', 'Coleraine', 'Ballymena',
    'Londonderry'
]
UK_CITIES_LOWER = {city.lower() for city in UK_CITIES}

# --- SECTOR MAPPING DEFINITION ---
SECTOR_KEYWORDS_MAP = {
    'Builders and construction': ['construction', 'building', 'erection', 'development projects', 'residential building', 'demolition', 'civil engineering', 'renovation', 'plumbing', 'electricians', 'roofing', 'carpentry', 'foundations', 'framing', 'glazing', 'joinery', 'plastering', 'scaffolding', 'specialised construction'],
    'Real estate': ['real estate', 'property', 'letting agent', 'estate agent', 'residents property management', 'property management', 'buying and selling of real estate'],
    'Architect': ['architect', 'architecture', 'quantity surveying', 'design planning'],
    'Installation of industrial machinery and equipment': ['industrial machinery installation', 'equipment installation', 'electrical wiring installation'],
    'Development of building projects': ['development of building projects', 'house construction', 'domestic buildings'],
    'Maintenance and repair of motor vehicles': ['repair', 'maintenance', 'motor vehicles', 'vehicle recovery', 'handyman services'],
    'Management consultancy': ['management consulting', 'business consulting', 'change management', 'outsourcing', 'risk evaluation', 'strategy consulting', 'operations consulting'],
    'Accountants': ['accounting', 'bookkeeping', 'tax', 'auditing', 'financial audit'],
    'Lawyers and solicitors and barristers': ['solicitor', 'lawyer', 'legal services', 'legal practice', 'barrister'],
    'Human resources services': ['employment placement', 'recruitment', 'staffing', 'human resources', 'talent acquisition'],
    'Administration': ['administration', 'head office', 'office administration', 'corporate office management'],
    'Information technology and services': ['information technology', 'it services', 'cloud computing', 'cybersecurity', 'computer programming', 'software development', 'web development', 'systems integration', 'analytics', 'embedded software', 'ai', 'artificial intelligence', 'robotics'],
    'Telecommunications': ['telecommunications', 'wireless communication', 'network services'],
    'Retail': ['retail sale', 'wholesale', 'store', 'dealership', 'shop', 'boutique', 'supermarket', 'thrift', 'ecommerce', 'e-commerce', 'online shop'],
    'Restaurants': ['restaurant', 'pub', 'takeaway', 'food stand', 'cafe', 'coffee shop', 'bar'],
    'Hotels': ['hotel', 'accommodation', 'holiday rental', 'lodging', 'hospitality'],
    'Manufacturing': ['manufacturing', 'production', 'fabrication', 'making of', 'assembly', 'appliances', 'electronics', 'textile', 'chemical', 'plastic', 'rubber', 'machinery', 'packaging', 'container manufacturing', 'equipment manufacturing'],
    'Advertising': ['advertising', 'content marketing', 'digital marketing', 'public relations', 'branding', 'creative services', 'communications services'],
    'Media': ['media production', 'broadcasting', 'film', 'motion picture', 'video production', 'sound recording', 'publishing', 'internet publishing', 'music production'],
    'Healthcare': ['medical', 'healthcare', 'dental', 'optometry', 'radiology', 'clinical research', 'pharmacy', 'veterinary', 'hospital'],
    'Wellness': ['wellness', 'yoga', 'pilates', 'meditation', 'fitness', 'massage', 'therapy', 'counselling', 'mental health'],
    'Beauty': ['beauty', 'hair', 'barber', 'salon', 'cosmetics', 'skincare', 'spa', 'aesthetics'],
    'Freight and logistics': ['freight', 'logistics', 'courier', 'warehousing', 'storage', 'cargo', 'distribution'],
    'Transport': ['transport', 'taxi', 'bus', 'rail', 'ground passenger', 'sightseeing', 'vehicle transport'],
    'Financial services': ['financial services', 'banking', 'capital markets', 'investment management', 'fintech', 'insurance', 'venture capital', 'private equity'],
    'Business support services': ['business support', 'back office', 'shared services', 'corporate services', 'bpo'],
    'Education': ['education', 'training', 'academy', 'learning', 'school', 'college', 'e-learning', 'professional training', 'language school', 'teaching'],
    'Charities and non profits': ['charity', 'non-profit', 'philanthropy', 'social organization', 'fundraising', 'community development', 'voluntary organization'],
    'Dormant company': ['dormant company', 'inactive company', 'non-trading entity']
}
SECTOR_KEYWORDS_MAP_LOWER = {
    sector: [kw.lower() for kw in keywords]
    for sector, keywords in SECTOR_KEYWORDS_MAP.items()
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- UTILITY FUNCTIONS ---

def map_sic_to_sector(sic_description):
    """Maps a cleaned SIC description to a predefined sector."""
    if not sic_description or sic_description.lower() == 'n/a':
        return 'N/A'
    
    desc_lower = sic_description.lower()
    best_match = 'Multi sector company'
    max_keyword_length = 0

    if 'dormant company' in desc_lower:
        return 'Dormant company'
    
    for sector, keywords in SECTOR_KEYWORDS_MAP_LOWER.items():
        if sector == 'Dormant company': continue
            
        for keyword in keywords:
            if keyword in desc_lower:
                if len(keyword) > max_keyword_length:
                    best_match = sector
                    max_keyword_length = len(keyword)

    return best_match if max_keyword_length > 0 else 'Sector Unknown'

def clean_phone_number(phone_number):
    """Removes spaces and removes leading '0' from a phone number string."""
    if not phone_number or phone_number.lower() == 'n/a':
        return 'N/A'
    
    cleaned = phone_number.replace(' ', '')
    
    # Remove leading '0' if the number is likely a UK local number (starts with 0)
    # and not already starting with a country code like +44
    if cleaned.startswith('0') and not cleaned.startswith('+'):
        return cleaned[1:]
    
    return cleaned

def extract_postcode_prefix(postcode):
    """Extracts the outward code (prefix) from a full UK postcode."""
    if not postcode or postcode.lower() == 'n/a':
        return None
    postcode = str(postcode).strip().upper()
    postcode_compact = postcode.replace(' ', '')
    
    # Check if the compact postcode is long enough to determine an outward code
    if len(postcode_compact) >= 5:
        # Outward code is the part before the last 3 chars (the inward code)
        return postcode_compact[:-3]
    
    # For short/incomplete postcodes, use the whole thing if non-empty
    if len(postcode_compact) > 0:
        return postcode_compact
    
    return None

def get_city_from_postcode_prefix(postcode):
    """
    Looks up the City based on the postcode's outward code (prefix) using the map.
    It checks for a full prefix match first, then falls back to the Postcode Area.
    """
    prefix = extract_postcode_prefix(postcode)
    
    if prefix:
        # 1. Check for exact match of the full prefix (e.g., 'BB5', 'WC1B')
        city = POSTCODE_TO_CITY_MAP.get(prefix)
        if city:
            return city
            
        # 2. Fallback: Check for match of the 1 or 2-letter Postcode Area (e.g., 'BB', 'WC')
        # Postcode Area is typically the part of the prefix before the first digit.
        area_code_match = re.match(r'^([A-Z]{1,2})', prefix)
        if area_code_match:
            area_code = area_code_match.group(1)
            city = POSTCODE_TO_CITY_MAP.get(area_code)
            if city:
                return city
    
    # If the postcode is present but the prefix isn't in our map, return 'N/A'
    return 'N/A'


def fetch_url_with_retry(url):
    """
    Fetches a URL with retry logic and random delays, using cloudscraper for Endole.
    """
    is_endole = 'endole.co.uk' in url

    if is_endole:
        try:
            scraper = cloudscraper.create_scraper() 
        except Exception as e:
            logger.error(f"Failed to initialize cloudscraper: {e}")
            return None
        
        min_delay = MIN_DELAY_ENDOLE
        max_delay = MAX_DELAY_ENDOLE
        retries = ENDOLE_SEARCH_RETRIES if SEARCH_URL_ENDOLE in url else MAX_RETRIES
    else:
        min_delay = MIN_DELAY_GOV
        max_delay = MAX_DELAY_GOV
        retries = MAX_RETRIES
        
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }

    for attempt in range(retries):
        try:
            delay = random.uniform(min_delay, max_delay)
            logger.info(f"Waiting {delay:.2f}s before request (attempt {attempt + 1}/{retries})")
            time.sleep(delay)
            
            if is_endole:
                response = scraper.get(url, timeout=30)
            else:
                response = requests.get(url, headers=headers, timeout=30)
                
            response.raise_for_status()
            logger.info(f"Successfully fetched: {url}")
            return response.text
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error {response.status_code} for {url}: {e}")
            
            if response.status_code == 429:
                wait_time = (attempt + 1) * 30
                logger.warning(f"Rate limited. Waiting {wait_time}s...")
                time.sleep(wait_time)
            elif response.status_code == 403 and SEARCH_URL_ENDOLE in url and ENDOLE_SEARCH_RETRIES == 1:
                return None
            elif attempt == retries - 1:
                return None
                
        except (requests.exceptions.Timeout, cloudscraper.exceptions.CloudflareTimeout, requests.exceptions.RequestException) as e:
            logger.error(f"Request failed for {url}: {e}")
            if attempt == retries - 1:
                return None
    
    return None

def parse_address_components(full_address):
    """
    Parses full address to extract PostCode and Street Address.
    City extraction is intentionally set to 'N/A' here to rely on the PostCode lookup.
    """
    if not full_address or full_address == 'N/A':
        return 'N/A', 'N/A', 'N/A' # street, city, postcode

    postcode = 'N/A'
    street_address = 'N/A'
    
    # 1. Postcode Extraction
    postcode_pattern = r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2})\b'
    postcode_match = re.search(postcode_pattern, full_address, re.IGNORECASE)

    if postcode_match:
        postcode = postcode_match.group(1).strip().upper()
        # Remove postcode and any preceding comma/space from the address string
        address_parts_raw = full_address[:postcode_match.start()].strip(', ')
    else:
        address_parts_raw = full_address

    address_parts = [part.strip() for part in address_parts_raw.split(',') if part.strip()]

    # 2. Heuristic for street_address (Remove UK city if it's the last part)
    street_components_to_keep = []
    
    if address_parts:
        last_part = address_parts[-1]
        
        # If the last part looks like a known city, remove it to clean the street address.
        if last_part.lower() in UK_CITIES_LOWER:
            street_components_to_keep = address_parts[:-1]
        else:
            # Otherwise, keep it as part of the address.
            street_components_to_keep = address_parts

    street_address = ', '.join(street_components_to_keep).strip(', ')
    street_address = street_address if street_address else 'N/A'
    
    # City is deliberately set to 'N/A' here, forcing the caller (scrape_gov_uk) to use the Postcode map.
    return street_address, 'N/A', postcode 

def slugify(text):
    """Converts company name into URL-friendly slug."""
    text = text.lower()
    text = text.replace('&', 'and')
    text = text.replace('  ', ' ')
    text = re.sub(r'[^a-z0-9\s-]', '', text)
    text = re.sub(r'\s+', '-', text).strip('-')
    text = re.sub(r'-+', '-', text) 
    return text

def extract_company_number(text):
    """Extracts company number from various text formats."""
    if not text: return 'N/A'
    patterns = [r'\b([A-Z]{0,2}\d{6,8})\b', r'(?:Company\s+No\.?|Registration\s+No\.?|CRN)[:\s]+([A-Z]{0,2}\d{6,8})', r'^([A-Z]{0,2}\d{6,8})\s*-']
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match: return match.group(1).upper()
    return 'N/A'

# ----------------------------------------------------------------------
# 2. GOV.UK Scraping 
# ----------------------------------------------------------------------

def scrape_gov_uk(company_name):
    """Scrapes Companies House GOV.UK, using postcode prefix to find the City."""
    logger.info(f"Searching GOV.UK for: {company_name}")
    search_query = company_name.replace(" ", "+")
    gov_search_url = SEARCH_URL_GOV + search_query
    
    data = {
        'full_address': 'N/A',
        'address': 'N/A',
        'city': 'N/A',
        'postcode': 'N/A',
        'crn': 'N/A',
        'incorporation_date': 'N/A',
        'status': 'N/A',
        'company_type': 'N/A', 
        'sic': 'N/A',          
        'detail_url_suffix': None 
    }
    
    html_content = fetch_url_with_retry(gov_search_url)
    if not html_content:
        logger.warning(f"Failed to fetch GOV.UK search page for {company_name}")
        return data
    
    soup = BeautifulSoup(html_content, 'html.parser')
    first_result = soup.find('li', class_='type-company')
    
    if not first_result:
        logger.warning(f"No results found on GOV.UK for {company_name}")
        return data
    
    link = first_result.find('a', class_='govuk-link')
    if link and link.get('href'):
        data['detail_url_suffix'] = link.get('href') 
        crn_match = re.search(r'/company/(\w+)', data['detail_url_suffix'])
        if crn_match:
            data['crn'] = crn_match.group(1).upper()
    
    meta_tag = first_result.find('p', class_='meta crumbtrail')
    if meta_tag:
        meta_text = meta_tag.get_text(strip=True)
        crn_from_meta = extract_company_number(meta_text)
        if crn_from_meta != 'N/A':
            data['crn'] = crn_from_meta
        date_match = re.search(r'Incorporated\s+on\s+(\d{1,2}\s+\w+\s+\d{4})', meta_text)
        if date_match:
            data['incorporation_date'] = date_match.group(1)
            
    address_tag = first_result.find('p', class_=None)
    if address_tag:
        full_address = address_tag.get_text(strip=True)
        data['full_address'] = full_address 
        
        street, _, postcode = parse_address_components(full_address) # City returned is N/A
        
        data['address'] = street
        data['postcode'] = postcode
        data['status'] = 'Active' 
        
        # NEW LOGIC: Use Postcode to determine City (overrides the N/A from parsing)
        data['city'] = get_city_from_postcode_prefix(postcode)
    
    # --- STEP 2: Scrape Detail Page ---
    if data['detail_url_suffix'] and data['crn'] != 'N/A':
        detail_url = GOV_BASE_URL + data['detail_url_suffix']
        logger.info(f"Fetching GOV.UK detail page: {detail_url}")
        detail_html_content = fetch_url_with_retry(detail_url)
        
        if detail_html_content:
            detail_soup = BeautifulSoup(detail_html_content, 'html.parser')
            
            status_dd = detail_soup.find('dd', id='company-status', class_='text data')
            if status_dd: data['status'] = status_dd.get_text(strip=True)
            
            type_dd = detail_soup.find('dd', id='company-type', class_='text data')
            if type_dd: data['company_type'] = type_dd.get_text(strip=True)
                
            sic_heading = detail_soup.find('h2', id='sic-title')
            if sic_heading:
                sic_ul = sic_heading.find_next_sibling('ul')
                if sic_ul:
                    sic_span = sic_ul.find('span', id=lambda x: x and x.startswith('sic'))
                    if sic_span: data['sic'] = sic_span.get_text(strip=True)

        else:
            logger.warning(f"Failed to fetch GOV.UK detail page for CRN: {data['crn']}")

    data.pop('detail_url_suffix', None) 
    logger.info(f"GOV.UK extraction completed for {company_name}")
    return data

# ----------------------------------------------------------------------
# 3. Endole Scraping
# ----------------------------------------------------------------------

def scrape_endole_search(company_name):
    """Scrapes Endole search page for company number and basic info using cloudscraper."""
    logger.info(f"Searching Endole for: {company_name}")
    search_query = company_name.replace(" ", "+")
    endole_search_url = SEARCH_URL_ENDOLE + search_query
    
    data = {'crn': 'N/A', 'status': 'N/A', 'website': 'N/A'}
    html_content = fetch_url_with_retry(endole_search_url)
    
    if not html_content:
        logger.warning(f"Failed to fetch Endole search for {company_name}")
        return data
    
    soup = BeautifulSoup(html_content, 'html.parser')
    company_link = soup.find('a', class_='_company-name')
    if not company_link: return data
    
    result_container = company_link.find_parent('div')
    if result_container:
        info_grid = result_container.find('div', class_='_company-info grid-resp')
        
        if info_grid:
            info_items = info_grid.find_all('div', recursive=False)
            
            for i in range(0, len(info_items), 2):
                if i + 1 < len(info_items):
                    label = info_items[i].get_text(strip=True)
                    value_div = info_items[i + 1]
                    value = value_div.get_text(strip=True)
                    
                    if 'Company No' in label: data['crn'] = value
                    elif 'Status' in label:
                        status_elem = value_div.find('div', class_='status')
                        if status_elem: data['status'] = status_elem.get_text(strip=True)
                    elif 'Website' in label:
                        website_link = value_div.find('a')
                        if website_link: data['website'] = website_link.get('href', 'N/A')
                        elif value: data['website'] = value
    
    logger.info(f"Endole search extraction completed for {company_name}")
    return data

def scrape_endole_detail(crn, company_name):
    """Scrapes Endole detail page for contact information, cleans the telephone number."""
    if not crn or crn == 'N/A':
        logger.warning(f"No CRN provided for Endole detail scrape: {company_name}")
        return {'telephone': 'N/A', 'email': 'N/A', 'website': 'N/A'}
    
    company_slug = slugify(company_name)
    detail_url = f"{ENDOLE_DETAIL_BASE_URL}/{crn}-{company_slug}"
    
    logger.info(f"Fetching Endole detail page: {detail_url}")
    
    data = {'telephone': 'N/A', 'email': 'N/A', 'website': 'N/A'}
    html_content = fetch_url_with_retry(detail_url)
    
    if not html_content:
        logger.warning(f"Failed to fetch Endole detail page for {company_name}")
        return data
    
    soup = BeautifulSoup(html_content, 'html.parser')
    info_items = soup.find_all('div', class_='info-item')
    
    for item in info_items:
        title_div = item.find('div', class_='_title')
        stat_div = item.find('div', class_='_stat')
        
        if title_div and stat_div:
            title = title_div.get_text(strip=True)
            value = stat_div.get_text(strip=True)
            
            if 'Telephone' in title and value:
                data['telephone'] = clean_phone_number(value)
            elif 'Email' in title and value:
                data['email'] = value
            elif 'Website' in title:
                website_link = stat_div.find('a')
                if website_link: data['website'] = website_link.get('href', value)
                elif value: data['website'] = value
    
    logger.info(f"Endole detail extraction completed for {company_name}")
    return data

# ----------------------------------------------------------------------
# 4. Main Processing Function 
# ----------------------------------------------------------------------

def process_company(company_name):
    """Main function to process a single company by scraping multiple sources."""
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing: {company_name}")
    logger.info(f"{'='*60}")
    
    result = {
        'Business Name': company_name,
        'Full Address': 'N/A',
        'Adress': 'N/A',
        'City': 'N/A',
        'PostCode': 'N/A',
        'Company Type': 'N/A',
        'SIC': 'N/A',
        'Telephone': 'N/A',
        'Website': 'N/A',
        'Email': 'N/A',
        'Short Description': '', 
        'Description': '',
        'Sector': 'N/A', 
        'Sector Status': '',
        'Compnay Facebook': '',
        'LinkedIn': '',
        'Instagram': '',
        'Youtube': '',
        'Company Status': 'N/A',
        'Researcher': '',
        'Date': datetime.now().strftime('%Y-%m-%d'),
        'Data Cleaner Status': '',
        'Notes': '',
        'Row Fixed': '',
        'QA Status': '',
        'QA Name': '',
        'QA Date': '',
        'Reason for Needs Fixing / Invalid': '',
        'QA Notes': '',
        'QAs Review Status': '',
        'TL Notes': '',
        'CRN': 'N/A',
        'Source': ''
    }
    
    try:
        # Phase 1: GOV.UK (primary source for address, CRN, Company Type, SIC)
        gov_data = scrape_gov_uk(company_name)
        if gov_data['crn'] != 'N/A':
            result['CRN'] = gov_data['crn']
            result['Full Address'] = gov_data['full_address']
            result['Adress'] = gov_data['address']
            result['City'] = gov_data['city']
            result['PostCode'] = gov_data['postcode']
            result['Company Status'] = gov_data['status']
            result['Company Type'] = gov_data['company_type']
            result['SIC'] = gov_data['sic']
            result['Source'] = 'GOV.UK'
            
            sic_code_full = gov_data['sic']
            short_description = ''
            if sic_code_full and ' - ' in sic_code_full:
                short_description = sic_code_full.split(' - ', 1)[-1].strip()
                if short_description:
                    result['Short Description'] = short_description
            
            if short_description:
                result['Sector'] = map_sic_to_sector(short_description)
        
        # Phase 2 & 3: Endole
        endole_search_data = scrape_endole_search(company_name)
        
        if result['CRN'] == 'N/A' and endole_search_data['crn'] != 'N/A':
            result['CRN'] = endole_search_data['crn']
        
        if endole_search_data['status'] != 'N/A':
            result['Company Status'] = endole_search_data['status']
        
        if endole_search_data['website'] != 'N/A' and result['Website'] == 'N/A':
            result['Website'] = endole_search_data['website']
        
        if result['Source']:
            result['Source'] += ' + Endole'
        else:
            result['Source'] = 'Endole'
        
        if result['CRN'] != 'N/A':
            endole_detail_data = scrape_endole_detail(result['CRN'], company_name)
            
            if endole_detail_data['telephone'] != 'N/A':
                result['Telephone'] = endole_detail_data['telephone'] 
            if endole_detail_data['email'] != 'N/A':
                result['Email'] = endole_detail_data['email']
            if endole_detail_data['website'] != 'N/A' and result['Website'] == 'N/A':
                result['Website'] = endole_detail_data['website']
        
        logger.info(f"✓ Successfully processed: {company_name}")
        
    except Exception as e:
        logger.error(f"Error processing {company_name}: {str(e)}", exc_info=True)
        result['Notes'] = f"Error: {str(e)}"
    
    return result

# ----------------------------------------------------------------------
# 5. Main Execution 
# ----------------------------------------------------------------------

def main():
    """Main execution function."""
    logger.info("="*60)
    logger.info("Company Data Scraper - Starting")
    logger.info("="*60)
    
    try:
        if INPUT_FILENAME.endswith('.xlsx'):
            df = pd.read_excel(INPUT_FILENAME)
        elif INPUT_FILENAME.endswith('.csv'):
            df = pd.read_csv(INPUT_FILENAME)
        else:
            logger.error("Input file must be .csv or .xlsx")
            return
        
        if 'Business Name' not in df.columns:
            logger.error("Input file must contain 'Business Name' column")
            return
        
        logger.info(f"Loaded {len(df)} companies from {INPUT_FILENAME}")
        
    except FileNotFoundError:
        logger.error(f"File not found: {INPUT_FILENAME}")
        return
    except Exception as e:
        logger.error(f"Error reading input file: {e}")
        return
    
    results = []
    total = len(df)
    
    for idx, row in df.iterrows():
        company_name = row.get('Business Name', '')
        
        if pd.isna(company_name) or not company_name.strip():
            logger.warning(f"Skipping row {idx + 1}: Empty company name")
            continue
        
        logger.info(f"\nProgress: {idx + 1}/{total}")
        result = process_company(str(company_name).strip())
        results.append(result)
        
        if (idx + 1) % 10 == 0:
            # Save partial results periodically
            temp_df = pd.DataFrame(results)
            temp_df.to_excel('temp_' + OUTPUT_FILENAME, index=False)
            logger.info(f"Progress saved to temp_{OUTPUT_FILENAME}")
    
    if results:
        output_df = pd.DataFrame(results)
        
        # Merge new columns with original columns order
        original_cols = df.columns.tolist()
        new_cols = [col for col in output_df.columns if col not in original_cols]
        final_cols = original_cols + new_cols
        output_df = output_df[[col for col in final_cols if col in output_df.columns]]
        
        output_df.to_excel(OUTPUT_FILENAME, index=False)
        logger.info(f"\n{'='*60}")
        logger.info(f"✓ SUCCESS: Data saved to {OUTPUT_FILENAME}")
        logger.info(f"Processed {len(results)} companies")
        logger.info(f"{'='*60}")
    else:
        logger.warning("No results to save")

if __name__ == "__main__":
    main()