"""
Module for matching businesses against Colorado Secretary of State business entities database.

Downloads and caches the Colorado business entities CSV and performs fuzzy matching
to find registered agent names for businesses found via Google Places API.
"""
import os
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pandas as pd
import requests
from rapidfuzz import fuzz, process

from .config import DATA_DIR

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Colorado Business Entities Data Source
COLORADO_DATA_URL = "https://data.colorado.gov/api/views/4ykn-tg5h/rows.csv?accessType=DOWNLOAD"
COLORADO_CSV_FILENAME = "colorado_business_entities.csv"
CACHE_MAX_AGE_DAYS = 7

# Fuzzy matching settings
FUZZY_MATCH_THRESHOLD_NAME = 80  # Minimum score for name matching
FUZZY_MATCH_THRESHOLD_ADDRESS = 70  # Lower threshold for address (since we filter by city)


def get_cache_path() -> Path:
    """Get the path to the cached Colorado CSV file."""
    data_dir = Path(DATA_DIR)
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / COLORADO_CSV_FILENAME


def is_cache_valid() -> bool:
    """
    Check if the cached Colorado CSV exists and is less than CACHE_MAX_AGE_DAYS old.
    
    Returns:
        True if cache is valid, False if needs refresh
    """
    cache_path = get_cache_path()
    
    if not cache_path.exists():
        logger.info("Colorado data cache not found")
        return False
    
    # Check file age
    file_mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
    age = datetime.now() - file_mtime
    
    if age > timedelta(days=CACHE_MAX_AGE_DAYS):
        logger.info(f"Colorado data cache is {age.days} days old, needs refresh")
        return False
    
    logger.info(f"Colorado data cache is valid ({age.days} days old)")
    return True


def download_colorado_data(progress_callback: callable = None) -> bool:
    """
    Download the Colorado business entities CSV from the state's open data portal.
    
    Args:
        progress_callback: Optional callback(bytes_downloaded, total_bytes) for progress
    
    Returns:
        True if download successful, False otherwise
    """
    cache_path = get_cache_path()
    
    logger.info("Downloading Colorado business entities data...")
    logger.info(f"Source: {COLORADO_DATA_URL}")
    
    try:
        response = requests.get(COLORADO_DATA_URL, stream=True, timeout=300)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(cache_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if progress_callback and total_size:
                        progress_callback(downloaded, total_size)
        
        file_size_mb = cache_path.stat().st_size / (1024 * 1024)
        logger.info(f"Download complete: {file_size_mb:.1f} MB saved to {cache_path}")
        return True
        
    except requests.RequestException as e:
        logger.error(f"Failed to download Colorado data: {e}")
        return False
    except IOError as e:
        logger.error(f"Failed to save Colorado data: {e}")
        return False


def load_colorado_data() -> Optional[pd.DataFrame]:
    """
    Load the Colorado business entities data from cache.
    Downloads if cache is missing or expired.
    
    Returns:
        DataFrame with Colorado business data, or None if failed
    """
    # Check if we need to download/refresh
    if not is_cache_valid():
        if not download_colorado_data():
            return None
    
    cache_path = get_cache_path()
    
    try:
        logger.info("Loading Colorado business entities data...")
        
        # Only load columns we need to save memory
        usecols = [
            'entityname',
            'principaladdress1',
            'principalcity',
            'principalstate',
            'agentfirstname',
            'agentmiddlename',
            'agentlastname',
            'agentorganizationname',
            'entityformationdate'  # To prioritize most recent businesses
        ]
        
        df = pd.read_csv(
            cache_path,
            usecols=lambda c: c.lower() in [col.lower() for col in usecols],
            dtype=str,
            low_memory=False
        )
        
        # Normalize column names to lowercase
        df.columns = df.columns.str.lower()
        
        # Filter to Colorado businesses only
        df = df[df['principalstate'].str.upper() == 'CO']
        
        # Parse formation date and sort by most recent first
        # This ensures when we find multiple matches, we get the most recent one
        df['formation_date_parsed'] = pd.to_datetime(
            df['entityformationdate'], 
            errors='coerce'
        )
        df = df.sort_values('formation_date_parsed', ascending=False, na_position='last')
        
        # Clean up business names for matching
        df['entityname_clean'] = df['entityname'].fillna('').str.upper().str.strip()
        
        # Clean up city names
        df['principalcity_clean'] = df['principalcity'].fillna('').str.upper().str.strip()
        
        logger.info(f"Loaded {len(df):,} Colorado business records")
        return df
        
    except Exception as e:
        logger.error(f"Failed to load Colorado data: {e}")
        return None


def extract_city_from_address(address: str) -> str:
    """
    Extract city name from a full address string.
    
    Args:
        address: Full address like "123 Main St, Aurora, CO 80010, USA"
    
    Returns:
        City name in uppercase, or empty string
    """
    if not address or pd.isna(address):
        return ""
    
    parts = [p.strip() for p in address.split(',')]
    
    # Handle different address formats:
    # Format 1: "Street, City, State ZIP, Country" (4 parts)
    # Format 2: "Street, City, State ZIP" (3 parts)
    # Format 3: "Street, City, State" (3 parts)
    
    if len(parts) >= 4:
        # "Street, City, State ZIP, Country" - city is second part
        return parts[1].upper()
    elif len(parts) == 3:
        # "Street, City, State ZIP" - city is second part
        return parts[1].upper()
    elif len(parts) == 2:
        # "Street, City" - city is second part
        return parts[1].upper()
    
    return ""


def extract_zip_from_address(address: str) -> str:
    """
    Extract ZIP code from a full address string.
    
    Args:
        address: Full address like "123 Main St, Aurora, CO 80010, USA"
    
    Returns:
        5-digit ZIP code or empty string
    """
    import re
    if not address or pd.isna(address):
        return ""
    
    # Look for 5-digit ZIP code pattern
    zip_match = re.search(r'\b(\d{5})(?:-\d{4})?\b', address)
    if zip_match:
        return zip_match.group(1)
    
    return ""


def normalize_street_address(address: str) -> str:
    """
    Normalize a street address for comparison by removing/standardizing
    unit numbers, abbreviations, and other variations.
    
    Args:
        address: Street address like "2101 N Ursula St #10" or "2101 N Ursula St Unit 10"
    
    Returns:
        Normalized address in uppercase
    """
    import re
    if not address or pd.isna(address):
        return ""
    
    addr = str(address).upper().strip()
    
    # Remove common unit/suite indicators and their numbers
    # This handles: #10, Unit 10, Suite 100, Apt 5, Ste 200, etc.
    addr = re.sub(r'\s*[#]\s*\d+\w*', '', addr)
    addr = re.sub(r'\s*(UNIT|SUITE|STE|APT|APARTMENT|BLDG|BUILDING|FL|FLOOR|RM|ROOM)\s*[#]?\s*\d*\w*', '', addr, flags=re.IGNORECASE)
    
    # Standardize common abbreviations
    replacements = {
        r'\bSTREET\b': 'ST',
        r'\bAVENUE\b': 'AVE',
        r'\bBOULEVARD\b': 'BLVD',
        r'\bDRIVE\b': 'DR',
        r'\bLANE\b': 'LN',
        r'\bROAD\b': 'RD',
        r'\bCOURT\b': 'CT',
        r'\bCIRCLE\b': 'CIR',
        r'\bPLACE\b': 'PL',
        r'\bNORTH\b': 'N',
        r'\bSOUTH\b': 'S',
        r'\bEAST\b': 'E',
        r'\bWEST\b': 'W',
    }
    
    for pattern, replacement in replacements.items():
        addr = re.sub(pattern, replacement, addr)
    
    # Remove extra whitespace
    addr = re.sub(r'\s+', ' ', addr).strip()
    
    return addr


def build_agent_name(row: pd.Series) -> str:
    """
    Build agent name from first, middle, and last name fields.
    
    Rules:
    - If both first and last name exist: return "First Middle Last" or "First Last"
    - If only first name exists (no last): return just the first name
    - If only last name exists (no first): return empty (we don't know gender)
    - If neither exists: return empty
    - Never use organization name - we want person names only
    
    Args:
        row: DataFrame row with agent name fields
    
    Returns:
        Formatted agent name or empty string
    """
    first = str(row.get('agentfirstname', '') or '').strip()
    middle = str(row.get('agentmiddlename', '') or '').strip()
    last = str(row.get('agentlastname', '') or '').strip()
    
    # If both first and last name exist
    if first and last:
        if middle:
            return f"{first} {middle} {last}"
        return f"{first} {last}"
    
    # If only first name exists (no last name), just use first name
    if first and not last:
        return first
    
    # If only last name exists (no first name), leave blank (don't know gender)
    # If neither exists, also leave blank
    return ""


def extract_street_address(address: str) -> str:
    """
    Extract street address (first part) from a full address string.
    
    Args:
        address: Full address like "123 Main St, Aurora, CO 80010"
    
    Returns:
        Street address in uppercase, or empty string
    """
    if not address or pd.isna(address):
        return ""
    
    parts = address.split(',')
    if parts:
        return parts[0].strip().upper()
    return ""


def find_best_match_by_name(
    business_name: str,
    city: str,
    colorado_df: pd.DataFrame,
    threshold: int = FUZZY_MATCH_THRESHOLD_NAME
) -> Tuple[Optional[pd.Series], int]:
    """
    Find the best matching Colorado business entity by business name.
    
    Args:
        business_name: Name of the business to match
        city: City name to help filter matches
        colorado_df: DataFrame with Colorado business data
        threshold: Minimum fuzzy match score (0-100)
    
    Returns:
        Tuple of (matched row or None, match score)
    """
    if not business_name or colorado_df is None or colorado_df.empty:
        return None, 0
    
    business_name_clean = business_name.upper().strip()
    city_clean = city.upper().strip() if city else ""
    
    # First, try to narrow down by city if provided
    if city_clean:
        city_matches = colorado_df[colorado_df['principalcity_clean'] == city_clean]
        if not city_matches.empty:
            # Search within city first
            result = process.extractOne(
                business_name_clean,
                city_matches['entityname_clean'].tolist(),
                scorer=fuzz.token_sort_ratio
            )
            
            if result and result[1] >= threshold:
                match_idx = city_matches[city_matches['entityname_clean'] == result[0]].index[0]
                return colorado_df.loc[match_idx], result[1]
    
    # If no city match or no good match in city, search all Colorado businesses
    # Use prefix matching for performance (1M+ records is too slow for full search)
    if len(business_name_clean) >= 3:
        prefix = business_name_clean[:3]
        prefix_matches = colorado_df[colorado_df['entityname_clean'].str.startswith(prefix)]
        
        if not prefix_matches.empty:
            result = process.extractOne(
                business_name_clean,
                prefix_matches['entityname_clean'].tolist(),
                scorer=fuzz.token_sort_ratio
            )
            
            if result and result[1] >= threshold:
                match_idx = prefix_matches[prefix_matches['entityname_clean'] == result[0]].index[0]
                return colorado_df.loc[match_idx], result[1]
    
    return None, 0


def find_best_match_by_address(
    street_address: str,
    city: str,
    colorado_df: pd.DataFrame,
    threshold: int = FUZZY_MATCH_THRESHOLD_ADDRESS
) -> Tuple[Optional[pd.Series], int]:
    """
    Find the best matching Colorado business entity by street address.
    Uses normalized addresses to handle variations like "#10" vs "Unit 10".
    When multiple businesses match the same address, returns the most recently formed one.
    
    Args:
        street_address: Street address to match (e.g., "123 Main St #10")
        city: City name to filter matches
        colorado_df: DataFrame with Colorado business data
        threshold: Minimum fuzzy match score (0-100)
    
    Returns:
        Tuple of (matched row or None, match score)
    """
    if not street_address or colorado_df is None or colorado_df.empty:
        return None, 0
    
    # Normalize the input street address
    street_normalized = normalize_street_address(street_address)
    city_clean = city.upper().strip() if city else ""
    
    if not street_normalized:
        return None, 0
    
    # Filter by city first (address matching without city is too broad)
    if city_clean:
        city_matches = colorado_df[colorado_df['principalcity_clean'] == city_clean]
        if not city_matches.empty:
            # Normalize Colorado addresses for comparison
            city_matches = city_matches.copy()
            city_matches['address_normalized'] = city_matches['principaladdress1'].apply(normalize_street_address)
            
            # Only search non-empty addresses
            address_matches = city_matches[city_matches['address_normalized'] != '']
            
            if not address_matches.empty:
                # Get all matches above threshold, not just the best one
                results = process.extract(
                    street_normalized,
                    address_matches['address_normalized'].tolist(),
                    scorer=fuzz.token_sort_ratio,
                    limit=10  # Get top 10 matches
                )
                
                # Filter to matches above threshold
                good_matches = [(addr, score, idx) for addr, score, idx in results if score >= threshold]
                
                if good_matches:
                    # Get the best score
                    best_score = good_matches[0][1]
                    
                    # Find all matches with the best score
                    best_matches = [m for m in good_matches if m[1] == best_score]
                    
                    # Get the indices of these matches in the original DataFrame
                    best_addresses = [m[0] for m in best_matches]
                    matching_rows = address_matches[address_matches['address_normalized'].isin(best_addresses)]
                    
                    # Sort by formation date (most recent first) and take the first one
                    # The DataFrame is already sorted by formation_date_parsed descending
                    if not matching_rows.empty:
                        best_match_idx = matching_rows.index[0]
                        return colorado_df.loc[best_match_idx], best_score
    
    return None, 0
    
    return None, 0


def find_best_match(
    business_name: str,
    address: str,
    city: str,
    colorado_df: pd.DataFrame
) -> Tuple[Optional[pd.Series], int, str]:
    """
    Find the best matching Colorado business entity for a given business.
    First tries matching by name, then falls back to address matching.
    
    Args:
        business_name: Name of the business to match
        address: Full address string
        city: City name to help filter matches
        colorado_df: DataFrame with Colorado business data
    
    Returns:
        Tuple of (matched row or None, match score, match_type: 'name'|'address'|'')
    """
    # First, try matching by business name (80% threshold)
    match, score = find_best_match_by_name(business_name, city, colorado_df)
    if match is not None:
        return match, score, 'name'
    
    # If name matching failed, try matching by address (70% threshold)
    street_address = extract_street_address(address)
    if street_address:
        match, score = find_best_match_by_address(street_address, city, colorado_df)
        if match is not None:
            return match, score, 'address'
    
    return None, 0, ''


def enrich_with_agent_names(
    places_df: pd.DataFrame,
    progress_callback: callable = None
) -> pd.DataFrame:
    """
    Enrich a DataFrame of places with registered agent names from Colorado SOS data.
    
    Matching strategy:
    1. First tries to match by business name
    2. If no name match, falls back to matching by street address
    
    Args:
        places_df: DataFrame with place data (must have 'Name' and 'Address' columns)
        progress_callback: Optional callback(current, total) for progress updates
    
    Returns:
        DataFrame with added 'BusinessName', 'AgentName', 'MatchConfidence', and 'MatchType' columns
    """
    # Load Colorado data
    colorado_df = load_colorado_data()
    
    if colorado_df is None:
        logger.warning("Could not load Colorado data, skipping agent name enrichment")
        places_df['BusinessName'] = ''
        places_df['AgentName'] = ''
        places_df['MatchConfidence'] = 0.0
        places_df['MatchType'] = ''
        return places_df
    
    # Initialize new columns
    places_df = places_df.copy()
    places_df['BusinessName'] = ''
    places_df['AgentName'] = ''
    places_df['MatchConfidence'] = 0.0  # Use float to avoid dtype warning
    places_df['MatchType'] = ''
    
    total = len(places_df)
    matched_count = 0
    name_match_count = 0
    address_match_count = 0
    
    logger.info(f"Matching {total} businesses against Colorado database...")
    
    for idx, row in places_df.iterrows():
        business_name = row.get('Name', '')
        address = row.get('Address', '')
        
        # Extract city from address
        city = extract_city_from_address(address)
        
        # Find best match (tries name first, then address)
        match, score, match_type = find_best_match(business_name, address, city, colorado_df)
        
        if match is not None:
            # Get the official business name from Colorado SOS (entityname)
            entity_name = match.get('entityname', '')
            # Make sure we don't put nan/null values
            if entity_name and pd.notna(entity_name) and str(entity_name).lower() not in ('nan', 'null', 'none', ''):
                places_df.at[idx, 'BusinessName'] = str(entity_name).strip()
            
            agent_name = build_agent_name(match)
            places_df.at[idx, 'AgentName'] = agent_name
            places_df.at[idx, 'MatchConfidence'] = score
            places_df.at[idx, 'MatchType'] = match_type
            
            matched_count += 1
            if match_type == 'name':
                name_match_count += 1
            elif match_type == 'address':
                address_match_count += 1
        
        if progress_callback:
            progress_callback(idx + 1, total)
    
    # Log summary
    unmatched_count = total - matched_count
    match_rate = (matched_count / total * 100) if total > 0 else 0
    
    logger.info(f"Agent name matching complete:")
    logger.info(f"  - Total matched: {matched_count} ({match_rate:.1f}%)")
    logger.info(f"    - By name: {name_match_count}")
    logger.info(f"    - By address: {address_match_count}")
    logger.info(f"  - Unmatched: {unmatched_count} ({100-match_rate:.1f}%)")
    
    return places_df


def get_colorado_data_status() -> dict:
    """
    Get status information about the Colorado data cache.
    
    Returns:
        Dictionary with cache status info
    """
    cache_path = get_cache_path()
    
    if not cache_path.exists():
        return {
            'cached': False,
            'path': str(cache_path),
            'size_mb': 0,
            'age_days': None,
            'valid': False
        }
    
    file_mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
    age = datetime.now() - file_mtime
    
    return {
        'cached': True,
        'path': str(cache_path),
        'size_mb': round(cache_path.stat().st_size / (1024 * 1024), 1),
        'age_days': age.days,
        'last_updated': file_mtime.strftime('%Y-%m-%d %H:%M'),
        'valid': age < timedelta(days=CACHE_MAX_AGE_DAYS)
    }