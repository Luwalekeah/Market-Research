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
        
        # First, read just the header to see what columns exist
        df_header = pd.read_csv(cache_path, nrows=0)
        available_cols = [c.lower() for c in df_header.columns]
        logger.info(f"Available columns: {available_cols[:10]}...")  # Log first 10
        
        # Define columns we want (will only load ones that exist)
        desired_cols = [
            'entityname',
            'principaladdress1',
            'principalcity',
            'principalstate',
            'principalzipcode',
            'agentfirstname',
            'agentmiddlename',
            'agentlastname',
            'agentorganizationname',
            # Try different possible names for formation date
            'entityformdate',  # This is the actual column name!
            'entityformationdate',
            'formationdate', 
            'formation_date',
            'dateformed',
            # Try different possible names for status
            'entitystatus',
            'status',
            'entitystatusname'
        ]
        
        # Filter to only columns that actually exist
        cols_to_load = [c for c in desired_cols if c.lower() in available_cols]
        logger.info(f"Loading columns: {cols_to_load}")
        
        df = pd.read_csv(
            cache_path,
            usecols=lambda c: c.lower() in [col.lower() for col in cols_to_load],
            dtype=str,
            low_memory=False
        )
        
        # Normalize column names to lowercase
        df.columns = df.columns.str.lower()
        
        # Filter to Colorado businesses only
        if 'principalstate' in df.columns:
            df = df[df['principalstate'].fillna('').str.upper() == 'CO']
        
        # Handle entity status - try different column names
        status_col = None
        for col_name in ['entitystatus', 'status', 'entitystatusname']:
            if col_name in df.columns:
                status_col = col_name
                break
        
        if status_col:
            df['entitystatus_clean'] = df[status_col].fillna('').str.upper().str.strip()
            
            # Create status priority (lower = better)
            def status_priority(status):
                status_upper = str(status).upper()
                if 'GOOD STANDING' in status_upper:
                    return 1
                elif 'EXISTS' in status_upper:
                    return 2
                elif 'DELINQUENT' in status_upper:
                    return 3
                elif 'DISSOLVED' in status_upper or 'WITHDRAWN' in status_upper or 'REVOKED' in status_upper:
                    return 99
                else:
                    return 50
            
            df['status_priority'] = df['entitystatus_clean'].apply(status_priority)
            
            # Filter out dissolved/withdrawn businesses
            df = df[df['status_priority'] < 99]
        else:
            logger.warning("No status column found - cannot filter by entity status")
            df['status_priority'] = 50  # Default priority
        
        # Handle formation date - try different column names
        date_col = None
        for col_name in ['entityformdate', 'entityformationdate', 'formationdate', 'formation_date', 'dateformed']:
            if col_name in df.columns:
                date_col = col_name
                break
        
        if date_col:
            df['formation_date_parsed'] = pd.to_datetime(df[date_col], errors='coerce')
        else:
            logger.warning("No formation date column found - cannot sort by date")
            df['formation_date_parsed'] = pd.NaT
        
        # Sort by: status priority (ascending), then formation date (descending/most recent)
        df = df.sort_values(
            ['status_priority', 'formation_date_parsed'], 
            ascending=[True, False], 
            na_position='last'
        )
        
        # Clean up business names for matching
        df['entityname_clean'] = df['entityname'].fillna('').str.upper().str.strip()
        
        # Clean up city names
        if 'principalcity' in df.columns:
            df['principalcity_clean'] = df['principalcity'].fillna('').str.upper().str.strip()
        else:
            df['principalcity_clean'] = ''
        
        # Clean up zip codes
        if 'principalzipcode' in df.columns:
            df['principalzipcode_clean'] = df['principalzipcode'].fillna('').str.strip().str[:5]
        else:
            df['principalzipcode_clean'] = ''
        
        logger.info(f"Loaded {len(df):,} Colorado business records")
        return df
        
    except Exception as e:
        logger.error(f"Failed to load Colorado data: {e}")
        import traceback
        logger.error(traceback.format_exc())
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


def _clean_name_field(value) -> str:
    """Clean a name field, handling nan/None/empty values."""
    if value is None or pd.isna(value):
        return ""
    s = str(value).strip()
    # Check for various nan representations
    if s.lower() in ('nan', 'none', 'null', ''):
        return ""
    return s


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
    first = _clean_name_field(row.get('agentfirstname', ''))
    middle = _clean_name_field(row.get('agentmiddlename', ''))
    last = _clean_name_field(row.get('agentlastname', ''))
    
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
    When multiple businesses have similar names, prioritizes by:
    1. Entity status (Good Standing > Delinquent)
    2. Formation date (most recent)
    
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
            # Get multiple matches, not just one
            results = process.extract(
                business_name_clean,
                city_matches['entityname_clean'].tolist(),
                scorer=fuzz.token_sort_ratio,
                limit=10
            )
            
            # Filter to matches above threshold
            good_matches = [(name, score, idx) for name, score, idx in results if score >= threshold]
            
            if good_matches:
                best_score = good_matches[0][1]
                # Get all matches with the best score (or within 5 points)
                best_names = [m[0] for m in good_matches if m[1] >= best_score - 5]
                matching_rows = city_matches[city_matches['entityname_clean'].isin(best_names)]
                
                if not matching_rows.empty:
                    # DataFrame is already sorted by status_priority, formation_date
                    # So just take the first one
                    best_match_idx = matching_rows.index[0]
                    return colorado_df.loc[best_match_idx], best_score
    
    # If no city match or no good match in city, search all Colorado businesses
    # Use prefix matching for performance (1M+ records is too slow for full search)
    if len(business_name_clean) >= 3:
        prefix = business_name_clean[:3]
        prefix_matches = colorado_df[colorado_df['entityname_clean'].str.startswith(prefix)]
        
        if not prefix_matches.empty:
            results = process.extract(
                business_name_clean,
                prefix_matches['entityname_clean'].tolist(),
                scorer=fuzz.token_sort_ratio,
                limit=10
            )
            
            good_matches = [(name, score, idx) for name, score, idx in results if score >= threshold]
            
            if good_matches:
                best_score = good_matches[0][1]
                best_names = [m[0] for m in good_matches if m[1] >= best_score - 5]
                matching_rows = prefix_matches[prefix_matches['entityname_clean'].isin(best_names)]
                
                if not matching_rows.empty:
                    best_match_idx = matching_rows.index[0]
                    return colorado_df.loc[best_match_idx], best_score
    
    return None, 0


def find_best_match_by_address(
    street_address: str,
    city: str,
    colorado_df: pd.DataFrame,
    business_name: str = "",
    threshold: int = FUZZY_MATCH_THRESHOLD_ADDRESS
) -> Tuple[Optional[pd.Series], int]:
    """
    Find the best matching Colorado business entity by street address.
    Uses normalized addresses to handle variations like "#10" vs "Unit 10".
    When multiple businesses match the same address, prioritizes by:
    1. Entity status (Good Standing > Delinquent)
    2. Name similarity to the Google Places business name
    3. Formation date (most recent)
    
    Args:
        street_address: Street address to match (e.g., "123 Main St #10")
        city: City name to filter matches
        colorado_df: DataFrame with Colorado business data
        business_name: Original business name for similarity comparison
        threshold: Minimum fuzzy match score (0-100)
    
    Returns:
        Tuple of (matched row or None, match score)
    """
    if not street_address or colorado_df is None or colorado_df.empty:
        return None, 0
    
    # Normalize the input street address
    street_normalized = normalize_street_address(street_address)
    city_clean = city.upper().strip() if city else ""
    business_name_clean = business_name.upper().strip() if business_name else ""
    
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
                # Get all matches above threshold
                results = process.extract(
                    street_normalized,
                    address_matches['address_normalized'].tolist(),
                    scorer=fuzz.token_sort_ratio,
                    limit=20  # Get more matches to filter
                )
                
                # Filter to matches above threshold
                good_matches = [(addr, score, idx) for addr, score, idx in results if score >= threshold]
                
                if good_matches:
                    # Get the best address match score
                    best_score = good_matches[0][1]
                    
                    # Find all matches with the best score (or close to it)
                    best_addresses = [m[0] for m in good_matches if m[1] >= best_score - 5]
                    matching_rows = address_matches[address_matches['address_normalized'].isin(best_addresses)].copy()
                    
                    if not matching_rows.empty:
                        # Calculate name similarity score for each match
                        if business_name_clean:
                            matching_rows['name_similarity'] = matching_rows['entityname_clean'].apply(
                                lambda x: fuzz.token_sort_ratio(business_name_clean, x)
                            )
                        else:
                            matching_rows['name_similarity'] = 0
                        
                        # Sort by: status_priority (asc), name_similarity (desc), formation_date (desc)
                        matching_rows = matching_rows.sort_values(
                            ['status_priority', 'name_similarity', 'formation_date_parsed'],
                            ascending=[True, False, False]
                        )
                        
                        best_match_idx = matching_rows.index[0]
                        return colorado_df.loc[best_match_idx], best_score
    
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
    # Pass business_name so we can use name similarity as a tiebreaker
    street_address = extract_street_address(address)
    if street_address:
        match, score = find_best_match_by_address(
            street_address, 
            city, 
            colorado_df,
            business_name=business_name
        )
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