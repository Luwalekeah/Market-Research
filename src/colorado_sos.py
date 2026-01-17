"""
Module for matching businesses against Colorado Secretary of State business entities database.

Downloads and caches the Colorado business entities CSV and performs fuzzy matching
to find registered agent names for businesses found via Google Places API.

Optimizations:
- Business name normalization (strips LLC, Inc, etc.)
- Precomputed normalized addresses and name prefixes
- ZIP code filtering for disambiguation
- WRatio scorer for better fuzzy matching
- Index-based selection (deterministic)
"""
import os
import re
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, List
from collections import defaultdict

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
FUZZY_MATCH_THRESHOLD_NAME = 85  # Increased from 80 for stricter matching
FUZZY_MATCH_THRESHOLD_ADDRESS = 70  # Lower threshold for address (since we filter by city)
MIN_NAME_SIMILARITY_FOR_ADDRESS = 45  # Minimum name similarity for address-based matches
MIN_WORD_OVERLAP = 1  # Minimum number of significant words that must match

# =============================================================================
# BUSINESS NAME NORMALIZATION
# =============================================================================

# Legal suffixes to strip from business names
LEGAL_SUFFIXES = {
    "LLC", "L L C", "INC", "INCORPORATED", "CORP", "CORPORATION", 
    "CO", "COMPANY", "LTD", "LIMITED", "LP", "L P", "LLP", "L L P",
    "PC", "P C", "PLLC", "P L L C", "PA", "P A", "NA", "N A",
    "PROF", "PROFESSIONAL", "ASSOC", "ASSOCIATES", "ASSOCIATION",
    "GROUP", "GRP", "HOLDINGS", "ENTERPRISES", "PARTNERS", "PARTNERSHIP"
}

# Stopwords to remove from beginning
STOPWORDS = {"THE", "A", "AN"}


def normalize_business_name(name: str) -> str:
    """
    Normalize a business name for comparison.
    
    - Converts to uppercase
    - Replaces & with AND
    - Removes punctuation
    - Strips leading articles (THE, A, AN)
    - Strips trailing legal suffixes (LLC, INC, CORP, etc.)
    
    Args:
        name: Business name to normalize
    
    Returns:
        Normalized business name
    """
    if not name or pd.isna(name):
        return ""
    
    s = str(name).upper().strip()
    
    # Replace common symbols
    s = s.replace("&", " AND ")
    s = s.replace("+", " AND ")
    
    # Drop punctuation to spaces
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    
    # Collapse whitespace and tokenize
    tokens = [t for t in s.split() if t]
    
    # Drop leading stopwords
    while tokens and tokens[0] in STOPWORDS:
        tokens = tokens[1:]
    
    # Drop trailing legal suffixes (repeat because some have multiple like "LLC INC")
    while tokens and tokens[-1] in LEGAL_SUFFIXES:
        tokens.pop()
    
    return " ".join(tokens)


def get_significant_words(normalized_name: str) -> set:
    """
    Get significant words from a normalized business name.
    Excludes common filler words that don't indicate the actual business.
    
    Args:
        normalized_name: Already normalized business name
    
    Returns:
        Set of significant words
    """
    if not normalized_name:
        return set()
    
    # Words that don't help identify a business uniquely
    FILLER_WORDS = {
        # Common articles/prepositions
        'AND', 'OF', 'THE', 'IN', 'AT', 'ON', 'FOR', 'TO', 'BY', 'WITH', 'A', 'AN',
        # Generic business terms
        'SERVICES', 'SERVICE', 'SOLUTIONS', 'CONSULTING', 'MANAGEMENT',
        'GROUP', 'HOLDINGS', 'ENTERPRISES', 'PARTNERS', 'ASSOCIATES', 'ASSOCIATION',
        'COMPANY', 'COMPANIES', 'CORPORATION', 'CORP', 'BUSINESS', 'BUSINESSES',
        'INTERNATIONAL', 'GLOBAL', 'NATIONAL', 'AMERICAN', 'USA', 'US',
        'LIMITED', 'LTD', 'UNLIMITED',
        # Directional/locational
        'NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL', 'NORTHERN', 'SOUTHERN',
        'EASTERN', 'WESTERN', 'DOWNTOWN', 'UPTOWN', 'MIDTOWN',
        # Ordinals
        'NEW', 'OLD', 'FIRST', 'SECOND', 'THIRD', 'FOURTH', 'FIFTH',
        # Common generic words that appear in many business names
        'COLORADO', 'DENVER', 'AURORA', 'BOULDER', 'SPRINGS',  # CO cities
        'MOUNTAIN', 'MOUNTAINS', 'VALLEY', 'CREEK', 'RIVER', 'LAKE', 'PARK',
        'WILD', 'GOLDEN', 'SILVER', 'BLUE', 'GREEN', 'RED', 'BLACK', 'WHITE',
        'BEST', 'GREAT', 'PREMIER', 'ELITE', 'PRO', 'PREMIUM', 'QUALITY',
        'MEETING', 'MEETINGS', 'FRIENDS', 'FRIEND',
        # Industry terms (too generic)
        'RESTAURANT', 'RESTAURANTS', 'BAR', 'GRILL', 'CAFE', 'KITCHEN',
        'BREWING', 'BREWERY', 'BREWHOUSE', 'BREW', 'BEER', 'BEERS', 'ALE',
        'TAP', 'TAPROOM', 'ROOM', 'HOUSE', 'PUB', 'TAVERN', 'LOUNGE',
        'WELLNESS', 'HEALTH', 'FITNESS', 'SPA', 'SALON',
    }
    
    words = set(normalized_name.split())
    significant = words - FILLER_WORDS
    
    # Also filter out very short words (1-2 chars) and pure numbers
    significant = {w for w in significant if len(w) >= 3 and not w.isdigit()}
    
    return significant


def has_sufficient_word_overlap(name1_norm: str, name2_norm: str, min_overlap: int = MIN_WORD_OVERLAP) -> bool:
    """
    Check if two normalized business names have sufficient word overlap.
    This prevents matches like "Cerebral Brewing" → "Aurora Downtown Partnership"
    where the fuzzy score might be high due to partial matches but core words don't match.
    
    Args:
        name1_norm: First normalized name
        name2_norm: Second normalized name
        min_overlap: Minimum number of significant words that must match
    
    Returns:
        True if sufficient overlap exists
    """
    words1 = get_significant_words(name1_norm)
    words2 = get_significant_words(name2_norm)
    
    # If either name has no significant words after filtering, we can't determine
    # In this case, be conservative and return False (don't allow match)
    if not words1 or not words2:
        return False
    
    # Check exact word overlap
    overlap = words1 & words2
    if len(overlap) >= min_overlap:
        return True
    
    # Check for partial word matches (e.g., "STATION" in "STATION26")
    # But be stricter - require at least 5 char match and the word must be substantial
    for w1 in words1:
        for w2 in words2:
            # Both words must be at least 5 chars for partial matching
            if len(w1) >= 5 and len(w2) >= 5:
                # Check if one contains the other
                if w1 in w2 or w2 in w1:
                    # Make sure it's not just a common suffix/prefix
                    shorter = w1 if len(w1) <= len(w2) else w2
                    if len(shorter) >= 5:  # Substantial word
                        return True
                # Check if they share a common root (first 5+ chars)
                if w1[:5] == w2[:5]:
                    return True
    
    return False


def get_name_prefix(normalized_name: str, length: int = 4) -> str:
    """
    Get a prefix key from a normalized name for fast lookups.
    
    Args:
        normalized_name: Already normalized business name
        length: Prefix length (default 4)
    
    Returns:
        Alphanumeric prefix of specified length
    """
    # Remove spaces and get first N characters
    key = re.sub(r"[^A-Z0-9]", "", normalized_name)
    return key[:length] if key else ""


# =============================================================================
# ADDRESS NORMALIZATION
# =============================================================================

def normalize_street_address(address: str) -> str:
    """
    Normalize a street address for comparison.
    
    - Converts to uppercase
    - Removes unit/suite/apt numbers
    - Standardizes abbreviations
    
    Args:
        address: Street address to normalize
    
    Returns:
        Normalized address in uppercase
    """
    if not address or pd.isna(address):
        return ""
    
    addr = str(address).upper().strip()
    
    # Remove common unit/suite indicators and their numbers
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
        r'\bPARKWAY\b': 'PKWY',
        r'\bHIGHWAY\b': 'HWY',
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


def extract_street_address(address: str) -> str:
    """Extract street address (first part) from a full address string."""
    if not address or pd.isna(address):
        return ""
    
    parts = address.split(',')
    if parts:
        return parts[0].strip().upper()
    return ""


def extract_city_from_address(address: str) -> str:
    """Extract city name from a full address string."""
    if not address or pd.isna(address):
        return ""
    
    parts = [p.strip() for p in address.split(',')]
    
    # Format: "Street, City, State ZIP, Country" or variations
    if len(parts) >= 2:
        return parts[1].upper()
    
    return ""


def extract_zip_from_address(address: str) -> str:
    """Extract 5-digit ZIP code from address."""
    if not address or pd.isna(address):
        return ""
    
    zip_match = re.search(r'\b(\d{5})(?:-\d{4})?\b', address)
    if zip_match:
        return zip_match.group(1)
    
    return ""


# =============================================================================
# AGENT NAME HANDLING
# =============================================================================

def _clean_name_field(value) -> str:
    """Clean a name field, handling nan/None/empty values."""
    if value is None or pd.isna(value):
        return ""
    s = str(value).strip()
    if s.lower() in ('nan', 'none', 'null', ''):
        return ""
    return s


def build_agent_name(row: pd.Series) -> str:
    """
    Build agent name from first, middle, and last name fields.
    
    Rules:
    - If both first and last name exist: return "First Middle Last" or "First Last"
    - If only first name exists: return just the first name
    - If only last name exists: return empty (we don't know gender)
    - Never use organization name - we want person names only
    """
    first = _clean_name_field(row.get('agentfirstname', ''))
    middle = _clean_name_field(row.get('agentmiddlename', ''))
    last = _clean_name_field(row.get('agentlastname', ''))
    
    if first and last:
        if middle:
            return f"{first} {middle} {last}"
        return f"{first} {last}"
    
    if first and not last:
        return first
    
    return ""


# =============================================================================
# CACHE MANAGEMENT
# =============================================================================

def get_cache_path() -> Path:
    """Get the path to the cached Colorado CSV file."""
    data_dir = Path(DATA_DIR)
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / COLORADO_CSV_FILENAME


def is_cache_valid() -> bool:
    """Check if the cached Colorado CSV exists and is fresh."""
    cache_path = get_cache_path()
    
    if not cache_path.exists():
        logger.info("Colorado data cache not found")
        return False
    
    file_mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
    age = datetime.now() - file_mtime
    
    if age > timedelta(days=CACHE_MAX_AGE_DAYS):
        logger.info(f"Colorado data cache is {age.days} days old, needs refresh")
        return False
    
    logger.info(f"Colorado data cache is valid ({age.days} days old)")
    return True


def download_colorado_data(progress_callback: callable = None) -> bool:
    """Download the Colorado business entities CSV."""
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


# =============================================================================
# DATA LOADING WITH PRECOMPUTATION
# =============================================================================

# Global cache for prefix lookup map
_prefix_lookup_map: Dict[str, List[int]] = {}


def load_colorado_data() -> Optional[pd.DataFrame]:
    """
    Load the Colorado business entities data from cache.
    Downloads if cache is missing or expired.
    
    Precomputes:
    - Normalized business names
    - Name prefixes for fast lookup
    - Normalized addresses
    - Status priorities
    """
    global _prefix_lookup_map
    
    if not is_cache_valid():
        if not download_colorado_data():
            return None
    
    cache_path = get_cache_path()
    
    try:
        logger.info("Loading Colorado business entities data...")
        
        # Read header to see available columns
        df_header = pd.read_csv(cache_path, nrows=0)
        available_cols = [c.lower() for c in df_header.columns]
        
        # Define columns we want
        desired_cols = [
            'entityname', 'principaladdress1', 'principalcity', 'principalstate',
            'principalzipcode', 'agentfirstname', 'agentmiddlename', 'agentlastname',
            'agentorganizationname', 'entityformdate', 'entityformationdate',
            'entitystatus', 'status'
        ]
        
        cols_to_load = [c for c in desired_cols if c.lower() in available_cols]
        
        df = pd.read_csv(
            cache_path,
            usecols=lambda c: c.lower() in [col.lower() for col in cols_to_load],
            dtype=str,
            low_memory=False
        )
        
        df.columns = df.columns.str.lower()
        
        # Filter to Colorado businesses only
        if 'principalstate' in df.columns:
            df = df[df['principalstate'].fillna('').str.upper() == 'CO']
        
        # =================================================================
        # STATUS HANDLING
        # =================================================================
        status_col = None
        for col_name in ['entitystatus', 'status']:
            if col_name in df.columns:
                status_col = col_name
                break
        
        if status_col:
            # Keep original status for display
            df['entitystatus_display'] = df[status_col].fillna('').astype(str).str.strip()
            df['entitystatus_clean'] = df['entitystatus_display'].str.upper()
            
            def status_priority(status):
                status_upper = str(status).upper()
                if 'GOOD STANDING' in status_upper:
                    return 1
                elif 'EXISTS' in status_upper:
                    return 2
                elif 'DELINQUENT' in status_upper:
                    return 3
                elif any(x in status_upper for x in ['DISSOLVED', 'WITHDRAWN', 'REVOKED', 'NONCOMPLIANT']):
                    return 99
                else:
                    return 50
            
            df['status_priority'] = df['entitystatus_clean'].apply(status_priority)
            
            # Filter out dissolved/withdrawn businesses
            before_filter = len(df)
            df = df[df['status_priority'] < 99]
            logger.info(f"Filtered out {before_filter - len(df):,} dissolved/withdrawn entities")
        else:
            df['entitystatus_display'] = ''
            df['status_priority'] = 50
        
        # =================================================================
        # FORMATION DATE
        # =================================================================
        date_col = None
        for col_name in ['entityformdate', 'entityformationdate']:
            if col_name in df.columns:
                date_col = col_name
                break
        
        if date_col:
            df['formation_date_parsed'] = pd.to_datetime(df[date_col], errors='coerce')
        else:
            df['formation_date_parsed'] = pd.NaT
        
        # =================================================================
        # PRECOMPUTE NORMALIZED NAMES
        # =================================================================
        logger.info("Precomputing normalized business names...")
        df['entityname_norm'] = df['entityname'].apply(normalize_business_name)
        df['name_prefix4'] = df['entityname_norm'].apply(lambda x: get_name_prefix(x, 4))
        
        # Build prefix lookup map for O(1) lookups
        _prefix_lookup_map = defaultdict(list)
        for idx, prefix in zip(df.index, df['name_prefix4']):
            if prefix:
                _prefix_lookup_map[prefix].append(idx)
        logger.info(f"Built prefix map with {len(_prefix_lookup_map):,} unique prefixes")
        
        # =================================================================
        # PRECOMPUTE NORMALIZED ADDRESSES
        # =================================================================
        logger.info("Precomputing normalized addresses...")
        if 'principaladdress1' in df.columns:
            df['principaladdress1_norm'] = df['principaladdress1'].apply(normalize_street_address)
        else:
            df['principaladdress1_norm'] = ''
        
        # =================================================================
        # CLEAN UP OTHER FIELDS
        # =================================================================
        df['principalcity_clean'] = df['principalcity'].fillna('').str.upper().str.strip() if 'principalcity' in df.columns else ''
        df['principalzipcode_clean'] = df['principalzipcode'].fillna('').str.strip().str[:5] if 'principalzipcode' in df.columns else ''
        
        # Sort by status priority, then formation date
        df = df.sort_values(
            ['status_priority', 'formation_date_parsed'],
            ascending=[True, False],
            na_position='last'
        )
        
        logger.info(f"Loaded {len(df):,} Colorado business records")
        return df
        
    except Exception as e:
        logger.error(f"Failed to load Colorado data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


# =============================================================================
# MATCHING FUNCTIONS
# =============================================================================

def find_best_match_by_name(
    business_name: str,
    city: str,
    colorado_df: pd.DataFrame,
    zip_code: str = "",
    threshold: int = FUZZY_MATCH_THRESHOLD_NAME
) -> Tuple[Optional[pd.Series], int]:
    """
    Find the best matching Colorado business entity by business name.
    
    Uses normalized names and WRatio scorer for better matching.
    Validates that matches have sufficient word overlap to prevent
    false matches like "Cerebral Brewing" → "Aurora Downtown Partnership".
    """
    if not business_name or colorado_df is None or colorado_df.empty:
        return None, 0
    
    business_name_norm = normalize_business_name(business_name)
    if not business_name_norm:
        return None, 0
    
    city_clean = city.upper().strip() if city else ""
    zip_clean = zip_code.strip()[:5] if zip_code else ""
    
    # Try to narrow down by city first
    if city_clean:
        candidates = colorado_df[colorado_df['principalcity_clean'] == city_clean]
        
        # Further filter by ZIP if available and results are large
        if len(candidates) > 100 and zip_clean:
            zip_filtered = candidates[candidates['principalzipcode_clean'] == zip_clean]
            if not zip_filtered.empty:
                candidates = zip_filtered
        
        if not candidates.empty:
            choices = candidates['entityname_norm'].tolist()
            results = process.extract(
                business_name_norm,
                choices,
                scorer=fuzz.WRatio,
                limit=10,
                score_cutoff=threshold
            )
            
            if results:
                top_score = results[0][1]
                # Get positions within 3 points of top score (tight bucket)
                positions = [pos for _, score, pos in results if score >= top_score - 3]
                
                # Select from candidates using positions
                matched_candidates = candidates.iloc[positions].copy()
                
                # Filter by word overlap to prevent false matches
                matched_candidates = matched_candidates[
                    matched_candidates['entityname_norm'].apply(
                        lambda x: has_sufficient_word_overlap(business_name_norm, x)
                    )
                ]
                
                if matched_candidates.empty:
                    # No matches with sufficient word overlap in city
                    pass  # Fall through to prefix search
                else:
                    # Sort by status priority and formation date
                    matched_candidates = matched_candidates.sort_values(
                        ['status_priority', 'formation_date_parsed'],
                        ascending=[True, False],
                        na_position='last'
                    )
                    
                    best_row = matched_candidates.iloc[0]
                    return best_row, top_score
    
    # Fallback: use prefix lookup map for fast search
    prefix = get_name_prefix(business_name_norm, 4)
    if prefix and prefix in _prefix_lookup_map:
        indices = _prefix_lookup_map[prefix]
        prefix_matches = colorado_df.loc[colorado_df.index.isin(indices)]
        
        if not prefix_matches.empty:
            choices = prefix_matches['entityname_norm'].tolist()
            results = process.extract(
                business_name_norm,
                choices,
                scorer=fuzz.WRatio,
                limit=10,
                score_cutoff=threshold
            )
            
            if results:
                top_score = results[0][1]
                positions = [pos for _, score, pos in results if score >= top_score - 3]
                
                matched_candidates = prefix_matches.iloc[positions].copy()
                
                # Filter by word overlap
                matched_candidates = matched_candidates[
                    matched_candidates['entityname_norm'].apply(
                        lambda x: has_sufficient_word_overlap(business_name_norm, x)
                    )
                ]
                
                if not matched_candidates.empty:
                    matched_candidates = matched_candidates.sort_values(
                        ['status_priority', 'formation_date_parsed'],
                        ascending=[True, False],
                        na_position='last'
                    )
                    
                    best_row = matched_candidates.iloc[0]
                    return best_row, top_score
    
    return None, 0


def find_best_match_by_address(
    street_address: str,
    city: str,
    colorado_df: pd.DataFrame,
    business_name: str = "",
    zip_code: str = "",
    threshold: int = FUZZY_MATCH_THRESHOLD_ADDRESS,
    min_name_similarity: int = MIN_NAME_SIMILARITY_FOR_ADDRESS
) -> Tuple[Optional[pd.Series], int]:
    """
    Find the best matching Colorado business entity by street address.
    
    Only returns matches where business name similarity is above min_name_similarity
    to avoid returning unrelated businesses at the same address.
    """
    if not street_address or colorado_df is None or colorado_df.empty:
        return None, 0
    
    street_normalized = normalize_street_address(street_address)
    if not street_normalized:
        return None, 0
    
    city_clean = city.upper().strip() if city else ""
    zip_clean = zip_code.strip()[:5] if zip_code else ""
    business_name_norm = normalize_business_name(business_name) if business_name else ""
    
    if not city_clean:
        return None, 0
    
    # Filter by city
    candidates = colorado_df[colorado_df['principalcity_clean'] == city_clean]
    
    if candidates.empty:
        return None, 0
    
    # Further filter by ZIP if available
    if zip_clean:
        zip_filtered = candidates[candidates['principalzipcode_clean'] == zip_clean]
        if not zip_filtered.empty:
            candidates = zip_filtered
    
    # Only search non-empty addresses (using precomputed normalized addresses)
    address_candidates = candidates[candidates['principaladdress1_norm'] != '']
    
    if address_candidates.empty:
        return None, 0
    
    # Find address matches
    choices = address_candidates['principaladdress1_norm'].tolist()
    results = process.extract(
        street_normalized,
        choices,
        scorer=fuzz.token_sort_ratio,
        limit=20,
        score_cutoff=threshold
    )
    
    if not results:
        return None, 0
    
    top_score = results[0][1]
    # Get positions within 5 points for addresses (more tolerance)
    positions = [pos for _, score, pos in results if score >= top_score - 5]
    
    matched_candidates = address_candidates.iloc[positions].copy()
    
    if matched_candidates.empty:
        return None, 0
    
    # Calculate name similarity for each match
    if business_name_norm:
        matched_candidates['name_similarity'] = matched_candidates['entityname_norm'].apply(
            lambda x: fuzz.WRatio(business_name_norm, x)
        )
        
        # Filter by minimum name similarity
        matched_candidates = matched_candidates[matched_candidates['name_similarity'] >= min_name_similarity]
        
        if matched_candidates.empty:
            return None, 0
        
        # Sort by: status, name similarity, formation date
        matched_candidates = matched_candidates.sort_values(
            ['status_priority', 'name_similarity', 'formation_date_parsed'],
            ascending=[True, False, False],
            na_position='last'
        )
    else:
        matched_candidates = matched_candidates.sort_values(
            ['status_priority', 'formation_date_parsed'],
            ascending=[True, False],
            na_position='last'
        )
    
    best_row = matched_candidates.iloc[0]
    return best_row, top_score


def find_best_match(
    business_name: str,
    address: str,
    city: str,
    colorado_df: pd.DataFrame
) -> Tuple[Optional[pd.Series], int, str]:
    """
    Find the best matching Colorado business entity.
    Tries name matching first, then falls back to address matching.
    
    Returns:
        Tuple of (matched row or None, match score, match_type: 'name'|'address'|'')
    """
    zip_code = extract_zip_from_address(address)
    
    # First, try matching by business name
    match, score = find_best_match_by_name(business_name, city, colorado_df, zip_code)
    if match is not None:
        return match, score, 'name'
    
    # If name matching failed, try matching by address
    street_address = extract_street_address(address)
    if street_address:
        match, score = find_best_match_by_address(
            street_address, city, colorado_df,
            business_name=business_name,
            zip_code=zip_code
        )
        if match is not None:
            return match, score, 'address'
    
    return None, 0, ''


# =============================================================================
# ENRICHMENT FUNCTION
# =============================================================================

def enrich_with_agent_names(
    places_df: pd.DataFrame,
    progress_callback: callable = None
) -> pd.DataFrame:
    """
    Enrich a DataFrame of places with registered agent names from Colorado SOS data.
    
    Returns:
        DataFrame with added columns:
        - BusinessName: Official registered name
        - AgentName: Registered agent's name (person only)
        - EntityStatus: Business status (Good Standing, etc.)
        - MatchConfidence: Match score (0-100)
        - MatchType: 'name' or 'address'
        - NameSimilarity: How similar Google name is to BusinessName (0-100)
    """
    colorado_df = load_colorado_data()
    
    if colorado_df is None:
        logger.warning("Could not load Colorado data, skipping agent name enrichment")
        places_df = places_df.copy()
        places_df['BusinessName'] = ''
        places_df['AgentName'] = ''
        places_df['EntityStatus'] = ''
        places_df['MatchConfidence'] = 0.0
        places_df['MatchType'] = ''
        places_df['NameSimilarity'] = 0.0
        return places_df
    
    # Initialize new columns
    places_df = places_df.copy()
    places_df['BusinessName'] = ''
    places_df['AgentName'] = ''
    places_df['EntityStatus'] = ''
    places_df['MatchConfidence'] = 0.0
    places_df['MatchType'] = ''
    places_df['NameSimilarity'] = 0.0
    
    total = len(places_df)
    matched_count = 0
    name_match_count = 0
    address_match_count = 0
    
    logger.info(f"Matching {total} businesses against Colorado database...")
    
    for idx, row in places_df.iterrows():
        business_name = row.get('Name', '')
        address = row.get('Address', '')
        
        city = extract_city_from_address(address)
        
        match, score, match_type = find_best_match(business_name, address, city, colorado_df)
        
        if match is not None:
            # Get the official business name
            entity_name = match.get('entityname', '')
            if entity_name and pd.notna(entity_name) and str(entity_name).lower() not in ('nan', 'null', 'none', ''):
                places_df.at[idx, 'BusinessName'] = str(entity_name).strip()
                
                # Calculate name similarity using normalized names
                google_name_norm = normalize_business_name(business_name)
                entity_name_norm = match.get('entityname_norm', '')
                if google_name_norm and entity_name_norm:
                    name_similarity = fuzz.WRatio(google_name_norm, entity_name_norm)
                    places_df.at[idx, 'NameSimilarity'] = float(name_similarity)
            
            # Get entity status (use display version)
            entity_status = match.get('entitystatus_display', '')
            if entity_status:
                places_df.at[idx, 'EntityStatus'] = entity_status
            
            agent_name = build_agent_name(match)
            places_df.at[idx, 'AgentName'] = agent_name
            places_df.at[idx, 'MatchConfidence'] = float(score)
            places_df.at[idx, 'MatchType'] = match_type
            
            matched_count += 1
            if match_type == 'name':
                name_match_count += 1
            elif match_type == 'address':
                address_match_count += 1
        
        if progress_callback:
            progress_callback(idx + 1, total)
    
    # Log summary
    unmatched = total - matched_count
    match_rate = (matched_count / total * 100) if total > 0 else 0
    
    logger.info(f"Agent name matching complete:")
    logger.info(f"  - Total matched: {matched_count} ({match_rate:.1f}%)")
    logger.info(f"    - By name: {name_match_count}")
    logger.info(f"    - By address: {address_match_count}")
    logger.info(f"  - Unmatched: {unmatched}")
    
    return places_df


# =============================================================================
# STATUS FUNCTION
# =============================================================================

def get_colorado_data_status() -> dict:
    """Get status information about the Colorado data cache."""
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