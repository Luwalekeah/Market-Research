"""
Module for extracting email addresses from business websites.

Note: Google Places API does not provide email addresses directly.
This module extracts emails by scraping business websites found via the API.
"""
import re
import requests
from typing import Optional
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import EMAIL_EXTRACTION_TIMEOUT, MAX_EMAILS_PER_WEBSITE


# Common email pattern
EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
    re.IGNORECASE
)

# Patterns to exclude (common false positives)
EXCLUDE_PATTERNS = [
    r'.*@example\.com',
    r'.*@domain\.com',
    r'.*@email\.com',
    r'.*@yourdomain\.com',
    r'.*\.png$',
    r'.*\.jpg$',
    r'.*\.gif$',
    r'.*\.css$',
    r'.*\.js$',
    r'.*wixpress\.com',
    r'.*sentry\.io',
]

# Common contact page paths to check
CONTACT_PATHS = [
    '/contact',
    '/contact-us',
    '/about',
    '/about-us',
    '/contactus',
    '/reach-us',
    '/get-in-touch',
]

# Request headers to mimic browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}


def is_valid_email(email: str) -> bool:
    """
    Check if an email address appears valid and isn't a false positive.
    
    Args:
        email: Email address string
    
    Returns:
        True if email appears valid
    """
    email = email.lower().strip()
    
    # Check against exclude patterns
    for pattern in EXCLUDE_PATTERNS:
        if re.match(pattern, email, re.IGNORECASE):
            return False
    
    # Basic validation
    if len(email) > 254:
        return False
    
    # Must have valid TLD
    parts = email.split('.')
    if len(parts[-1]) < 2:
        return False
    
    return True


def extract_emails_from_text(text: str) -> list:
    """
    Extract email addresses from text content.
    
    Args:
        text: Text to search for emails
    
    Returns:
        List of unique valid email addresses
    """
    if not text:
        return []
    
    # Find all potential emails
    matches = EMAIL_PATTERN.findall(text)
    
    # Filter and deduplicate
    valid_emails = []
    seen = set()
    
    for email in matches:
        email = email.lower().strip()
        if email not in seen and is_valid_email(email):
            valid_emails.append(email)
            seen.add(email)
            
            if len(valid_emails) >= MAX_EMAILS_PER_WEBSITE:
                break
    
    return valid_emails


def fetch_page_content(url: str, timeout: int = EMAIL_EXTRACTION_TIMEOUT) -> Optional[str]:
    """
    Fetch the HTML content of a webpage.
    
    Args:
        url: URL to fetch
        timeout: Request timeout in seconds
    
    Returns:
        HTML content as string, or None if failed
    """
    try:
        response = requests.get(
            url,
            headers=HEADERS,
            timeout=timeout,
            allow_redirects=True,
            verify=True
        )
        response.raise_for_status()
        return response.text
    except Exception:
        return None


def extract_email_from_website(website_url: str) -> list:
    """
    Extract email addresses from a business website.
    
    Checks the main page and common contact page paths.
    
    Args:
        website_url: The business website URL
    
    Returns:
        List of found email addresses
    """
    if not website_url:
        return []
    
    # Ensure URL has scheme
    if not website_url.startswith(('http://', 'https://')):
        website_url = 'https://' + website_url
    
    all_emails = set()
    
    # Check main page
    content = fetch_page_content(website_url)
    if content:
        emails = extract_emails_from_text(content)
        all_emails.update(emails)
    
    # If we found emails on main page, might be enough
    if len(all_emails) >= MAX_EMAILS_PER_WEBSITE:
        return list(all_emails)[:MAX_EMAILS_PER_WEBSITE]
    
    # Check contact pages
    parsed = urlparse(website_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    
    for path in CONTACT_PATHS:
        if len(all_emails) >= MAX_EMAILS_PER_WEBSITE:
            break
        
        contact_url = urljoin(base_url, path)
        content = fetch_page_content(contact_url)
        if content:
            emails = extract_emails_from_text(content)
            all_emails.update(emails)
    
    return list(all_emails)[:MAX_EMAILS_PER_WEBSITE]


def enrich_places_with_emails(
    places: list,
    max_workers: int = 5,
    progress_callback: callable = None
) -> list:
    """
    Enrich a list of places with email addresses extracted from their websites.
    
    Args:
        places: List of place dictionaries (must have 'website' key)
        max_workers: Number of concurrent workers for extraction
        progress_callback: Optional callback(completed, total) for progress
    
    Returns:
        Updated list of places with 'email' field populated
    """
    # Filter places with websites
    places_with_websites = [
        (i, p) for i, p in enumerate(places) 
        if p.get('website')
    ]
    
    total = len(places_with_websites)
    completed = 0
    
    if total == 0:
        return places
    
    def extract_for_place(index_place):
        idx, place = index_place
        emails = extract_email_from_website(place['website'])
        return idx, emails
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(extract_for_place, ip): ip 
            for ip in places_with_websites
        }
        
        for future in as_completed(futures):
            try:
                idx, emails = future.result()
                if emails:
                    places[idx]['email'] = '; '.join(emails)
            except Exception:
                pass
            
            completed += 1
            if progress_callback:
                progress_callback(completed, total)
    
    return places


def extract_single_email(website_url: str) -> str:
    """
    Extract a single email from a website (returns first found).
    
    Args:
        website_url: The website URL to check
    
    Returns:
        Email address string or empty string
    """
    emails = extract_email_from_website(website_url)
    return emails[0] if emails else ''
