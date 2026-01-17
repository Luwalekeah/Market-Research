"""
Module for extracting email addresses from business websites.

Extraction Strategies:
1. Deep Crawl: Homepage + discovered contact/about/team pages
2. WHOIS Lookup: Domain registration data as fallback
3. Regex & Obfuscation: Standard, obfuscated ([at]/[dot]), and mailto patterns
4. Search Dorking: Google search for publicly indexed emails (optional)

Additional techniques:
- JSON-LD structured data parsing
- Footer section extraction
- Link discovery for contact-related pages
"""
import re
import json
import time
import logging
import requests
from typing import Optional, Tuple, List, Set
from urllib.parse import urlparse, urljoin, quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

from .config import EMAIL_EXTRACTION_TIMEOUT, MAX_EMAILS_PER_WEBSITE

# Configure logging
logger = logging.getLogger(__name__)

# =============================================================================
# REGEX PATTERNS
# =============================================================================

# Pattern 1: Standard email pattern
EMAIL_PATTERN_STANDARD = re.compile(
    r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
    re.IGNORECASE
)

# Pattern 2: Obfuscated email patterns (e.g., "john [at] example [dot] com")
EMAIL_PATTERN_OBFUSCATED = re.compile(
    r'([a-zA-Z0-9._%+-]+)\s*[\[\(]?\s*(?:at|AT|@)\s*[\]\)]?\s*([a-zA-Z0-9.-]+)\s*[\[\(]?\s*(?:dot|DOT|\.)\s*[\]\)]?\s*([a-zA-Z]{2,})',
    re.IGNORECASE
)

# Pattern 3: Mailto link pattern
MAILTO_PATTERN = re.compile(
    r'href=["\']mailto:([^"\'?]+)',
    re.IGNORECASE
)

# Pattern 4: HTML encoded @ symbol (&#64; or &#x40;)
EMAIL_PATTERN_HTML_ENCODED = re.compile(
    r'([a-zA-Z0-9._%+-]+)(?:&#64;|&#x40;)([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
    re.IGNORECASE
)

# Pattern 5: JavaScript escaped emails
EMAIL_PATTERN_JS_ESCAPED = re.compile(
    r'([a-zA-Z0-9._%+-]+)\\x40([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
    re.IGNORECASE
)

# JSON-LD pattern
JSONLD_PATTERN = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL
)

# Footer pattern
FOOTER_PATTERN = re.compile(
    r'<footer[^>]*>(.*?)</footer>',
    re.IGNORECASE | re.DOTALL
)

# Link discovery patterns for contact-related pages
CONTACT_LINK_KEYWORDS = [
    'contact', 'about', 'team', 'staff', 'legal', 'support',
    'help', 'reach', 'connect', 'company', 'info', 'impressum',
    'kontakt', 'über-uns', 'equipo'  # German/Spanish
]

# =============================================================================
# EXCLUSION PATTERNS
# =============================================================================

# Patterns to exclude (common false positives)
EXCLUDE_PATTERNS = [
    r'.*@example\.com$',
    r'.*@domain\.com$',
    r'.*@email\.com$',
    r'.*@yourdomain\.com$',
    r'.*@yourcompany\.com$',
    r'.*@yoursite\.com$',
    r'.*@company\.com$',
    r'.*@test\.com$',
    r'.*@localhost.*',
    r'.*@sentry\.io$',
    r'.*@cloudflare\.com$',
    r'.*@googleapis\.com$',
    r'.*@schema\.org$',
    r'.*@w3\.org$',
    r'.*@wordpress\.com$',
    r'.*@mailchimp\.com$',
    r'.*@constantcontact\.com$',
    r'.*@hubspot\.com$',
    # Wix tracking/error emails
    r'.*@sentry\.wixpress\.com$',
    r'.*@sentry-next\.wixpress\.com$',
    r'.*@wixpress\.com$',
    r'.*wixpress\.com.*',
    # WHOIS privacy/anonymization services
    r'.*@anonymised\.email$',
    r'.*@domaindiscreet\.com$',
    r'.*@whoisprotection\..*',
    r'.*@privacyprotect\..*',
    r'.*@contactprivacy\..*',
    r'.*@whoisprivacyprotect\..*',
    r'.*-registrant@anonymised\..*',
    # Abuse/admin emails
    r'^abuse[@-].*',
    r'^abuse-complaints@.*',
    r'^hostmaster@.*',
    r'^postmaster@.*',
    r'^webmaster@.*',
    r'^admin@.*',
    r'^administrator@.*',
    r'^noc@.*',
    r'^root@.*',
    # Legal/privacy/compliance emails (not useful for business contact)
    r'^legal@.*',
    r'^privacy@.*',
    r'^compliance@.*',
    r'^gdpr@.*',
    r'^dpo@.*',  # Data Protection Officer
    r'^dmca@.*',
    r'^copyright@.*',
    r'^terms@.*',
    r'^tos@.*',
    r'.*privacy.*@.*',  # Catches californiaprivacyrights@, privacyrequest@, etc.
    r'.*legal.*@.*',    # Catches legalteam@, legaldept@, etc.
    r'.*compliance.*@.*',
    r'.*optout.*@.*',
    r'.*unsubscribe.*@.*',
    r'.*donotcontact.*@.*',
    # Domain/hosting operations emails
    r'^domain\..*@.*',  # domain.operations@, domain.admin@, etc.
    r'.*\.operations@.*',  # Catches domain.operations@web.com
    r'.*@web\.com$',  # web.com is a registrar
    r'^domains@.*',
    r'^dns@.*',
    r'^hosting@.*',
    r'^technical@.*',
    r'^tech@.*',
    r'^it@.*',
    r'^operations@.*',
    r'^billing@.*',
    r'^accounts@.*',
    r'^accounting@.*',
    # AWS and cloud services
    r'.*amazonaws\.com$',
    r'.*s3-acceler.*',
    r'.*cloudfront\.net$',
    r'.*azurewebsites\.net$',
    # Registrar/hosting
    r'.*@squarespace\.com$',
    r'.*@wildwestdomains\.com$',
    r'.*@godaddy\.com$',
    r'.*@namecheap\.com$',
    r'.*@networksolutions\.com$',
    # Generic bounce addresses
    r'noreply@.*',
    r'no-reply@.*',
    r'donotreply@.*',
    r'mailer-daemon@.*',
    r'bounce@.*',
    # File extensions (from image URLs)
    r'.*\.png$',
    r'.*\.jpg$',
    r'.*\.jpeg$',
    r'.*\.gif$',
    r'.*\.svg$',
    r'.*\.css$',
    r'.*\.js$',
    r'.*\.webp$',
    r'.*\.pdf$',
    # CSS/JS artifacts
    r'.*@.*-image\..*',
    r'.*@.*-uploads\..*',
    r'.*\.has-.*@.*',
    r'body\..*@.*',
    r'.*@e\.[a-z]+$',  # like @e.amazonaws
]

# JavaScript/CSS false positive patterns (domains that look like code)
JS_CSS_FAKE_DOMAINS = [
    'ion.replace', 'ion.primary', 'ion.post', 'ion.start',
    'h.abs', 'c.com', 'ic.com', 'ic.cookie', 'io.start',
    'gst', 'fonts', 'customiz',
]

# Common fake local parts from JS/CSS
JS_CSS_FAKE_LOCAL_PARTS = [
    'window', 'document', 'location', 'navigator',
    'post', 'header', 'footer', 'primary', 'secondary',
    'fonts', 'style', 'script', 'config', 'settings',
    'st', 'gt', 'lt', 'eq', 'ne',  # Comparison operators
    'm', 'n', 'p', 'q', 'r', 's', 't', 'x', 'y', 'z',  # Single letters
]

# WHOIS privacy service patterns to skip
WHOIS_PRIVACY_PATTERNS = [
    # Privacy services
    'privacy',
    'proxy', 
    'protect',
    'redacted',
    'whoisguard',
    'privacyguardian',
    'domainsbyproxy',
    'contactprivacy',
    'withheld',
    'gdpr',
    'data protected',
    'redacted for privacy',
    'not disclosed',
    'identity protect',
    # Abuse emails (registrar contacts, not business contacts)
    'abuse@',
    'abuse-',
    'hostmaster@',
    'postmaster@',
    'webmaster@',
    'admin@',  # Often generic
    'administrator@',
    'root@',
    'support@',  # Often generic support desk
    'noc@',
    'security@',
    # Common registrar/hosting domains (not business emails)
    'squarespace.com',
    'wildwestdomains.com',
    'godaddy.com',
    'namecheap.com',
    'networksolutions.com',
    'register.com',
    'enom.com',
    'tucows.com',
    'publicdomainregistry.com',
    'name.com',
    'dynadot.com',
    'hover.com',
    'gandi.net',
    'cloudflare.com',
    'amazonaws.com',
    'googledomains.com',
    'domains.google',
    'hostgator.com',
    'bluehost.com',
    'dreamhost.com',
    'siteground.com',
    'ionos.com',
    '1and1.com',
    'ovh.com',
    'ovhcloud.com',
    'hostinger.com',
    'wpengine.com',
    'shopify.com',
    'wix.com',
    'weebly.com',
]

# Expanded list of contact page paths to check
CONTACT_PATHS = [
    '/contact',
    '/contact-us',
    '/contactus',
    '/contact.html',
    '/about',
    '/about-us',
    '/aboutus',
    '/about.html',
    '/team',
    '/our-team',
    '/meet-the-team',
    '/staff',
    '/people',
    '/connect',
    '/support',
    '/help',
    '/customer-service',
    '/reach-us',
    '/get-in-touch',
    '/info',
    '/information',
    '/company',
    '/corporate',
    '/location',
    '/locations',
    '/legal',
    '/imprint',
    '/impressum',
    '/privacy',
    '/privacy-policy',
]

# Request headers to mimic browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

# =============================================================================
# VALID TLD WHITELIST (most common real TLDs)
# =============================================================================
VALID_TLDS = {
    # Generic TLDs
    'com', 'org', 'net', 'edu', 'gov', 'mil', 'int',
    # Common new TLDs
    'io', 'co', 'ai', 'app', 'dev', 'tech', 'online', 'site', 'web',
    'biz', 'info', 'name', 'pro', 'mobi', 'tel', 'jobs', 'travel',
    'shop', 'store', 'blog', 'news', 'media', 'email', 'cloud',
    # Country codes (common ones)
    'us', 'uk', 'ca', 'au', 'de', 'fr', 'es', 'it', 'nl', 'be', 'ch',
    'at', 'pl', 'cz', 'se', 'no', 'dk', 'fi', 'ie', 'pt', 'gr',
    'jp', 'cn', 'kr', 'in', 'sg', 'hk', 'tw', 'th', 'my', 'ph',
    'nz', 'za', 'mx', 'br', 'ar', 'cl', 'co', 'pe',
    'ru', 'ua', 'il', 'ae', 'sa',
}

# Known fake/junk domains from JS/CSS
FAKE_DOMAIN_PATTERNS = [
    r'^[a-z]{1,2}\.[a-z]{2,4}$',  # like "ar.com", "ed.if", "h.io"
    r'^(ion|or|and|if|for|var|let|const|nav|doc)\.',  # JS keywords
    r'\.(abs|rel|fixed|static|block|none|auto|replace|remove|add)$',  # CSS/JS methods
    r'^(window|document|location|navigator|console|element)\.',  # JS objects
    r'(present|please|click|here|button|submit|form|input)\.',  # UI elements
    r'^(grav|loc|sec|nav|btn|img|div|span)\.',  # Short abbreviations
]

# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================

def is_valid_email(email: str) -> bool:
    """
    Check if an email address appears valid and isn't a false positive.
    Uses strict validation including TLD whitelist and fake domain detection.
    """
    if not email:
        return False
        
    email = email.lower().strip()
    
    # Check against exclude patterns
    for pattern in EXCLUDE_PATTERNS:
        if re.match(pattern, email, re.IGNORECASE):
            return False
    
    # Minimum length check (real emails are rarely < 10 chars like "a@bc.com")
    if len(email) > 254 or len(email) < 10:
        return False
    
    # Must have exactly one @
    if email.count('@') != 1:
        return False
    
    local, domain = email.rsplit('@', 1)
    if not local or not domain or '.' not in domain:
        return False
    
    # =================================================================
    # STRICT LOCAL PART VALIDATION
    # =================================================================
    
    # Local part length check
    if len(local) < 3 or len(local) > 64:
        return False
    
    # Local part should start with a letter
    if not local[0].isalpha():
        return False
    
    # Local part should end with letter or digit
    if not local[-1].isalnum():
        return False
    
    # Check for JS/CSS fake local parts
    if local in JS_CSS_FAKE_LOCAL_PARTS:
        return False
    
    # Local part shouldn't be common code words
    code_words = {'secure', 'grav', 'loc', 'nav', 'btn', 'img', 'div', 'span', 
                  'var', 'let', 'const', 'func', 'data', 'item', 'elem', 'node',
                  'address', 'present', 'please', 'click', 'submit', 'form',
                  'cre', 'www', 'http', 'https', 'ftp'}  # Added more junk
    if local in code_words:
        return False
    
    # Local part shouldn't start with digits
    if re.match(r'^\d', local):
        return False
    
    # Local part shouldn't start with www (like www.yelpreserv)
    if local.startswith('www'):
        return False
    
    # Local part shouldn't be too short (like "cre")
    if len(local) < 4:
        return False
    
    # =================================================================
    # STRICT DOMAIN VALIDATION
    # =================================================================
    
    # Check against fake domain patterns
    for pattern in FAKE_DOMAIN_PATTERNS:
        if re.match(pattern, domain):
            return False
    
    # Domain parts validation
    parts = domain.split('.')
    
    # Must have at least 2 parts
    if len(parts) < 2:
        return False
    
    # Get TLD
    tld = parts[-1]
    
    # TLD must be in whitelist
    if tld not in VALID_TLDS:
        return False
    
    # Second level domain validation
    sld = parts[-2]
    
    # SLD must be at least 3 chars (filters "ar.com", "ed.if", etc.)
    if len(sld) < 3:
        return False
    
    # SLD shouldn't be "ors" or other junk (like cre@ors.yahoo.com)
    junk_slds = {'ors', 'ions', 'ons', 'ers', 'ing', 'tion', 'sion'}
    if sld in junk_slds:
        return False
    
    # SLD should be mostly letters
    if not re.match(r'^[a-z][a-z0-9-]*[a-z0-9]$', sld) and len(sld) > 2:
        return False
    
    # Domain shouldn't have consecutive dots or hyphens
    if '..' in domain or '--' in domain:
        return False
    
    # Full domain (without TLD) should be reasonable length
    domain_without_tld = '.'.join(parts[:-1])
    if len(domain_without_tld) < 3:
        return False
    
    # =================================================================
    # FINAL SANITY CHECKS
    # =================================================================
    
    # Email should have reasonable character distribution
    # Real emails have more letters than special chars
    letter_count = sum(1 for c in email if c.isalpha())
    if letter_count < len(email) * 0.5:
        return False
    
    # Reject if local part looks like it came from minified JS
    if re.match(r'^[a-z]{1,2}\.[a-z]{1,4}$', local):
        return False
    
    return True


def is_whois_privacy_email(email: str) -> bool:
    """Check if an email appears to be a WHOIS privacy service."""
    if not email:
        return True
        
    email_lower = email.lower()
    
    for pattern in WHOIS_PRIVACY_PATTERNS:
        if pattern in email_lower:
            return True
    
    return False


# Common contact email prefixes - if found, trim anything before them
COMMON_EMAIL_PREFIXES = [
    'info@',
    'contact@',
    'hello@',
    'hi@',
    'hey@',
    'mail@',
    'email@',
    'enquiries@',
    'inquiries@',
    'enquiry@',
    'inquiry@',
    'sales@',
    'support@',
    'help@',
    'service@',
    'customerservice@',
    'office@',
    'general@',
    'team@',
    'booking@',
    'bookings@',
    'reservations@',
    'events@',
    'orders@',
    'feedback@',
    'press@',
    'media@',
    'marketing@',
    'careers@',
    'jobs@',
    'hr@',
]


def clean_email(email: str) -> str:
    """
    Clean an email address by trimming junk characters before common prefixes.
    
    Example: "955-10335info@milieufc.com" -> "info@milieufc.com"
    
    Args:
        email: Raw email string that may have junk prepended
    
    Returns:
        Cleaned email string
    """
    if not email:
        return ""
    
    email = email.lower().strip()
    
    # Check if email contains a common prefix with junk before it
    for prefix in COMMON_EMAIL_PREFIXES:
        if prefix in email:
            # Find position of the common prefix
            pos = email.find(prefix)
            if pos > 0:
                # There's something before the prefix - trim it
                email = email[pos:]
                break
    
    return email


# =============================================================================
# EXTRACTION FUNCTIONS
# =============================================================================

def extract_emails_standard(text: str) -> List[str]:
    """Extract emails using standard regex pattern."""
    if not text:
        return []
    
    matches = EMAIL_PATTERN_STANDARD.findall(text)
    cleaned_emails = []
    for email in matches:
        cleaned = clean_email(email)
        if is_valid_email(cleaned):
            cleaned_emails.append(cleaned)
    return cleaned_emails


def extract_emails_obfuscated(text: str) -> List[str]:
    """
    Extract obfuscated emails like "john [at] example [dot] com".
    """
    if not text:
        return []
    
    emails = []
    matches = EMAIL_PATTERN_OBFUSCATED.findall(text)
    
    for match in matches:
        if len(match) == 3:
            email = f"{match[0]}@{match[1]}.{match[2]}".lower().strip()
            cleaned = clean_email(email)
            if is_valid_email(cleaned):
                emails.append(cleaned)
    
    return emails


def extract_emails_html_encoded(text: str) -> List[str]:
    """Extract emails with HTML-encoded @ symbols."""
    if not text:
        return []
    
    emails = []
    matches = EMAIL_PATTERN_HTML_ENCODED.findall(text)
    
    for match in matches:
        if len(match) == 2:
            email = f"{match[0]}@{match[1]}".lower().strip()
            cleaned = clean_email(email)
            if is_valid_email(cleaned):
                emails.append(cleaned)
    
    return emails


def extract_mailto_emails(html: str) -> List[str]:
    """Extract emails from mailto: links in HTML."""
    if not html:
        return []
    
    matches = MAILTO_PATTERN.findall(html)
    emails = []
    
    for email in matches:
        # Handle URL encoding
        email = requests.utils.unquote(email)
        cleaned = clean_email(email)
        if is_valid_email(cleaned):
            emails.append(cleaned)
    
    return emails


def extract_footer_emails(html: str) -> List[str]:
    """Extract emails specifically from footer sections."""
    if not html:
        return []
    
    footer_matches = FOOTER_PATTERN.findall(html)
    all_emails = []
    
    for footer_content in footer_matches:
        # Check mailto links in footer first (most reliable)
        mailto_emails = extract_mailto_emails(footer_content)
        all_emails.extend(mailto_emails)
        
        # Then check standard pattern
        standard_emails = extract_emails_standard(footer_content)
        for email in standard_emails:
            if email not in all_emails:
                all_emails.append(email)
        
        # Check obfuscated patterns
        obfuscated_emails = extract_emails_obfuscated(footer_content)
        for email in obfuscated_emails:
            if email not in all_emails:
                all_emails.append(email)
    
    return all_emails


def extract_jsonld_emails(html: str) -> List[str]:
    """Extract emails from JSON-LD structured data."""
    if not html:
        return []
    
    emails = []
    jsonld_matches = JSONLD_PATTERN.findall(html)
    
    for jsonld_str in jsonld_matches:
        try:
            data = json.loads(jsonld_str)
            
            # Handle both single objects and arrays
            items = data if isinstance(data, list) else [data]
            
            for item in items:
                if not isinstance(item, dict):
                    continue
                    
                # Look for email field
                email = item.get('email', '')
                if email and is_valid_email(email):
                    emails.append(email.lower())
                
                # Check contactPoint
                contact_points = item.get('contactPoint', [])
                if isinstance(contact_points, dict):
                    contact_points = [contact_points]
                
                for cp in contact_points:
                    if isinstance(cp, dict):
                        cp_email = cp.get('email', '')
                        if cp_email and is_valid_email(cp_email):
                            emails.append(cp_email.lower())
                
                # Check author
                author = item.get('author', {})
                if isinstance(author, dict):
                    author_email = author.get('email', '')
                    if author_email and is_valid_email(author_email):
                        emails.append(author_email.lower())
                
        except (json.JSONDecodeError, TypeError, AttributeError):
            continue
    
    return list(set(emails))


def extract_all_emails_from_html(html: str) -> Tuple[List[str], str]:
    """
    Extract emails from HTML using all available methods.
    Returns (emails, source) where source indicates extraction method.
    """
    if not html:
        return [], ''
    
    all_emails = set()
    source = ''
    
    # Priority 1: Mailto links (most reliable, intentionally public)
    mailto_emails = extract_mailto_emails(html)
    if mailto_emails:
        all_emails.update(mailto_emails)
        source = 'mailto'
    
    # Priority 2: JSON-LD structured data
    if not all_emails:
        jsonld_emails = extract_jsonld_emails(html)
        if jsonld_emails:
            all_emails.update(jsonld_emails)
            source = 'jsonld'
    
    # Priority 3: Footer section
    if not all_emails:
        footer_emails = extract_footer_emails(html)
        if footer_emails:
            all_emails.update(footer_emails)
            source = 'footer'
    
    # Priority 4: Standard regex on full page
    if not all_emails:
        standard_emails = extract_emails_standard(html)
        if standard_emails:
            all_emails.update(standard_emails)
            source = 'page'
    
    # Priority 5: Obfuscated patterns
    if not all_emails:
        obfuscated_emails = extract_emails_obfuscated(html)
        if obfuscated_emails:
            all_emails.update(obfuscated_emails)
            source = 'obfuscated'
    
    # Priority 6: HTML encoded
    if not all_emails:
        encoded_emails = extract_emails_html_encoded(html)
        if encoded_emails:
            all_emails.update(encoded_emails)
            source = 'encoded'
    
    return list(all_emails), source


# =============================================================================
# LINK DISCOVERY
# =============================================================================

def discover_contact_links(html: str, base_url: str) -> List[str]:
    """
    Discover links to contact/about/team pages by analyzing anchor tags.
    """
    if not html or not base_url:
        return []
    
    discovered_urls = set()
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            link_text = link.get_text().lower().strip()
            
            # Check if link text or href contains contact-related keywords
            is_contact_link = False
            
            for keyword in CONTACT_LINK_KEYWORDS:
                if keyword in href.lower() or keyword in link_text:
                    is_contact_link = True
                    break
            
            if is_contact_link:
                # Convert to absolute URL
                full_url = urljoin(base_url, href)
                
                # Only add if same domain
                if urlparse(full_url).netloc == urlparse(base_url).netloc:
                    discovered_urls.add(full_url)
    
    except Exception as e:
        logger.debug(f"Link discovery error: {e}")
    
    return list(discovered_urls)[:10]  # Limit to 10 discovered links


# =============================================================================
# PAGE FETCHING
# =============================================================================

def fetch_page_content(url: str, timeout: int = EMAIL_EXTRACTION_TIMEOUT) -> Optional[str]:
    """Fetch the HTML content of a webpage."""
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
    except Exception as e:
        logger.debug(f"Failed to fetch {url}: {e}")
        return None


def extract_domain(url: str) -> str:
    """Extract the domain name from a URL."""
    if not url:
        return ""
    
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        
        if domain.startswith('www.'):
            domain = domain[4:]
        
        if ':' in domain:
            domain = domain.split(':')[0]
        
        return domain
    except Exception:
        return ""


# =============================================================================
# WHOIS LOOKUP
# =============================================================================

def whois_lookup_email(domain: str) -> Optional[str]:
    """
    Perform WHOIS lookup to find email addresses.
    Skips privacy-protected emails.
    """
    if not domain:
        return None
    
    try:
        # Suppress noisy WHOIS library logging
        logging.getLogger('whois').setLevel(logging.CRITICAL)
        logging.getLogger('whois.whois').setLevel(logging.CRITICAL)
        
        import whois
        
        # Rate limiting
        time.sleep(1.5)
        
        w = whois.whois(domain)
        
        if not w:
            return None
        
        # Check various email fields in WHOIS data
        email_fields = ['emails', 'registrant_email', 'admin_email', 'tech_email']
        
        for field in email_fields:
            value = getattr(w, field, None)
            
            if not value:
                continue
            
            # Handle list of emails
            if isinstance(value, list):
                for email in value:
                    if email and is_valid_email(email) and not is_whois_privacy_email(email):
                        return email.lower()
            else:
                if is_valid_email(value) and not is_whois_privacy_email(value):
                    return value.lower()
        
        return None
        
    except ImportError:
        logger.warning("python-whois not installed. Run: pip install python-whois")
        return None
    except Exception:
        # Silently fail - WHOIS is just a fallback
        return None


# =============================================================================
# SEARCH DORKING (Optional - requires API key)
# =============================================================================

def search_dork_email(domain: str, api_key: str = None) -> Optional[str]:
    """
    Use search engine to find publicly indexed emails.
    Query format: site:domain.com "@domain.com"
    
    Note: Requires Google Custom Search API key or similar.
    This is a placeholder for the technique.
    """
    if not domain:
        return None
    
    # This would require a Google Custom Search API key
    # For now, return None - can be implemented with proper API access
    logger.debug(f"Search dorking not implemented for {domain}")
    return None


# =============================================================================
# MAIN EXTRACTION FUNCTION
# =============================================================================

def extract_email_from_website(
    website_url: str, 
    use_whois: bool = True,
    deep_crawl: bool = True
) -> Tuple[List[str], str]:
    """
    Extract email addresses from a business website using multiple strategies.
    
    Strategies (in order):
    1. Homepage scan (mailto, JSON-LD, footer, standard, obfuscated)
    2. Static contact pages (/contact, /about, etc.)
    3. Discovered contact links (dynamic link discovery)
    4. WHOIS lookup (fallback)
    
    Args:
        website_url: The business website URL
        use_whois: Whether to use WHOIS as fallback
        deep_crawl: Whether to discover and crawl additional contact pages
    
    Returns:
        Tuple of (list of emails, source string)
    """
    if not website_url:
        return [], ''
    
    # Ensure URL has scheme
    if not website_url.startswith(('http://', 'https://')):
        website_url = 'https://' + website_url
    
    all_emails = set()
    source = ''
    discovered_links = []
    
    # Step 1: Check homepage
    content = fetch_page_content(website_url)
    
    if content:
        emails, src = extract_all_emails_from_html(content)
        if emails:
            all_emails.update(emails)
            source = src
        
        # Discover additional contact links for deep crawl
        if deep_crawl and not all_emails:
            discovered_links = discover_contact_links(content, website_url)
    
    if all_emails:
        return list(all_emails)[:MAX_EMAILS_PER_WEBSITE], source
    
    # Step 2: Check static contact paths
    parsed = urlparse(website_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    
    for path in CONTACT_PATHS:
        if all_emails:
            break
        
        contact_url = urljoin(base_url, path)
        content = fetch_page_content(contact_url)
        
        if content:
            emails, src = extract_all_emails_from_html(content)
            if emails:
                all_emails.update(emails)
                source = 'contact_page'
                break
    
    if all_emails:
        return list(all_emails)[:MAX_EMAILS_PER_WEBSITE], source
    
    # Step 3: Deep crawl discovered links
    if deep_crawl and discovered_links:
        for link_url in discovered_links:
            if all_emails:
                break
            
            # Skip if we already checked this path
            link_path = urlparse(link_url).path
            if any(link_path.rstrip('/') == p.rstrip('/') for p in CONTACT_PATHS):
                continue
            
            content = fetch_page_content(link_url)
            if content:
                emails, src = extract_all_emails_from_html(content)
                if emails:
                    all_emails.update(emails)
                    source = 'discovered_page'
                    break
    
    if all_emails:
        return list(all_emails)[:MAX_EMAILS_PER_WEBSITE], source
    
    # Step 4: WHOIS lookup as final fallback
    if use_whois:
        domain = extract_domain(website_url)
        if domain:
            whois_email = whois_lookup_email(domain)
            if whois_email:
                return [whois_email], 'whois'
    
    return [], ''


# =============================================================================
# BATCH PROCESSING
# =============================================================================

def enrich_places_with_emails(
    places: list,
    max_workers: int = 5,
    use_whois: bool = True,
    deep_crawl: bool = True,
    progress_callback: callable = None
) -> list:
    """
    Enrich a list of places with email addresses extracted from their websites.
    
    Args:
        places: List of place dictionaries (must have 'website' key)
        max_workers: Number of concurrent workers for extraction
        use_whois: Whether to use WHOIS lookup as fallback
        deep_crawl: Whether to discover and crawl additional contact pages
        progress_callback: Optional callback(completed, total) for progress
    
    Returns:
        Updated list of places with 'email' and 'email_source' fields populated
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
        emails, source = extract_email_from_website(
            place['website'], 
            use_whois=use_whois,
            deep_crawl=deep_crawl
        )
        return idx, emails, source
    
    # Use fewer workers to be respectful of rate limits
    effective_workers = min(max_workers, 3) if use_whois else max_workers
    
    with ThreadPoolExecutor(max_workers=effective_workers) as executor:
        futures = {
            executor.submit(extract_for_place, ip): ip 
            for ip in places_with_websites
        }
        
        for future in as_completed(futures):
            try:
                idx, emails, source = future.result()
                if emails:
                    places[idx]['email'] = '; '.join(emails)
                    places[idx]['email_source'] = source
                else:
                    places[idx]['email'] = ''
                    places[idx]['email_source'] = ''
            except Exception as e:
                logger.debug(f"Email extraction failed: {e}")
            
            completed += 1
            if progress_callback:
                progress_callback(completed, total)
    
    return places


def extract_single_email(website_url: str, use_whois: bool = True) -> Tuple[str, str]:
    """
    Extract a single email from a website (returns first found).
    
    Args:
        website_url: The website URL to check
        use_whois: Whether to use WHOIS as fallback
    
    Returns:
        Tuple of (email address, source) or ('', '')
    """
    emails, source = extract_email_from_website(website_url, use_whois=use_whois)
    if emails:
        return emails[0], source
    return '', ''


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_extraction_stats(places: list) -> dict:
    """
    Get statistics about email extraction results.
    """
    total = len(places)
    with_email = sum(1 for p in places if p.get('email'))
    
    # Count by source
    source_counts = {}
    for p in places:
        source = p.get('email_source', '')
        if source:
            source_counts[source] = source_counts.get(source, 0) + 1
    
    return {
        'total': total,
        'with_email': with_email,
        'without_email': total - with_email,
        'extraction_rate': round(with_email / total * 100, 1) if total > 0 else 0,
        'by_source': source_counts
    }