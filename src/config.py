"""
Configuration and constants for the Market Research App.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API Configuration
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# Path Settings
BASE_DIR = Path(__file__).parent.parent
STYLES_DIR = BASE_DIR / "styles"
DATA_DIR = BASE_DIR / "data"

# App Settings
PAGE_TITLE = "Find Nearby Places"
PAGE_ICON = "📍"

# Search Defaults - Reads from .env with fallbacks
DEFAULT_LOCATION = os.getenv("DEFAULT_LOCATION", "Denver")
DEFAULT_PLACE_TYPE = os.getenv("DEFAULT_PLACE_TYPE", "gas")

# Handle distance as float (env vars are strings)
_default_distance = os.getenv("DEFAULT_DISTANCE_MILES", "10.0")
try:
    DEFAULT_DISTANCE_MILES = float(_default_distance)
except ValueError:
    DEFAULT_DISTANCE_MILES = 10.0

MAX_DISTANCE_MILES = 50.0

# API Rate Limiting
API_REQUEST_DELAY = 2  # seconds between paginated requests

# Email Extraction Settings
EMAIL_EXTRACTION_TIMEOUT = int(os.getenv("EMAIL_EXTRACTION_TIMEOUT", "10"))
MAX_EMAILS_PER_WEBSITE = int(os.getenv("MAX_EMAILS_PER_WEBSITE", "5"))