"""
Market Research App - Find nearby places and extract business contact information.

This package provides tools for:
- Searching for places using the Google Maps API
- Extracting email addresses from business websites
- Exporting data to Excel/CSV
- Visualizing results on interactive maps
"""

from .config import (
    GOOGLE_MAPS_API_KEY,
    DEFAULT_LOCATION,
    DEFAULT_PLACE_TYPE,
    DEFAULT_DISTANCE_MILES,
)

from .places import (
    search_places,
    search_places_nearby,
    get_place_details,
    calculate_distance,
    geocode_location,
)

from .email_extractor import (
    extract_email_from_website,
    enrich_places_with_emails,
    extract_single_email,
)

from .data_utils import (
    places_to_dataframe,
    clean_dataframe,
    export_to_excel,
    export_to_csv,
    get_summary_stats,
)

from .mapping import (
    create_places_map,
    generate_google_maps_link,
    generate_single_maps_link,
)

__version__ = '1.0.0'
__author__ = 'Luwalekeah'

__all__ = [
    # Config
    'GOOGLE_MAPS_API_KEY',
    'DEFAULT_LOCATION',
    'DEFAULT_PLACE_TYPE',
    'DEFAULT_DISTANCE_MILES',
    # Places
    'search_places',
    'search_places_nearby',
    'get_place_details',
    'calculate_distance',
    'geocode_location',
    # Email
    'extract_email_from_website',
    'enrich_places_with_emails',
    'extract_single_email',
    # Data
    'places_to_dataframe',
    'clean_dataframe',
    'export_to_excel',
    'export_to_csv',
    'get_summary_stats',
    # Mapping
    'create_places_map',
    'generate_google_maps_link',
    'generate_single_maps_link',
]
