"""
Core module for searching and retrieving place data from Google Maps API.
"""
import time
import googlemaps
from typing import Optional
from geopy.distance import geodesic

from .config import API_REQUEST_DELAY


def calculate_distance(origin: tuple, destination: tuple) -> float:
    """
    Calculate distance between two coordinates in miles.
    
    Args:
        origin: Tuple of (latitude, longitude)
        destination: Tuple of (latitude, longitude)
    
    Returns:
        Distance in miles
    """
    return geodesic(origin, destination).miles


def geocode_location(gmaps_client: googlemaps.Client, location: str) -> dict:
    """
    Convert a location string to latitude/longitude coordinates.
    
    Args:
        gmaps_client: Initialized Google Maps client
        location: Address, city, or location string
    
    Returns:
        Dictionary with 'lat' and 'lng' keys
    
    Raises:
        ValueError: If location cannot be geocoded
    """
    geocode_result = gmaps_client.geocode(location)
    if not geocode_result:
        raise ValueError(f"Could not geocode location: {location}")
    return geocode_result[0]['geometry']['location']


def get_place_details(
    gmaps_client: googlemaps.Client, 
    place_id: str,
    fields: Optional[list] = None
) -> dict:
    """
    Get detailed information for a specific place.
    
    Args:
        gmaps_client: Initialized Google Maps client
        place_id: The Google Place ID
        fields: List of fields to retrieve (defaults to common fields)
    
    Returns:
        Dictionary of place details
    """
    if fields is None:
        fields = [
            'formatted_address',
            'formatted_phone_number',
            'international_phone_number',
            'opening_hours',
            'website',
            'rating',
            'user_ratings_total'
        ]
    
    try:
        result = gmaps_client.place(place_id=place_id, fields=fields)
        return result.get('result', {})
    except Exception as e:
        print(f"Error fetching details for place {place_id}: {e}")
        return {}


def search_places(
    api_key: str,
    location: str,
    distance_miles: float,
    place_types: list,
    fetch_details: bool = True,
    progress_callback: callable = None
) -> list:
    """
    Search for places near a location.
    
    Args:
        api_key: Google Maps API key
        location: Search center location (address, city, etc.)
        distance_miles: Search radius in miles
        place_types: List of place type strings to search for
        fetch_details: Whether to fetch additional details for each place
        progress_callback: Optional callback function for progress updates
    
    Returns:
        List of place dictionaries with details
    """
    gmaps = googlemaps.Client(key=api_key)
    
    # Get coordinates for the location
    lat_lng = geocode_location(gmaps, location)
    origin = (lat_lng['lat'], lat_lng['lng'])
    
    all_places = []
    total_count = 0
    
    for place_type in place_types:
        next_page_token = None
        type_count = 0
        
        while True:
            # Search for places
            places_result = gmaps.places(
                query=place_type,
                location=lat_lng,
                radius=distance_miles * 1609.34,  # Convert miles to meters
                open_now=False,
                page_token=next_page_token
            )
            
            for place in places_result.get('results', []):
                # Calculate distance
                place_location = (
                    place['geometry']['location']['lat'],
                    place['geometry']['location']['lng']
                )
                distance = calculate_distance(origin, place_location)
                
                # Skip if outside search radius
                if distance > distance_miles:
                    continue
                
                # Get primary type
                primary_type = place.get('types', [''])[0] if place.get('types') else ''
                
                # Build basic place info
                place_info = {
                    'place_id': place['place_id'],
                    'name': place['name'],
                    'address': place.get('formatted_address', ''),
                    'type': primary_type,
                    'search_term': place_type,
                    'distance_miles': round(distance, 2),
                    'latitude': place_location[0],
                    'longitude': place_location[1],
                    'phone': '',
                    'website': '',
                    'email': '',
                    'opening_hours': '',
                    'rating': place.get('rating', 'N/A'),
                    'review_count': place.get('user_ratings_total', 'N/A')
                }
                
                # Fetch additional details if requested
                if fetch_details:
                    details = get_place_details(gmaps, place['place_id'])
                    
                    # Update address with full formatted address from details
                    if details.get('formatted_address'):
                        place_info['address'] = details.get('formatted_address')
                    
                    place_info['phone'] = details.get(
                        'formatted_phone_number', 
                        details.get('international_phone_number', '')
                    )
                    place_info['website'] = details.get('website', '')
                    
                    # Format opening hours
                    hours = details.get('opening_hours', {})
                    if hours:
                        weekday_text = hours.get('weekday_text', [])
                        place_info['opening_hours'] = '; '.join(weekday_text) if weekday_text else ''
                    
                    place_info['rating'] = details.get('rating', place_info['rating'])
                    place_info['review_count'] = details.get(
                        'user_ratings_total', 
                        place_info['review_count']
                    )
                
                all_places.append(place_info)
                type_count += 1
            
            # Check for more pages
            next_page_token = places_result.get('next_page_token')
            if not next_page_token:
                break
            
            # Rate limiting delay
            time.sleep(API_REQUEST_DELAY)
        
        total_count += type_count
        
        if progress_callback:
            progress_callback(place_type, type_count)
    
    return all_places


def search_places_nearby(
    api_key: str,
    coordinates: tuple,
    radius_meters: int,
    place_type: str,
    fetch_details: bool = True
) -> list:
    """
    Search for places using the Nearby Search API (coordinate-based).
    
    Args:
        api_key: Google Maps API key
        coordinates: Tuple of (latitude, longitude)
        radius_meters: Search radius in meters
        place_type: Single place type string
        fetch_details: Whether to fetch additional details
    
    Returns:
        List of place dictionaries
    """
    gmaps = googlemaps.Client(key=api_key)
    
    all_places = []
    places_result = gmaps.places_nearby(
        location=coordinates,
        radius=radius_meters,
        type=place_type
    )
    
    while True:
        for place in places_result.get('results', []):
            place_info = {
                'place_id': place['place_id'],
                'name': place.get('name', ''),
                'address': place.get('vicinity', ''),
                'latitude': place['geometry']['location']['lat'],
                'longitude': place['geometry']['location']['lng'],
                'phone': '',
                'website': '',
                'email': '',
                'opening_hours': '',
                'rating': 'N/A',
                'review_count': 'N/A'
            }
            
            if fetch_details:
                details = get_place_details(gmaps, place['place_id'])
                place_info['phone'] = details.get(
                    'formatted_phone_number',
                    details.get('international_phone_number', '')
                )
                place_info['website'] = details.get('website', '')
                
                hours = details.get('opening_hours', {})
                if hours:
                    weekday_text = hours.get('weekday_text', [])
                    place_info['opening_hours'] = '; '.join(weekday_text) if weekday_text else ''
                
                place_info['rating'] = details.get('rating', 'N/A')
                place_info['review_count'] = details.get('user_ratings_total', 'N/A')
            
            all_places.append(place_info)
        
        # Check for next page
        if 'next_page_token' not in places_result:
            break
        
        time.sleep(API_REQUEST_DELAY)
        places_result = gmaps.places_nearby(page_token=places_result['next_page_token'])
    
    return all_places