"""
Module for creating interactive maps with place markers.
"""
import folium
from folium.plugins import MarkerCluster
import pandas as pd
from typing import Optional


def create_places_map(
    df: pd.DataFrame,
    center: Optional[tuple] = None,
    zoom_start: int = 10,
    use_clustering: bool = True,
    show_scale: bool = True
) -> folium.Map:
    """
    Create a Folium map with markers for places.
    
    Args:
        df: DataFrame with place data (must have Latitude, Longitude columns)
        center: Optional (lat, lng) tuple for map center. 
                If None, uses mean of all locations.
        zoom_start: Initial zoom level
        use_clustering: Whether to use marker clustering
        show_scale: Whether to show scale control
    
    Returns:
        Folium Map object
    """
    if df.empty:
        # Return empty map centered on US
        return folium.Map(location=[39.8283, -98.5795], zoom_start=4)
    
    # Determine center
    if center is None:
        center = (df['Latitude'].mean(), df['Longitude'].mean())
    
    # Create map
    m = folium.Map(
        location=center,
        zoom_start=zoom_start,
        control_scale=show_scale
    )
    
    # Add markers
    if use_clustering:
        marker_cluster = MarkerCluster().add_to(m)
        container = marker_cluster
    else:
        container = m
    
    for _, row in df.iterrows():
        # Build popup content
        popup_html = _build_popup_html(row)
        
        # Create marker
        folium.Marker(
            location=[row['Latitude'], row['Longitude']],
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=row.get('Name', 'Unknown')
        ).add_to(container)
    
    return m


def _build_popup_html(row: pd.Series) -> str:
    """
    Build HTML content for a marker popup.
    
    Args:
        row: DataFrame row with place data
    
    Returns:
        HTML string for popup
    """
    name = row.get('Name', 'Unknown')
    
    # Extract city from address
    address = row.get('Address', '')
    address_parts = address.split(',')
    short_address = address_parts[0] if address_parts else address
    city = address_parts[-3].strip() if len(address_parts) >= 3 else ''
    
    distance = row.get('Distance', 'N/A')
    phone = row.get('Phone', '')
    website = row.get('Website', '')
    email = row.get('Email', '')
    rating = row.get('Rating', 'N/A')
    
    html_parts = [
        f"<div style='white-space: nowrap; min-width: 200px;'>",
        f"<b>{name}</b><br>",
        f"<small>{short_address}"
    ]
    
    if city:
        html_parts.append(f", {city}")
    
    html_parts.append("</small><br>")
    html_parts.append(f"📍 {distance} miles")
    
    if rating and rating != 'N/A':
        html_parts.append(f" | ⭐ {rating}")
    
    if phone:
        html_parts.append(f"<br>📞 {phone}")
    
    if email:
        html_parts.append(f"<br>✉️ <a href='mailto:{email}'>{email}</a>")
    
    if website:
        html_parts.append(f"<br>🌐 <a href='{website}' target='_blank'>Website</a>")
    
    html_parts.append("</div>")
    
    return ''.join(html_parts)


def generate_google_maps_link(addresses: list) -> str:
    """
    Generate a Google Maps link for multiple addresses.
    
    Args:
        addresses: List of address strings
    
    Returns:
        Google Maps URL
    """
    if not addresses:
        return ""
    
    from urllib.parse import quote
    addresses_str = '|'.join([quote(addr) for addr in addresses])
    return f"https://www.google.com/maps/search/?api=1&query={addresses_str}"


def generate_single_maps_link(address: str, lat: float = None, lng: float = None) -> str:
    """
    Generate a Google Maps link for a single address.
    
    Uses coordinates if provided (more reliable), otherwise falls back to address.
    
    Args:
        address: Address string
        lat: Optional latitude
        lng: Optional longitude
    
    Returns:
        Google Maps URL
    """
    from urllib.parse import quote
    
    # Prefer coordinates if available (more accurate)
    if lat is not None and lng is not None:
        return f"https://www.google.com/maps/search/?api=1&query={lat},{lng}"
    
    if not address:
        return ""
    
    return f"https://www.google.com/maps/search/?api=1&query={quote(address)}"