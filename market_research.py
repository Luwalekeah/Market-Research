import os
from io import StringIO
import base64
import folium
import googlemaps
import pandas as pd
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv
from geopy.distance import geodesic
from streamlit_folium import folium_static

# Load environment variables from .env file
load_dotenv()

# --- PATH SETTINGS ---
current_dir = Path(__file__).parent if "__file__" in locals() else Path.cwd()
css_file = current_dir / "styles" / "main.css"

# --- GENERAL SETTINGS ---
PAGE_TITLE = "Search Nearby Places"
PAGE_ICON = ":Round Pushpin:"
st.set_page_config(page_title=PAGE_TITLE, page_icon=PAGE_ICON)

GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

def calculate_distance(origin, destination):
    return geodesic(origin, destination).miles

def find_places(api_key, location, distance, place_types):
    gmaps = googlemaps.Client(key=api_key)

    # Geocoding to get latitude and longitude from the address
    geocode_result = gmaps.geocode(location)
    lat_lng = geocode_result[0]['geometry']['location']

    all_places = []

    for place_type in place_types:
        # Places API text search for each place type
        places_result = gmaps.places(
            query=place_type,
            location=lat_lng,
            radius=distance * 1609.34,  # Convert miles to meters
            open_now=False
        )

        places = []

        for place in places_result['results']:
            # Calculate the distance from the provided location in miles
            place_location = (place['geometry']['location']['lat'], place['geometry']['location']['lng'])
            distance_miles = calculate_distance((lat_lng['lat'], lat_lng['lng']), place_location)

            # Check if the distance is within the specified search distance
            if distance_miles <= distance:
                # Extract the primary type (first element in the types array)
                primary_type = place.get('types', [])[0] if place.get('types', []) else ''

                # Create a simplified place_info dictionary without phone, website, and opening hours
                place_info = {
                    'Place_ID': place['place_id'],
                    'Name': place['name'],
                    'Address': place.get('formatted_address', ''),
                    'Type': primary_type,
                    'Distance': round(distance_miles, 2),
                    'Latitude': place_location[0],
                    'Longitude': place_location[1],
                }

                places.append(place_info)

        # Print the count of each type for each object
        st.write(f"{place_type.capitalize()}: {len(places)}")

        # Extend the list of all places with the places for the current type
        all_places.extend(places)

    return all_places

# --- LOAD CSS ---
with open(css_file) as f:
    st.markdown("<style>{}</style>".format(f.read()), unsafe_allow_html=True)

# Streamlit app
st.title("Google Maps Places Search")

location = st.text_input("Enter your location (address, city, etc.):")
distance = st.slider("Choose the search distance in miles:", min_value=1.0, max_value=50.0, step=1.0)
place_types = st.text_input("Enter a comma-separated list of place types (e.g., gym, nursing_home, restaurant):").lower()

# Convert place types to a list
place_types_list = [place_type.strip() for place_type in place_types.split(',')]

# Find places based on user input
if GOOGLE_MAPS_API_KEY:
    all_places = find_places(GOOGLE_MAPS_API_KEY, location, distance, place_types_list)

    # Create DataFrame from the combined results
    df_unique = pd.DataFrame(all_places)

    # Rename columns to match the expected names for latitude and longitude
    df_unique.rename(columns={'Latitude': 'lat', 'Longitude': 'lon'}, inplace=True)

    # Filter the DataFrame to keep only unique places based on 'Place_ID'
    df_unique = df_unique.drop_duplicates(subset='Place_ID')

    # Sort the DataFrame by 'Distance' in ascending order
    df_unique = df_unique[df_unique['Distance'] <= distance].sort_values(by='Distance')


#----------------------------------------------------------------
#----------------------------------------------------------------

# Display the results
st.write(f"Results for {location}")
st.write(df_unique)

# Create a Folium map with markers based on the 'latitude' and 'longitude' columns in df_unique
map_with_markers = folium.Map(location=[df_unique['lat'].mean(), df_unique['lon'].mean()], zoom_start=10)

# Add markers to the map for each row in df_unique
for index, row in df_unique.iterrows():
    name = row['Name']  # Assuming 'Name' is a column in your DataFrame
    distance = row['Distance']  # Assuming 'Distance' is a column in your DataFrame
    lat, lon = row['lat'], row['lon']

    # Concatenate name and distance for the popup
    popup_content = f"{name} - {distance} miles"

    # Add a marker with a pop-up showing the name and distance
    folium.Marker(location=[lat, lon], popup=popup_content).add_to(map_with_markers)

# Display the Folium map using stfolium
st.write("Map with Markers:")
folium_static(map_with_markers)

##----------------------------------------------------------------
##----------------------------------------------------------------


# Download button for Excel file
output_file = 'MarketResearch.xlsx'

# Create a BytesIO buffer to hold the Excel file data
excel_buffer = io.BytesIO()

# Use pd.ExcelWriter as a context manager to write the DataFrame to the buffer
with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
    df_unique.to_excel(writer, index=False)

# Get the Excel file data as bytes
excel_bytes = excel_buffer.getvalue()

# Split the layout into columns
columns = st.columns(3)

# Add dummy columns if needed (skip if not required)
for _ in range(1):
    columns[0].text("")  # Add empty content to the first two columns


# Display a Streamlit download button in the third column (optional)
columns[1].download_button(
    label="Download Excel File",
    data=excel_bytes,
    file_name=output_file,
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    key="streamlit_download_button"
)

#----------------------------------------------------------------
#----------------------------------------------------------------


# Add empty space above and below the copyright notice
st.empty()

# Centered copyright notice and link to GitHub
st.markdown("""
    <div style="display: flex; justify-content: center; text-align: center;">
        <p>Copyright © 2024 Luwalekeah. 
        <a href="https://github.com/Luwalekeah" target="_blank">GitHub</a></p>
    </div>
""", unsafe_allow_html=True)

# Add empty space below the copyright notice
st.empty()

