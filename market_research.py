import os
import io
import streamlit as st
import googlemaps
import pandas as pd
from geopy.distance import geodesic
from pathlib import Path
import base64
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import folium_static
from dotenv import load_dotenv

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
                # Place Details request to get additional information
                details_result = gmaps.place(place['place_id'], fields=['formatted_phone_number', 'opening_hours', 'website', 'user_ratings_total', 'rating'])

                # Extract the primary type (first element in the types array)
                primary_type = place.get('types', [])[0] if place.get('types', []) else ''

                # Create a simplified place_info dictionary with additional details
                place_info = {
                    'Place_ID': place['place_id'],
                    'Name': place['name'],
                    'Address': place.get('formatted_address', ''),
                    'Type': primary_type,
                    'Distance': round(distance_miles, 2),
                    'Phone': details_result.get('formatted_phone_number', ''),
                    'Website': details_result.get('website', ''),
                    'Opening_Hours': details_result.get('opening_hours', {}).get('weekday_text', ''),
                    'Rating': details_result.get('rating', 'N/A'),
                    'User_Ratings_Total': details_result.get('user_ratings_total', 'N/A'),
                    'Latitude': place_location[0],
                    'Longitude': place_location[1]
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

# Streamlit app Begins
st.markdown("<h1 style='text-align: center;'>Google Maps Places Search</h1>", unsafe_allow_html=True)

# adding some spacing
st.write("\n")

# Banner below the title
st.markdown("<div style='text-align: center; background-color: white; padding: 10px; border: 2px solid red; border-radius: 5px; font-weight: bold; color: black;'>Error(s) resolves with input of address and place type.</div>", unsafe_allow_html=True)
# adding some spacing
st.write("\n")


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

    # Filter the DataFrame to keep only unique places based on 'Place_ID'
    df_unique = df_unique.drop_duplicates(subset='Place_ID')

    # Sort the DataFrame by 'Distance' in ascending order
    df_unique = df_unique[df_unique['Distance'] <= distance].sort_values(by='Distance')
    
    # Notify user of csv export cability
    st.markdown("<p style='text-align: center; color: red; font-style: italic;'>Want to export data to CSV or enlarge: click top-right corner of the table.</p>", unsafe_allow_html=True)

    # Display the results
    st.write(f"Results for {location}")
    st.write(df_unique)

    # Create a Folium map with markers based on the 'Latitude' and 'Longitude' columns in df_unique
    map_with_markers = folium.Map(location=[df_unique['Latitude'].mean(), df_unique['Longitude'].mean()], zoom_start=10)

    # Add a Marker Cluster to group markers
    marker_cluster = MarkerCluster().add_to(map_with_markers)

    for index, row in df_unique.iterrows():
        # Dynamic popup text for each marker
        popup_text = f"<div style='white-space: nowrap;'><b>{row['Name']}</b><br>" \
                     f"Distance: {row['Distance']} miles</div>"
                    #  f"Opening Hours: {row['Opening_Hours']}<br>" \ #------>Taking this out since im not able to accurately get them from the places API
                    #  f"Rating: {row.get('Rating', 'N/A')}<br>" \ #------>Taking this out since im not able to accurately get them from the places API


        # Add markers to the map with the dynamic popup
        folium.Marker(
            location=[row['Latitude'], row['Longitude']],
            popup=popup_text,
            icon=None  # You can customize the icon if needed
        ).add_to(marker_cluster)
    
    ##----------------------------------------------------------------
    ##----------------------------------------------------------------

    # Download button for Excel file
    output_file = 'MarketResearch.xlsx'

    # Create a BytesIO buffer to hold the Excel file data
    excel_buffer = io.BytesIO()

    # Use pd.ExcelWriter as a context manager to write the DataFrame to the buffer
    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
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

    # Display the Folium map using stfolium
    st.write("Map with Markers:")
    folium_static(map_with_markers)
    
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