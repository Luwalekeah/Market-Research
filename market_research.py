import os
import io
import sys
import time
import base64
import folium
import platform
import webbrowser
import googlemaps
import pandas as pd
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from folium.plugins import MarkerCluster
from streamlit.components.v1 import html
from streamlit_folium import folium_static

# Load environment variables from .env file
load_dotenv()

# --- PATH SETTINGS ---
current_dir = Path(__file__).parent if "__file__" in locals() else Path.cwd()
css_file = current_dir / "styles" / "main.css"

# --- GENERAL SETTINGS ---
PAGE_TITLE = "Find Nearby Places"
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
    total_all_places_count = 0  # Initialize the total count across all place types

    for place_type in place_types:
        next_page_token = None
        total_places_count = 0  # Initialize the count for each place type

        while True:
            # Display loading spinner with custom style
            with st.spinner('Loading More Places...'):
                # Places API text search for each place type with the current next_page_token
                places_result = gmaps.places(
                    query=place_type,
                    location=lat_lng,
                    radius=distance * 1609.34,  # Convert miles to meters
                    open_now=False,
                    page_token=next_page_token
                )

                places = []

                for place in places_result.get('results', []):
                    # Calculate the distance from the provided location in miles
                    place_location = (place['geometry']['location']['lat'], place['geometry']['location']['lng'])
                    distance_miles = calculate_distance((lat_lng['lat'], lat_lng['lng']), place_location)

                    # Check if the distance is within the specified search distance
                    if distance_miles <= distance:
                        # Place Details request to get additional information
                        #details_result = gmaps.place(place_id=place['place_id'], fields=['formatted_phone_number', 'opening_hours', 'website', 'user_ratings_total', 'rating'])

                        # Extract the primary type (first element in the types array)
                        primary_type = place.get('types', [])[0] if place.get('types', []) else ''

                        # Create a simplified place_info dictionary with additional details
                        place_info = {
                            'Place_ID': place['place_id'],
                            'Name': place['name'],
                            'Address': place.get('formatted_address', ''),
                            'Type': primary_type,
                            'Distance': round(distance_miles, 2),
                            # 'Phone': details_result.get('formatted_phone_number', ''),
                            # 'Website': details_result.get('website', ''),
                            # 'Opening_Hours': details_result.get('opening_hours', {}).get('weekday_text', ''),
                            # 'Rating': details_result.get('rating', 'N/A'),
                            # 'User_Ratings_Total': details_result.get('user_ratings_total', 'N/A'),
                            'Latitude': place_location[0],
                            'Longitude': place_location[1]
                        }

                        places.append(place_info)

                # Extend the list of all places with the places for the current type
                all_places.extend(places)

                # Accumulate the count of places for the current type
                total_places_count += len(places)

                # Check if there is a next_page_token
                next_page_token = places_result.get('next_page_token')

                # Break the loop if there is no next_page_token
                if not next_page_token:
                    break

                # Wait for a moment before making the next request (to avoid OVER_QUERY_LIMIT)
                time.sleep(2)

        # Print the total count of places for the current type after processing
        st.write(f"{place_type.capitalize()}: {total_places_count}")

        # Accumulate the total count across all place types
        total_all_places_count += total_places_count

    # Print the total count across all place types
    st.write(f"Total: {total_all_places_count}")

    return all_places

# --- LOAD CSS ---
with open(css_file) as f:
    st.markdown("<style>{}</style>".format(f.read()), unsafe_allow_html=True)

# Check if it's the user's first time
if 'first_time' not in st.session_state:
    # Display the welcome message with background color and wrapping
    welcome_message = """
    <div style="background-color: #87CEEB; padding: 10px; border-radius: 5px; margin-bottom: 10px;">
        👋 Welcome! Default Entry: <b><i><span style='color:#B87333;'>Denver, gas</span></i></b>. Enter yours. For issues, check the bottom of the app.
    </div>
    """
    st.markdown(welcome_message, unsafe_allow_html=True)

    # Split the layout into columns
    columns = st.columns(5)

    # Add dummy columns if needed (skip if not required)
    for _ in range(3):
        columns[0].text("")  # Add empty content to the first two columns

    # Add a button to close the message in the middle column
    close_button = columns[2].button("Close")

    # Check if the button is clicked
    if close_button:
        # Mark that the user has seen the popup
        st.session_state.first_time = True
        # Close the expander immediately
        st.rerun()

    
# Streamlit app Begins
st.markdown("<h1 style='text-align: center;'>Find Nearby Places</h1>", unsafe_allow_html=True)

# adding some spacing
st.write("\n")

#----------------------------------------------------------------
# ---------------------------------------------------------------

# Info section for the purpose of the app
with st.expander("Overview"):
    st.write("You're about to embark on a journey of exploration with the 'Find Nearby Places' app. Imagine having the power to discover hidden gems around you. You simply tell the app where you are or want to explore, set the distance you're willing to travel, and pick the types of places you're interested in. The app then uses its magic to fetch all the relevant information for you.")

# Info section for location
with st.expander("Usage"):
    st.write("Tell Me Where: Share your location, address, or favorite city for personalized exploration.")
    st.write("How Far?: Use a slider to control your exploration radius. You decide the distance!")
    st.write("Mood Filter: Specify preferences, like a meal, workout, or park. App understands you!")
    st.write("Results Snapshot: Neatly organized table with place name, type, distance, and ratings.")
    st.write("Visual Map: Discoveries plotted on a map, providing a virtual tour guide experience.")
    st.write("Take It With You: Download results as an Excel file for planning or sharing.")
    st.write("It's all about putting the adventure in your hands – where to go, what to discover, and how to make the most of your exploration. Happy exploring!")

#----------------------------------------------------------------
# ---------------------------------------------------------------

# Banner below the title
# st.markdown("<div style='text-align: center; background-color: white; padding: 10px; border: 2px solid #B87333; border-radius: 5px; font-weight: bold; color: black;'>Error(s) resolves with input of address and place type.</div>", unsafe_allow_html=True)

# adding some spacing
# st.write("\n")
st.write("\n")


location_default = "Denver"
help_text = "Location to search near: \n\n Your Current Location \n\n Place: Disney Land \n\n ZipCode: 80170\n\nCity: Ibiza"
location = st.text_input("Location (address, city, etc.):", location_default, help=help_text)
if not location:
    location = location_default



# adding some spacing
st.write("\n")
st.write("\n")

distance = st.slider("Distance:", min_value=1.0, max_value=50.0, step=1.0)

# adding some spacing
st.write("\n")
st.write("\n")

# Check if the session state has been initialized
if 'default_place_type' not in st.session_state:
    st.session_state.default_place_type = 'gas'

help_places_txt = "Types of places to search for: \n\n food, gym, church, adult_day_care\n\n cocktail_lounge, university, food_truck, etc..."
# Use st.session_state to persist the input state across reruns
place_types = st.text_input("Place(s) to find:", st.session_state.default_place_type, help=help_places_txt).lower()

# Update the session state with the entered place_types
st.session_state.default_place_type = place_types

# adding some spacing
st.write("\n")

# Convert place types to a list
place_types_list = [place_type.strip() for place_type in place_types.split(',')]

# Find places based on user input
if GOOGLE_MAPS_API_KEY:
    all_places = find_places(GOOGLE_MAPS_API_KEY, location, distance, place_types_list)

    # Create DataFrame from the combined results
    df_display = pd.DataFrame(all_places)

    # Filter the DataFrame to keep only unique places based on 'Place_ID'
    df_display = df_display.drop_duplicates(subset='Place_ID')

    # Sort the DataFrame by 'Distance' in ascending order
    df_display = df_display[df_display['Distance'] <= distance].sort_values(by='Distance')

    # add spacing
    st.write("\n")

    # Notify user of csv export capability
    st.markdown("<p style='text-align: center; color: #B87333; font-style: italic;'>Want to export data to CSV or enlarge: click top-right corner of the table.</p>", unsafe_allow_html=True)

    # Display the results
    st.write(f"Results for {location}")
#----------------------------------------------------------------
#----------------------------------------------------------------

    # Columns to exclude from the map-related columns
    map_related_columns = ['Name', 'Distance', 'Latitude', 'Longitude']

    # Additional text filters for columns not in map_related_columns
    additional_text_filters = [col for col in df_display.columns if col not in map_related_columns]

    # Initialize session state
    if 'filter_all_columns' not in st.session_state:
        st.session_state.filter_all_columns = True

    # Create an expander for filters
    with st.expander("Filters"):
        # Buttons to remove all columns and add all columns
        col1, col2 = st.columns(2)

        with col1:
            remove_all_button = st.button("Remove All Columns", key="remove_all_button", help="Click to remove all columns from the filter list.")
            st.markdown("<style>div[data-testid='stButton'] {margin-left: 0%;}</style>", unsafe_allow_html=True)

        with col2:
            add_all_button = st.button("Add All Columns", key="add_all_button", help="Click to add all columns back to the filter list.")
            st.markdown("<style>div[data-testid='stButton'] {margin-left: 25%;margin-right: 5%;}</style>", unsafe_allow_html=True)

        if remove_all_button:
            # Remove all columns from the filter list
            selected_columns = [column for column in df_display.columns if column not in map_related_columns]
        elif add_all_button:
            # Add all columns back to the filter list
            selected_columns = df_display.columns.tolist()
        else:
            # Default state for selected columns (map-related columns)
            selected_columns = map_related_columns

        # Create checkboxes for each column to control inclusion in text input filter
        include_in_filter = {}
        for column in df_display.columns:
            if column in selected_columns:
                include_in_filter[column] = st.checkbox(f"Include {column} in data filter", True, key=f"{column}_checkbox")

                # Only show the text input if the checkbox is checked
                if include_in_filter[column] and column != "Distance":
                    filter_value = st.text_input(f"Filter by {column}:", "")
                    df_display = df_display[df_display[column].astype(str).str.contains(filter_value, case=False, na=False)]

                # Use a slider for filtering by Distance
                elif include_in_filter[column] and column == "Distance":
                    max_distance = float(df_display["Distance"].max())
                    distance_filter = st.slider("Filter by Distance:", 0.0, max_distance, (0.0, max_distance), key="distance_slider")
                    df_display = df_display[(df_display["Distance"] >= distance_filter[0]) & (df_display["Distance"] <= distance_filter[1])]

        # Additional text filters for columns not in map_related_columns
        for column in additional_text_filters:
            include_in_filter[column] = st.checkbox(f"Include {column} in data filter", False, key=f"{column}_checkbox")

            # Only show the text input if the checkbox is checked
            if include_in_filter[column]:
                filter_value = st.text_input(f"Filter by {column}:", "")
                df_display = df_display[df_display[column].astype(str).str.contains(filter_value, case=False, na=False)]

    # Display the filtered DataFrame
    st.write("\n")
    st.write("\n")
    st.write(df_display)

#----------------------------------------------------------------
#----------------------------------------------------------------

    # Download button for Excel file
    output_file = 'MarketResearch.xlsx'

    # Create a BytesIO buffer to hold the Excel file data
    excel_buffer = io.BytesIO()

    # Use pd.ExcelWriter as a context manager to write the DataFrame to the buffer
    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
        df_display.to_excel(writer, index=False)

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

    # add spacing
    st.empty()
    st.empty()

#----------------------------------------------------------------
# --------------------------------------------------------------- 
    # Function to generate Google Maps link for multiple locations
    def generate_google_maps_link(addresses):
        addresses_str = '|'.join([address.replace(' ', '+') for address in addresses])
        return f"https://www.google.com/maps/search/?api=1&query={addresses_str}"

    # Function to open all locations in a single Google Maps link
    def open_in_google_maps():
        addresses = df_display['Address'].tolist()
        link = generate_google_maps_link(addresses)
        webbrowser.open(link, new=2)

    # Function to open a specific location in Google Maps
    def open_single_location_in_google_maps(address):
        link = generate_google_maps_link([address])
        webbrowser.open(link, new=2)

    st.write("\n")

    # Add an expander with buttons for each place
    with st.expander("Open Places in Google Maps"):
        # Display a warning message
        st.warning("Once a button is clicked, wait 15 seconds for the page to open in the web browser.")
        
        # Add a button to open all locations in Google Maps
        if st.button("Open All in Google Maps"):
            open_in_google_maps()

        for index, row in df_display.iterrows():
            button_label = f"{row['Name']} - {row.get('Distance', 'N/A')}"
            if st.button(button_label):
                open_single_location_in_google_maps(row['Address'])

    st.write("\n")


   # Create a Folium map with markers based on the 'Latitude' and 'Longitude' columns in df_display
    map_with_markers = folium.Map(
        location=[df_display['Latitude'].mean(), df_display['Longitude'].mean()],
        zoom_start=10,
        control_scale=True,  # Show scale control
    )

    # Add a Marker Cluster to group markers
    marker_cluster = MarkerCluster().add_to(map_with_markers)

    # Loop through each row in df_display and add markers to the map
    for index, row in df_display.iterrows():
        # Extract the first part of the address
        first_part_of_address = row['Address'].split(',')[0]

        # Extract the city from the address
        address_parts = row['Address'].split(',')
        city = address_parts[-3].strip()  # Assuming city is the third-to-last part

        # Dynamic popup text for each marker including the first part of the address and city
        popup_text = f"<div style='white-space: nowrap;'><b>{row['Name']} - {first_part_of_address}, {city}</b><br>" \
                    f"Distance: {row['Distance']} miles</div>"

        # Add markers to the map with the dynamic popup
        folium.Marker(
            location=[row['Latitude'], row['Longitude']],
            popup=popup_text,
            icon=None  # You can customize the icon if needed
        ).add_to(marker_cluster)
        
        
    # Display the Folium map using stfolium
    st.markdown("""
        <style>
        iframe {
            width: 100%;
            min-height: 400px;
            height: 100%:
        }
        </style>
        """, unsafe_allow_html=True)
    st.write("Map with Markers:")
    folium_static(map_with_markers)

#----------------------------------------------------------------
# ---------------------------------------------------------------

# Info section for potential errors
with st.expander("Potential Errors"):
    st.write("Here are some potential errors and how to handle them when using the 'Find Nearby Places' app:")
    st.write("No Nearby Places Found: Adjust search parameters or try a different location for results.")
    st.write("Typing Error in Place Types: Remove trailing commas to avoid unexpected results in input.")
    st.write("Invalid Location: Enter a valid address or city for accurate search results.")
    st.write("Unexpected Network Issues: Check internet connection if app warns of API disruptions.")
    st.write("Be aware of potential scenarios for a smoother app experience and exploration.")

# Info section for feature updates
with st.expander("Future Updates"):
    st.write("Exciting updates are in the pipeline for the 'Find Nearby Places' app. Here's what you can look forward to in future releases:")
    st.write("Auto Detect Location: App soon prompts for your location, streamlining nearby place searches effortlessly.")
    st.write("Place Details Upgrade: Upcoming update includes phone numbers, websites, and current open/closed status for discovery ease.")
    st.write("Open Now Filter: Tailor your search to show places open now for on-the-spot convenience.")
    st.write("Improved Input: App intelligently handles input, eliminating extra spaces and commas for smoother interaction.")
    st.write("User Accounts Coming: Create an account to save searches for personalized future explorations. Login soon!")
    st.write("**6. Enhanced filter: ** Ability to select items from the list below for quick and easy filtering options.")
    st.write("We're committed to continuously enhancing your experience with the app. Stay tuned for these exciting updates, and happy exploring!")

#----------------------------------------------------------------
# ---------------------------------------------------------------

# Add empty space above and below the copyright notice
st.empty()
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
