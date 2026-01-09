"""
Find Nearby Places - Streamlit Web Application

A market research tool for discovering nearby businesses and extracting
their contact information including phone numbers, websites, and emails.
"""
import streamlit as st
from streamlit_folium import st_folium

from src import (
    GOOGLE_MAPS_API_KEY,
    DEFAULT_LOCATION,
    DEFAULT_PLACE_TYPE,
    DEFAULT_DISTANCE_MILES,
    search_places,
    enrich_places_with_emails,
    enrich_with_agent_names,
    get_colorado_data_status,
    places_to_dataframe,
    clean_dataframe,
    export_to_excel,
    get_summary_stats,
    create_places_map,
    generate_single_maps_link,
)

# Page configuration
st.set_page_config(
    page_title="Find Nearby Places",
    page_icon="📍",
    layout="wide"
)

# Load custom CSS
def load_css():
    """Load custom CSS styles."""
    css = """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Readex+Pro:wght@300;400;500;600;700&display=swap');
    
    * {font-family: 'Readex Pro', sans-serif;}
    
    a {
        text-decoration: none;
        color: white !important;
        font-weight: 500;
    }
    
    a:hover {
        color: #B87333 !important;
    }
    
    .stAlert {
        border-radius: 10px;
    }
    
    .metric-card {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        text-align: center;
    }
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

load_css()


def show_welcome_message():
    """Display welcome message for first-time users."""
    if 'welcomed' not in st.session_state:
        st.info(
            "👋 **Welcome!** Enter a location, set your search distance, "
            "and specify the types of places you're looking for. "
            "Default: Denver, gas stations within 10 miles."
        )
        if st.button("Got it!", key="welcome_dismiss"):
            st.session_state.welcomed = True
            st.rerun()


def main():
    """Main application function."""
    
    # Header
    st.markdown(
        "<h1 style='text-align: center;'>📍 Find Nearby Places</h1>",
        unsafe_allow_html=True
    )
    
    show_welcome_message()
    
    # Sidebar with app info
    with st.sidebar:
        st.header("About")
        st.markdown("""
        This app helps you discover businesses near any location
        and gather their contact information.
        
        **Features:**
        - Search by location & radius
        - Get phone numbers & websites
        - Extract emails from websites
        - Export to Excel
        - Interactive map view
        """)
        
        st.divider()
        
        # Settings
        st.header("Settings")
        fetch_details = st.checkbox(
            "Fetch detailed info",
            value=True,
            help="Get phone numbers, websites, ratings, and hours. Slower but more complete."
        )
        extract_emails = st.checkbox(
            "Extract emails",
            value=False,
            help="Attempt to extract email addresses from business websites. This may take extra time."
        )
        
        # Colorado SOS matching
        st.divider()
        st.subheader("🏛️ Colorado SOS Lookup")
        match_colorado_sos = st.checkbox(
            "Find registered agents",
            value=False,
            help="Match businesses against Colorado Secretary of State database to find registered agent names."
        )
        
        if match_colorado_sos:
            # Show Colorado data status
            co_status = get_colorado_data_status()
            if co_status['cached']:
                st.caption(
                    f"📁 Data cached ({co_status['size_mb']} MB)\n\n"
                    f"Updated: {co_status['last_updated']}"
                )
            else:
                st.caption("⚠️ Data will be downloaded on first use (~200MB)")
    
    # Check API key
    if not GOOGLE_MAPS_API_KEY:
        st.error(
            "⚠️ Google Maps API key not found! "
            "Please set GOOGLE_MAPS_API_KEY in your .env file."
        )
        st.stop()
    
    # Search inputs
    col1, col2 = st.columns([2, 1])
    
    with col1:
        location = st.text_input(
            "📍 Location",
            value=DEFAULT_LOCATION,
            placeholder="Enter address, city, or place name",
            help="Examples: 'Denver, CO', '1600 Pennsylvania Ave', 'Central Park, NYC'"
        )
    
    with col2:
        distance = st.slider(
            "🎯 Search Radius (miles)",
            min_value=1.0,
            max_value=50.0,
            value=float(DEFAULT_DISTANCE_MILES),
            step=1.0
        )
    
    place_types_input = st.text_input(
        "🔍 Place Types",
        value=DEFAULT_PLACE_TYPE,
        placeholder="gas, restaurant, gym",
        help="Enter comma-separated place types: restaurant, gym, hospital, pharmacy, etc."
    )
    
    # Parse place types
    place_types = [pt.strip().lower() for pt in place_types_input.split(',') if pt.strip()]
    
    if not place_types:
        st.warning("Please enter at least one place type.")
        st.stop()
    
    # Search button
    search_clicked = st.button("🔍 Search Places", type="primary", use_container_width=True)
    
    if search_clicked or 'results_df' in st.session_state:
        
        if search_clicked:
            # Perform search
            with st.spinner("Searching for places..."):
                try:
                    # Progress tracking
                    progress_container = st.empty()
                    
                    def progress_callback(place_type, count):
                        progress_container.write(f"Found {count} results for '{place_type}'")
                    
                    places = search_places(
                        api_key=GOOGLE_MAPS_API_KEY,
                        location=location,
                        distance_miles=distance,
                        place_types=place_types,
                        fetch_details=fetch_details,
                        progress_callback=progress_callback
                    )
                    
                    progress_container.empty()
                    
                except Exception as e:
                    st.error(f"Search failed: {str(e)}")
                    st.stop()
            
            if not places:
                st.warning("No places found. Try adjusting your search criteria.")
                st.stop()
            
            # Extract emails if enabled
            if extract_emails:
                with st.spinner("Extracting emails from websites..."):
                    email_progress = st.progress(0)
                    
                    def email_progress_callback(completed, total):
                        email_progress.progress(completed / total)
                    
                    places = enrich_places_with_emails(
                        places,
                        progress_callback=email_progress_callback
                    )
                    
                    email_progress.empty()
            
            # Convert to DataFrame
            df = places_to_dataframe(places)
            df = clean_dataframe(df, max_distance=distance)
            
            # Match against Colorado SOS database if enabled
            if match_colorado_sos:
                with st.spinner("Matching against Colorado Secretary of State database..."):
                    sos_progress = st.progress(0)
                    sos_status = st.empty()
                    
                    def sos_progress_callback(completed, total):
                        sos_progress.progress(completed / total)
                        sos_status.caption(f"Processing {completed}/{total} businesses...")
                    
                    df = enrich_with_agent_names(
                        df,
                        progress_callback=sos_progress_callback
                    )
                    
                    sos_progress.empty()
                    sos_status.empty()
                    
                    # Show match summary
                    if 'BusinessName' in df.columns:
                        matched = (df['BusinessName'].notna() & (df['BusinessName'] != '')).sum()
                        agent_matched = (df['AgentName'].notna() & (df['AgentName'] != '')).sum()
                        st.success(f"🏛️ Matched {matched} businesses to Colorado SOS records ({agent_matched} with agent names)")
            
            # Store in session state
            st.session_state.results_df = df
            st.session_state.search_location = location
        
        else:
            df = st.session_state.results_df
            location = st.session_state.get('search_location', location)
        
        # Display results
        st.divider()
        
        # Summary metrics
        stats = get_summary_stats(df)
        
        # Calculate agent and business name match counts
        agent_count = 0
        business_name_count = 0
        if 'AgentName' in df.columns:
            agent_count = (df['AgentName'].notna() & (df['AgentName'] != '')).sum()
        if 'BusinessName' in df.columns:
            business_name_count = (df['BusinessName'].notna() & (df['BusinessName'] != '')).sum()
        
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        col1.metric("Total Places", stats['total_places'])
        col2.metric("With Phone", stats['with_phone'])
        col3.metric("With Website", stats['with_website'])
        col4.metric("With Email", stats['with_email'])
        col5.metric("SOS Matched", business_name_count)
        col6.metric("Avg Distance", f"{stats.get('avg_distance', 0)} mi")
        
        st.divider()
        
        # Tabs for different views
        tab_table, tab_map, tab_export = st.tabs(["📊 Data Table", "🗺️ Map View", "📥 Export"])
        
        with tab_table:
            st.subheader(f"Results for: {location}")
            
            # Filters in expander
            with st.expander("🔧 Filters"):
                col1, col2 = st.columns(2)
                
                with col1:
                    name_filter = st.text_input("Filter by name", "")
                    type_filter = st.text_input("Filter by type", "")
                
                with col2:
                    if 'Distance' in df.columns:
                        max_dist = float(df['Distance'].max())
                        dist_range = st.slider(
                            "Distance range",
                            0.0, max_dist, (0.0, max_dist)
                        )
                    
                    show_with_email = st.checkbox("Only show places with email")
            
            # Apply filters
            filtered_df = df.copy()
            
            if name_filter:
                filtered_df = filtered_df[
                    filtered_df['Name'].str.contains(name_filter, case=False, na=False)
                ]
            
            if type_filter:
                filtered_df = filtered_df[
                    filtered_df['Type'].str.contains(type_filter, case=False, na=False)
                ]
            
            if 'Distance' in filtered_df.columns:
                filtered_df = filtered_df[
                    (filtered_df['Distance'] >= dist_range[0]) &
                    (filtered_df['Distance'] <= dist_range[1])
                ]
            
            if show_with_email and 'Email' in filtered_df.columns:
                filtered_df = filtered_df[
                    filtered_df['Email'].notna() & (filtered_df['Email'] != '')
                ]
            
            # Display table
            st.dataframe(
                filtered_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Website": st.column_config.LinkColumn("Website"),
                    "Email": st.column_config.TextColumn("Email"),
                    "Distance": st.column_config.NumberColumn("Distance (mi)", format="%.2f"),
                    "Rating": st.column_config.NumberColumn("Rating", format="%.1f"),
                }
            )
            
            st.caption(f"Showing {len(filtered_df)} of {len(df)} places")
        
        with tab_map:
            st.subheader("Interactive Map")
            
            if not df.empty and 'Latitude' in df.columns:
                places_map = create_places_map(df)
                
                st_folium(places_map, width=None, height=500, returned_objects=[])
                
                # Google Maps links
                with st.expander("🔗 Open in Google Maps"):
                    for _, row in df.head(20).iterrows():
                        # Use coordinates for more reliable links
                        link = generate_single_maps_link(
                            row['Address'], 
                            lat=row.get('Latitude'), 
                            lng=row.get('Longitude')
                        )
                        st.markdown(
                            f"[{row['Name']} - {row['Distance']} mi]({link})"
                        )
            else:
                st.info("No location data available for mapping.")
        
        with tab_export:
            st.subheader("Export Data")
            
            # Select columns to export
            available_cols = df.columns.tolist()
            selected_cols = st.multiselect(
                "Select columns to export",
                available_cols,
                default=available_cols
            )
            
            if selected_cols:
                export_df = df[selected_cols]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Excel export
                    excel_bytes = export_to_excel(export_df, return_bytes=True)
                    st.download_button(
                        label="📥 Download Excel",
                        data=excel_bytes,
                        file_name="MarketResearch.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                
                with col2:
                    # CSV export
                    csv_bytes = export_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download CSV",
                        data=csv_bytes,
                        file_name="MarketResearch.csv",
                        mime="text/csv"
                    )
    
    # Footer
    st.divider()
    
    with st.expander("ℹ️ Help & Information"):
        st.markdown("""
        ### How to Use
        
        1. **Location**: Enter any address, city, landmark, or ZIP code
        2. **Distance**: Set how far you want to search (in miles)
        3. **Place Types**: Enter types of places separated by commas
           - Examples: `restaurant`, `gym`, `hospital`, `pharmacy`, `gas`, `hotel`
        
        ### About Email Extraction
        
        Google Maps does not provide email addresses directly. When enabled,
        this app visits business websites to search for email addresses on
        their main page and contact pages.
        
        ### About Colorado SOS Lookup
        
        This feature matches businesses against the Colorado Secretary of State's
        public business entities database to find registered agent names.
        
        **How it works:**
        1. First attempts to match by business name (fuzzy matching, 80%+ threshold)
        2. If no name match, falls back to matching by street address
        3. Only matches Colorado businesses (filters by city for accuracy)
        
        **Output columns when enabled:**
        - `BusinessName`: Official registered name from Colorado SOS
        - `AgentName`: Registered agent's name (person names only)
        - `MatchConfidence`: Fuzzy match score (0-100)
        - `MatchType`: How the match was made (`name` or `address`)
        
        **Note:** The Colorado data (~600MB) is downloaded on first use and cached
        locally for 7 days. Agent names are only included when a person's first name
        is available (organization names are excluded).
        
        ### Tips
        
        - Enable "Fetch detailed info" for complete contact information
        - Email extraction can be slow for large result sets
        - Use specific place types for better results
        - Colorado SOS lookup works best for Colorado-based searches
        
        ### Common Place Types
        
        `restaurant`, `cafe`, `bar`, `gym`, `hospital`, `pharmacy`, `bank`,
        `gas_station`, `hotel`, `school`, `church`, `park`, `store`, `salon`
        """)
    
    # Copyright
    st.markdown(
        """
        <div style="text-align: center; padding: 20px; color: gray;">
            Made with ❤️ by <a href="https://github.com/Luwalekeah" target="_blank">Luwalekeah</a>
        </div>
        """,
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()