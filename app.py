"""
Market Research Lead Generator - Streamlit Web Application

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

# App configuration
APP_NAME = "LeadFinder Pro"  # Change this to your preferred name
APP_ICON = "🎯"  # Options: 📍 🎯 🔍 📊 💼

# Page configuration
st.set_page_config(
    page_title=APP_NAME,
    page_icon=APP_ICON,
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
    """Display welcome message for first-time users with integrated X close button."""
    if 'welcomed' not in st.session_state:
        
        # Check if dismiss button was clicked
        if st.session_state.get('dismiss_clicked', False):
            st.session_state.welcomed = True
            st.session_state.dismiss_clicked = False
            st.rerun()
        
        # Create the banner with integrated X button
        banner_col, btn_col = st.columns([50, 1])
        
        with banner_col:
            st.markdown("""
                <div style="
                    background: #1e3a5f;
                    color: white;
                    padding: 15px 20px;
                    border-radius: 10px;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                ">
                    <span>👋 <strong>Welcome!</strong> Enter a location, set your search distance, and specify the types of places you're looking for. Default: Denver, gas stations within 10 miles.</span>
                </div>
            """, unsafe_allow_html=True)
        
        # Hidden button - we'll style it to appear inside the banner
        with btn_col:
            st.markdown("<div style='margin-top: 5px;'></div>", unsafe_allow_html=True)
            clicked = st.button("✕", key="welcome_dismiss")
            if clicked:
                st.session_state.welcomed = True
                st.rerun()
        
        # CSS to move the button visually inside the banner
        st.markdown("""
            <style>
            /* Target the specific column with the X button and overlay it on the banner */
            [data-testid="stHorizontalBlock"]:has([data-testid="baseButton-secondary"]) {
                position: relative;
            }
            [data-testid="stHorizontalBlock"]:has([data-testid="baseButton-secondary"]) > [data-testid="stColumn"]:last-child {
                position: absolute;
                right: 25px;
                top: 50%;
                transform: translateY(-50%);
                width: auto !important;
                flex: none !important;
            }
            [data-testid="stHorizontalBlock"]:has([data-testid="baseButton-secondary"]) > [data-testid="stColumn"]:last-child button {
                background: rgba(0, 0, 0, 0.3) !important;
                border: none !important;
                color: #000 !important;
                font-weight: bold !important;
                border-radius: 4px !important;
                padding: 2px 10px !important;
                min-height: 0 !important;
                height: 28px !important;
                line-height: 1 !important;
            }
            [data-testid="stHorizontalBlock"]:has([data-testid="baseButton-secondary"]) > [data-testid="stColumn"]:last-child button:hover {
                background: rgba(0, 0, 0, 0.5) !important;
            }
            </style>
        """, unsafe_allow_html=True)


def main():
    """Main application function."""
    
    # Header
    st.markdown(
        f"<h1 style='text-align: center;'>{APP_ICON} {APP_NAME}</h1>",
        unsafe_allow_html=True
    )
    
    show_welcome_message()
    
    # Sidebar with app info
    with st.sidebar:
        st.header("About")
        st.markdown("""
        Discover businesses near any location and gather their 
        contact information for lead generation.
        
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
        
        # Filter options (only show when SOS lookup is enabled)
        filter_low_similarity = False
        min_name_similarity = 50
        filter_good_standing_only = False
        
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
            
            # Filter options
            st.divider()
            
            # Good Standing filter
            filter_good_standing_only = st.checkbox(
                "Only 'Good Standing' businesses",
                value=True,
                help="Only show businesses with 'Good Standing' status (excludes Delinquent)"
            )
            
            # Filter option for mismatched names
            filter_low_similarity = st.checkbox(
                "Hide mismatched business names",
                value=True,
                help="Filter out results where the Google Places name doesn't match the Colorado registered name (e.g., different business at same address)"
            )
            
            if filter_low_similarity:
                min_name_similarity = st.slider(
                    "Minimum name similarity %",
                    min_value=0,
                    max_value=100,
                    value=40,
                    step=5,
                    help="Only show results where Google name is at least this similar to Colorado business name. 40% is recommended."
                )
    
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
            
            # Apply Good Standing filter (from sidebar settings)
            if filter_good_standing_only and 'EntityStatus' in filtered_df.columns:
                # Keep rows where: no match (EntityStatus is empty) OR status is Good Standing
                has_match = (filtered_df['EntityStatus'].notna() & (filtered_df['EntityStatus'] != ''))
                is_good_standing = filtered_df['EntityStatus'].str.upper().str.contains('GOOD STANDING', na=False)
                filtered_df = filtered_df[~has_match | is_good_standing]
            
            # Apply name similarity filter (from sidebar settings)
            if filter_low_similarity and 'NameSimilarity' in filtered_df.columns:
                # Keep rows where: no match (NameSimilarity=0 and BusinessName is empty) OR similarity >= threshold
                has_match = (filtered_df['BusinessName'].notna() & (filtered_df['BusinessName'] != ''))
                meets_threshold = (filtered_df['NameSimilarity'] >= min_name_similarity)
                filtered_df = filtered_df[~has_match | meets_threshold]
            
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
                    "MatchConfidence": st.column_config.NumberColumn("Match %", format="%.0f"),
                    "NameSimilarity": st.column_config.ProgressColumn(
                        "Name Match",
                        format="%.0f%%",
                        min_value=0,
                        max_value=100,
                        help="How similar the Google Places name is to the Colorado registered business name"
                    ),
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