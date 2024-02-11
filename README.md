** This code is still a work in progress, having issues with generating the phone, website, and opening hours for the places **


- The script interacts with the Google Places API to retrieve information about various place types (e.g., clinics, nursing homes) within a specified distance from a user-provided location.
- Users are prompted to input their Google Maps API key, location, search distance in miles, and a comma-separated list of place types of interest.
- Place details, including names, addresses, types, distances, phone numbers, opening hours, and website URLs, are collected from the API response and organized into a structured DataFrame.
- The resulting DataFrame is filtered to include only unique places, sorted by distance, and then saved to an Excel file for subsequent analysis and reference.
- Finally, the end user is able to download the file to their local machine
