# 📍 Market Research App - Find Nearby Places

A powerful market research tool that discovers businesses near any location and extracts their contact information including phone numbers, websites, and email addresses.

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-1.31+-red.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

## ✨ Features

- **Location-based Search**: Find businesses near any address, city, or ZIP code
- **Multiple Place Types**: Search for restaurants, gyms, pharmacies, and more
- **Contact Information**: Get phone numbers, websites, and operating hours
- **Email Extraction**: Automatically extract emails from business websites
- **Interactive Map**: Visualize results on an interactive map
- **Data Export**: Export to Excel or CSV formats
- **Dual Interface**: Use via web app (Streamlit) or command line

## 🚀 Quick Start

### Prerequisites

- Python 3.9 or higher
- Google Maps API key ([Get one here](https://console.cloud.google.com/google/maps-apis))

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Luwalekeah/market-research-app.git
   cd market-research-app
   ```

2. **Create a virtual environment** (recommended)
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure your API key**
   ```bash
   cp .env.example .env
   # Edit .env and add your Google Maps API key
   ```

### Running the App

**Web Interface (Streamlit):**
```bash
streamlit run app.py
```

**Command Line Interface:**
```bash
python cli.py -l "Denver, CO" -d 10 -t "restaurant,gym" --emails -o results.xlsx
```

## 📖 Usage

### Web App

1. Enter a location (address, city, or ZIP code)
2. Set your search radius in miles
3. Enter place types separated by commas (e.g., `restaurant, gym, pharmacy`)
4. Optionally enable email extraction
5. Click "Search Places"
6. View results in the data table or map
7. Export to Excel or CSV

### Command Line

```bash
# Basic search
python cli.py -l "New York, NY" -d 5 -t "pharmacy"

# With email extraction
python cli.py -l "Los Angeles" -d 10 -t "gym,yoga" --emails

# Export as CSV with verbose output
python cli.py -l "Miami" -d 15 -t "hotel" --csv -v -o hotels.csv

# Full options
python cli.py --help
```

### As a Python Package

```python
from src import (
    search_places,
    enrich_places_with_emails,
    places_to_dataframe,
    export_to_excel
)

# Search for places
places = search_places(
    api_key="YOUR_API_KEY",
    location="San Francisco, CA",
    distance_miles=5,
    place_types=["coffee", "cafe"]
)

# Extract emails from websites
places = enrich_places_with_emails(places)

# Convert to DataFrame and export
df = places_to_dataframe(places)
export_to_excel(df, "coffee_shops.xlsx")
```

## 📁 Project Structure

```
market-research-app/
├── app.py                 # Streamlit web application
├── cli.py                 # Command-line interface
├── src/
│   ├── __init__.py       # Package exports
│   ├── config.py         # Configuration and constants
│   ├── places.py         # Google Places API integration
│   ├── email_extractor.py # Email extraction from websites
│   ├── data_utils.py     # Data processing and export
│   └── mapping.py        # Map visualization
├── styles/
│   └── main.css          # Custom CSS styles
├── tests/
│   └── test_places.py    # Unit tests
├── data/                  # Output directory
├── requirements.txt       # Python dependencies
├── pyproject.toml        # Package configuration
├── .env.example          # Environment variables template
└── README.md             # This file
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `GOOGLE_MAPS_API_KEY` | Your Google Maps API key | Yes |
| `DEFAULT_LOCATION` | Default search location | No |
| `DEFAULT_DISTANCE_MILES` | Default search radius | No |
| `DEFAULT_PLACE_TYPE` | Default place type | No |

### Google Cloud Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or select existing)
3. Enable the following APIs:
   - Places API
   - Geocoding API
4. Create an API key in "Credentials"
5. Add the key to your `.env` file

## 📧 About Email Extraction

**Important**: Google Places API does not provide email addresses directly. This app extracts emails by:

1. Getting the business website URL from Google Places
2. Visiting the website's main page and common contact pages
3. Searching for email patterns in the HTML content

This process:
- Respects rate limits and robot.txt
- Only extracts publicly visible emails
- May not find emails on all websites
- Can increase processing time significantly

## 🏷️ Common Place Types

| Category | Types |
|----------|-------|
| Food & Drink | `restaurant`, `cafe`, `bar`, `bakery`, `meal_takeaway` |
| Health | `hospital`, `pharmacy`, `doctor`, `dentist`, `gym` |
| Services | `bank`, `atm`, `gas_station`, `car_repair`, `laundry` |
| Shopping | `store`, `supermarket`, `shopping_mall`, `clothing_store` |
| Lodging | `hotel`, `lodging`, `campground` |
| Recreation | `park`, `movie_theater`, `museum`, `spa` |

[Full list of place types](https://developers.google.com/maps/documentation/places/web-service/supported_types)

## 🧪 Development

### Running Tests

```bash
pip install -e ".[dev]"
pytest
```

### Code Formatting

```bash
black .
ruff check --fix .
```

### Type Checking

```bash
mypy src/
```

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 🙏 Acknowledgments

- [Google Maps Platform](https://developers.google.com/maps) for the Places API
- [Streamlit](https://streamlit.io/) for the web framework
- [Folium](https://python-visualization.github.io/folium/) for map visualization

## 📧 Contact

Luwalekeah - [GitHub](https://github.com/Luwalekeah)

---

Made with ❤️ for market researchers and lead generation professionals
