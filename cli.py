#!/usr/bin/env python3
"""
Find Nearby Places - Command Line Interface

Usage:
    python cli.py --location "Denver, CO" --distance 10 --types "restaurant,gym"
    python cli.py -l "New York" -d 5 -t "pharmacy" --emails --output results.xlsx
    python cli.py -l "Aurora, CO" -d 5 -t "gas" --agents -v
"""
import argparse
import sys
from pathlib import Path

from src import (
    GOOGLE_MAPS_API_KEY,
    search_places,
    enrich_places_with_emails,
    enrich_with_agent_names,
    places_to_dataframe,
    clean_dataframe,
    export_to_excel,
    export_to_csv,
    get_summary_stats,
)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Find nearby places and extract contact information",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -l "Denver, CO" -d 10 -t "restaurant"
  %(prog)s -l "NYC" -d 5 -t "gym,pharmacy" --emails -o leads.xlsx
  %(prog)s --location "90210" --distance 15 --types "hotel,restaurant" --csv
  %(prog)s -l "Aurora, CO" -d 5 -t "gas" --agents -v
        """
    )
    
    parser.add_argument(
        '-l', '--location',
        required=True,
        help='Search location (address, city, ZIP code)'
    )
    
    parser.add_argument(
        '-d', '--distance',
        type=float,
        default=10.0,
        help='Search radius in miles (default: 10)'
    )
    
    parser.add_argument(
        '-t', '--types',
        required=True,
        help='Comma-separated place types (e.g., "restaurant,gym,pharmacy")'
    )
    
    parser.add_argument(
        '--emails',
        action='store_true',
        help='Extract email addresses from business websites'
    )
    
    parser.add_argument(
        '--agents',
        action='store_true',
        help='Match against Colorado SOS database to find registered agent names'
    )
    
    parser.add_argument(
        '--no-details',
        action='store_true',
        help='Skip fetching detailed place information (faster but less data)'
    )
    
    parser.add_argument(
        '-o', '--output',
        default='MarketResearch.xlsx',
        help='Output file path (default: MarketResearch.xlsx)'
    )
    
    parser.add_argument(
        '--csv',
        action='store_true',
        help='Export as CSV instead of Excel'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Show detailed progress information'
    )
    
    return parser.parse_args()


def main():
    """Main CLI function."""
    args = parse_args()
    
    # Check API key
    if not GOOGLE_MAPS_API_KEY:
        print("Error: GOOGLE_MAPS_API_KEY not found in environment variables.")
        print("Please set it in your .env file or export it as an environment variable.")
        sys.exit(1)
    
    # Parse place types
    place_types = [pt.strip().lower() for pt in args.types.split(',') if pt.strip()]
    
    if not place_types:
        print("Error: Please provide at least one place type.")
        sys.exit(1)
    
    print(f"\n🔍 Searching for places near: {args.location}")
    print(f"   Distance: {args.distance} miles")
    print(f"   Types: {', '.join(place_types)}")
    print()
    
    # Progress callback for verbose mode
    def progress_callback(place_type, count):
        if args.verbose:
            print(f"   Found {count} results for '{place_type}'")
    
    # Search for places
    try:
        places = search_places(
            api_key=GOOGLE_MAPS_API_KEY,
            location=args.location,
            distance_miles=args.distance,
            place_types=place_types,
            fetch_details=not args.no_details,
            progress_callback=progress_callback if args.verbose else None
        )
    except Exception as e:
        print(f"Error during search: {e}")
        sys.exit(1)
    
    if not places:
        print("No places found. Try different search criteria.")
        sys.exit(0)
    
    print(f"✅ Found {len(places)} places")
    
    # Extract emails if requested
    if args.emails:
        print("\n📧 Extracting emails from websites...")
        
        def email_progress(completed, total):
            if args.verbose:
                print(f"   Progress: {completed}/{total}", end='\r')
        
        places = enrich_places_with_emails(
            places,
            progress_callback=email_progress if args.verbose else None
        )
        
        if args.verbose:
            print()
    
    # Convert to DataFrame and clean
    df = places_to_dataframe(places)
    df = clean_dataframe(df, max_distance=args.distance)
    
    # Match against Colorado SOS if requested
    if args.agents:
        print("\n🏛️ Matching against Colorado Secretary of State database...")
        
        def agent_progress(completed, total):
            if args.verbose:
                print(f"   Progress: {completed}/{total}", end='\r')
        
        df = enrich_with_agent_names(
            df,
            progress_callback=agent_progress if args.verbose else None
        )
        
        if args.verbose:
            print()
    
    # Get statistics
    stats = get_summary_stats(df)
    
    # Calculate match counts
    agent_count = 0
    business_name_count = 0
    if 'AgentName' in df.columns:
        agent_count = (df['AgentName'].notna() & (df['AgentName'] != '')).sum()
    if 'BusinessName' in df.columns:
        business_name_count = (df['BusinessName'].notna() & (df['BusinessName'] != '')).sum()
    
    print(f"\n📊 Summary:")
    print(f"   Total places: {stats['total_places']}")
    print(f"   With phone: {stats['with_phone']}")
    print(f"   With website: {stats['with_website']}")
    print(f"   With email: {stats['with_email']}")
    if args.agents:
        print(f"   SOS matched: {business_name_count}")
        print(f"   With agent name: {agent_count}")
    print(f"   Average distance: {stats.get('avg_distance', 'N/A')} miles")
    
    # Determine output path
    output_path = Path(args.output)
    
    if args.csv:
        if output_path.suffix.lower() != '.csv':
            output_path = output_path.with_suffix('.csv')
        export_to_csv(df, output_path)
    else:
        if output_path.suffix.lower() != '.xlsx':
            output_path = output_path.with_suffix('.xlsx')
        export_to_excel(df, output_path)
    
    print(f"\n💾 Results saved to: {output_path}")
    
    # Show sample if verbose
    if args.verbose and len(df) > 0:
        sample_cols = ['Name', 'Distance', 'Phone']
        if 'Email' in df.columns:
            sample_cols.append('Email')
        if 'BusinessName' in df.columns:
            sample_cols.append('BusinessName')
        if 'AgentName' in df.columns:
            sample_cols.append('AgentName')
        print("\n📝 Sample results:")
        print(df[sample_cols].head().to_string(index=False))
    
    print("\n✨ Done!")


if __name__ == "__main__":
    main()