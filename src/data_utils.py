"""
Utilities for exporting and processing place data.
"""
import io
import pandas as pd
from pathlib import Path
from typing import Union


def places_to_dataframe(places: list) -> pd.DataFrame:
    """
    Convert a list of place dictionaries to a pandas DataFrame.
    
    Args:
        places: List of place dictionaries
    
    Returns:
        DataFrame with place data
    """
    if not places:
        return pd.DataFrame()
    
    df = pd.DataFrame(places)
    
    # Standardize column names (convert to Title Case)
    column_mapping = {
        'place_id': 'Place_ID',
        'name': 'Name',
        'address': 'Address',
        'type': 'Type',
        'search_term': 'Search_Term',
        'distance_miles': 'Distance',
        'latitude': 'Latitude',
        'longitude': 'Longitude',
        'phone': 'Phone',
        'website': 'Website',
        'email': 'Email',
        'opening_hours': 'Opening_Hours',
        'rating': 'Rating',
        'review_count': 'Review_Count'
    }
    
    df = df.rename(columns=column_mapping)
    
    return df


def clean_dataframe(
    df: pd.DataFrame,
    remove_duplicates: bool = True,
    sort_by_distance: bool = True,
    max_distance: float = None
) -> pd.DataFrame:
    """
    Clean and process a places DataFrame.
    
    Args:
        df: DataFrame to clean
        remove_duplicates: Remove duplicate places by Place_ID
        sort_by_distance: Sort results by distance (ascending)
        max_distance: Filter out places beyond this distance
    
    Returns:
        Cleaned DataFrame
    """
    if df.empty:
        return df
    
    result = df.copy()
    
    # Remove duplicates
    if remove_duplicates and 'Place_ID' in result.columns:
        result = result.drop_duplicates(subset='Place_ID')
    
    # Filter by distance
    if max_distance is not None and 'Distance' in result.columns:
        result = result[result['Distance'] <= max_distance]
    
    # Sort by distance
    if sort_by_distance and 'Distance' in result.columns:
        result = result.sort_values(by='Distance')
    
    # Reset index
    result = result.reset_index(drop=True)
    
    return result


def export_to_excel(
    df: pd.DataFrame,
    filepath: Union[str, Path] = None,
    return_bytes: bool = False
) -> Union[None, bytes]:
    """
    Export DataFrame to Excel file.
    
    Args:
        df: DataFrame to export
        filepath: Output file path (ignored if return_bytes=True)
        return_bytes: If True, return bytes instead of writing to file
    
    Returns:
        None if writing to file, bytes if return_bytes=True
    """
    if return_bytes:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Places')
            
            # Auto-adjust column widths
            worksheet = writer.sheets['Places']
            for idx, col in enumerate(df.columns):
                max_len = max(
                    df[col].astype(str).map(len).max(),
                    len(col)
                ) + 2
                worksheet.set_column(idx, idx, min(max_len, 50))
        
        return buffer.getvalue()
    else:
        with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Places')
            
            worksheet = writer.sheets['Places']
            for idx, col in enumerate(df.columns):
                max_len = max(
                    df[col].astype(str).map(len).max(),
                    len(col)
                ) + 2
                worksheet.set_column(idx, idx, min(max_len, 50))
        
        return None


def export_to_csv(
    df: pd.DataFrame,
    filepath: Union[str, Path] = None,
    return_bytes: bool = False
) -> Union[None, bytes]:
    """
    Export DataFrame to CSV file.
    
    Args:
        df: DataFrame to export
        filepath: Output file path (ignored if return_bytes=True)
        return_bytes: If True, return bytes instead of writing to file
    
    Returns:
        None if writing to file, bytes if return_bytes=True
    """
    if return_bytes:
        return df.to_csv(index=False).encode('utf-8')
    else:
        df.to_csv(filepath, index=False)
        return None


def get_summary_stats(df: pd.DataFrame) -> dict:
    """
    Get summary statistics for a places DataFrame.
    
    Args:
        df: DataFrame with place data
    
    Returns:
        Dictionary of summary statistics
    """
    if df.empty:
        return {
            'total_places': 0,
            'unique_types': 0,
            'with_phone': 0,
            'with_website': 0,
            'with_email': 0,
            'avg_distance': 0,
            'avg_rating': 0
        }
    
    stats = {
        'total_places': len(df),
        'unique_types': df['Type'].nunique() if 'Type' in df.columns else 0,
        'with_phone': (df['Phone'].notna() & (df['Phone'] != '')).sum() if 'Phone' in df.columns else 0,
        'with_website': (df['Website'].notna() & (df['Website'] != '')).sum() if 'Website' in df.columns else 0,
        'with_email': (df['Email'].notna() & (df['Email'] != '')).sum() if 'Email' in df.columns else 0,
    }
    
    if 'Distance' in df.columns:
        stats['avg_distance'] = round(df['Distance'].mean(), 2)
        stats['min_distance'] = round(df['Distance'].min(), 2)
        stats['max_distance'] = round(df['Distance'].max(), 2)
    
    if 'Rating' in df.columns:
        numeric_ratings = pd.to_numeric(df['Rating'], errors='coerce')
        stats['avg_rating'] = round(numeric_ratings.mean(), 2) if not numeric_ratings.isna().all() else 'N/A'
    
    return stats
