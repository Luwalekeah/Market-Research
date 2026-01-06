"""
Unit tests for the Market Research App.
"""
import pytest
import pandas as pd
from unittest.mock import Mock, patch

from src.places import calculate_distance, geocode_location
from src.email_extractor import (
    is_valid_email,
    extract_emails_from_text,
    EMAIL_PATTERN
)
from src.data_utils import (
    places_to_dataframe,
    clean_dataframe,
    get_summary_stats
)


class TestCalculateDistance:
    """Tests for distance calculation."""
    
    def test_same_location(self):
        """Distance between same point should be 0."""
        origin = (39.7392, -104.9903)  # Denver
        result = calculate_distance(origin, origin)
        assert result == 0.0
    
    def test_known_distance(self):
        """Test distance between two known points."""
        denver = (39.7392, -104.9903)
        boulder = (40.0150, -105.2705)
        
        result = calculate_distance(denver, boulder)
        
        # Distance should be approximately 25-30 miles
        assert 25 < result < 35
    
    def test_symmetric(self):
        """Distance should be same in both directions."""
        point_a = (39.7392, -104.9903)
        point_b = (40.0150, -105.2705)
        
        dist_ab = calculate_distance(point_a, point_b)
        dist_ba = calculate_distance(point_b, point_a)
        
        assert abs(dist_ab - dist_ba) < 0.001


class TestEmailValidation:
    """Tests for email validation."""
    
    def test_valid_emails(self):
        """Test valid email addresses."""
        valid_emails = [
            "test@example.org",
            "user.name@domain.com",
            "contact@business.co.uk",
            "info@company.io",
        ]
        
        for email in valid_emails:
            assert is_valid_email(email), f"{email} should be valid"
    
    def test_invalid_emails(self):
        """Test invalid email addresses."""
        invalid_emails = [
            "test@example.com",  # Excluded domain
            "user@domain.com",   # Excluded domain
            "image.png",         # File extension
            "style.css",         # File extension
        ]
        
        for email in invalid_emails:
            assert not is_valid_email(email), f"{email} should be invalid"
    
    def test_extract_from_text(self):
        """Test email extraction from text."""
        text = """
        Contact us at info@realcompany.org or sales@realcompany.org
        For support: support@realcompany.org
        Fake email: test@example.com should be ignored
        """
        
        emails = extract_emails_from_text(text)
        
        assert len(emails) > 0
        assert "info@realcompany.org" in emails
        assert "test@example.com" not in emails


class TestDataUtils:
    """Tests for data utilities."""
    
    @pytest.fixture
    def sample_places(self):
        """Create sample place data."""
        return [
            {
                'place_id': '1',
                'name': 'Business A',
                'address': '123 Main St',
                'type': 'restaurant',
                'distance_miles': 1.5,
                'phone': '555-0001',
                'website': 'https://businessa.com',
                'email': 'contact@businessa.com',
                'latitude': 39.7392,
                'longitude': -104.9903,
            },
            {
                'place_id': '2',
                'name': 'Business B',
                'address': '456 Oak Ave',
                'type': 'gym',
                'distance_miles': 3.2,
                'phone': '',
                'website': '',
                'email': '',
                'latitude': 39.7500,
                'longitude': -104.9800,
            },
            {
                'place_id': '1',  # Duplicate
                'name': 'Business A',
                'address': '123 Main St',
                'type': 'restaurant',
                'distance_miles': 1.5,
                'phone': '555-0001',
                'website': 'https://businessa.com',
                'email': 'contact@businessa.com',
                'latitude': 39.7392,
                'longitude': -104.9903,
            },
        ]
    
    def test_places_to_dataframe(self, sample_places):
        """Test converting places to DataFrame."""
        df = places_to_dataframe(sample_places)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert 'Name' in df.columns
        assert 'Distance' in df.columns
    
    def test_clean_dataframe_removes_duplicates(self, sample_places):
        """Test that duplicates are removed."""
        df = places_to_dataframe(sample_places)
        cleaned = clean_dataframe(df, remove_duplicates=True)
        
        assert len(cleaned) == 2
    
    def test_clean_dataframe_sorts_by_distance(self, sample_places):
        """Test sorting by distance."""
        df = places_to_dataframe(sample_places)
        cleaned = clean_dataframe(df, sort_by_distance=True)
        
        distances = cleaned['Distance'].tolist()
        assert distances == sorted(distances)
    
    def test_clean_dataframe_filters_by_distance(self, sample_places):
        """Test filtering by max distance."""
        df = places_to_dataframe(sample_places)
        cleaned = clean_dataframe(df, max_distance=2.0)
        
        assert len(cleaned) == 1
        assert cleaned.iloc[0]['Distance'] <= 2.0
    
    def test_get_summary_stats(self, sample_places):
        """Test summary statistics."""
        df = places_to_dataframe(sample_places)
        df = clean_dataframe(df)
        stats = get_summary_stats(df)
        
        assert stats['total_places'] == 2
        assert stats['with_phone'] == 1
        assert stats['with_website'] == 1
        assert stats['with_email'] == 1
    
    def test_empty_dataframe(self):
        """Test handling of empty data."""
        df = places_to_dataframe([])
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        
        stats = get_summary_stats(df)
        assert stats['total_places'] == 0


class TestEmailPattern:
    """Tests for email regex pattern."""
    
    def test_pattern_matches_valid(self):
        """Test that pattern matches valid emails."""
        valid = [
            "test@test.com",
            "user.name@domain.org",
            "contact+info@company.co.uk",
        ]
        
        for email in valid:
            assert EMAIL_PATTERN.match(email), f"Pattern should match {email}"
    
    def test_pattern_no_false_positives(self):
        """Test pattern doesn't match invalid strings."""
        invalid = [
            "not an email",
            "@nodomain.com",
            "noatsign.com",
        ]
        
        for text in invalid:
            match = EMAIL_PATTERN.match(text)
            # For full-string match, should not match
            if match:
                assert match.group() != text


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
