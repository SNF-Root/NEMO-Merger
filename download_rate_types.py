#!/usr/bin/env python3
"""
Script to download all billing rate types from the NEMO API and create a lookup mapping.
This creates a mapping from rate type names to rate type IDs for use in rate creation.
"""

import requests
import json
import os
from dotenv import load_dotenv
from typing import Dict, Any

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoint for billing rate types
NEMO_RATE_TYPES_API_URL = "https://nemo-plan.stanford.edu/api/billing/rate_types/"

# Get NEMO token from environment
NEMO_TOKEN = os.getenv('NEMO_TOKEN')
if not NEMO_TOKEN:
    print("Error: NEMO_TOKEN not found in environment variables or .env file")
    print("Please create a .env file with: NEMO_TOKEN=your_token_here")
    print("Or set the environment variable: export NEMO_TOKEN=your_token_here")
    exit(1)

# API headers with authentication
API_HEADERS = {
    'Authorization': f'Token {NEMO_TOKEN}',
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

def test_api_connection():
    """Test the API connection and authentication."""
    try:
        response = requests.get(NEMO_RATE_TYPES_API_URL, headers=API_HEADERS)
        if response.status_code == 200:
            print("✓ API connection successful")
            return True
        elif response.status_code == 401:
            print("✗ Authentication failed: Check your NEMO_TOKEN")
            return False
        elif response.status_code == 403:
            print("✗ Permission denied: Check your API permissions")
            return False
        else:
            print(f"✗ API connection failed: HTTP {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error connecting to API: {e}")
        return False

def download_rate_types(api_url: str) -> list:
    """Download all billing rate types from the NEMO API."""
    print("Downloading billing rate types from NEMO API...")
    
    try:
        response = requests.get(api_url, headers=API_HEADERS)
        
        if response.status_code == 200:
            response_data = response.json()
            
            # Check if this is a paginated response
            if 'results' in response_data:
                rate_types = response_data['results']
                print(f"✓ Retrieved {len(rate_types)} rate types from paginated response")
            else:
                # Direct list response
                rate_types = response_data
                print(f"✓ Retrieved {len(rate_types)} rate types from direct response")
            
            return rate_types
            
        elif response.status_code == 401:
            print("✗ Authentication failed: Check your NEMO_TOKEN")
            return []
        elif response.status_code == 403:
            print("✗ Permission denied: Check your API permissions")
            return []
        else:
            print(f"✗ Failed to download rate types: HTTP {response.status_code}")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error downloading rate types: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"✗ Error parsing API response: {e}")
        return []

def save_rate_types_to_json(rate_types: list, filename: str = "billing_rate_types_download.json"):
    """Save the downloaded rate types to a JSON file."""
    try:
        with open(filename, 'w') as f:
            json.dump(rate_types, f, indent=2)
        print(f"✓ Rate types saved to {filename}")
    except Exception as e:
        print(f"✗ Error saving rate types to {filename}: {e}")

def create_rate_type_lookup(rate_types: list) -> Dict[str, int]:
    """Create a lookup mapping from rate type names to rate type IDs."""
    rate_type_lookup = {}
    
    for rate_type in rate_types:
        if 'type' in rate_type and 'id' in rate_type:
            type_name = rate_type['type']
            type_id = rate_type['id']
            rate_type_lookup[type_name] = type_id
    
    print(f"✓ Created rate type lookup with {len(rate_type_lookup)} types")
    return rate_type_lookup

def save_rate_type_lookup(rate_type_lookup: Dict[str, int], filename: str = "billing_rate_type_lookup.json"):
    """Save the rate type lookup to a JSON file."""
    try:
        with open(filename, 'w') as f:
            json.dump(rate_type_lookup, f, indent=2)
        print(f"✓ Rate type lookup saved to {filename}")
    except Exception as e:
        print(f"✗ Error saving rate type lookup to {filename}: {e}")

def main():
    """Main function to download rate types and create lookup."""
    print("Starting billing rate type download from NEMO API...")
    print(f"API Endpoint: {NEMO_RATE_TYPES_API_URL}")
    print("-" * 60)
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Download rate types
    rate_types = download_rate_types(NEMO_RATE_TYPES_API_URL)
    
    if not rate_types:
        print("No rate types downloaded. Cannot proceed.")
        return
    
    # Save raw rate types data
    save_rate_types_to_json(rate_types)
    
    # Create and save rate type lookup
    rate_type_lookup = create_rate_type_lookup(rate_types)
    save_rate_type_lookup(rate_type_lookup)
    
    # Show the complete lookup
    print("\nComplete billing rate type lookup:")
    for type_name, type_id in rate_type_lookup.items():
        print(f"  {type_name} → ID {type_id}")
    
    print("\n" + "=" * 60)
    print("RATE TYPE DOWNLOAD SUMMARY")
    print("=" * 60)
    print(f"Total rate types downloaded: {len(rate_types)}")
    print(f"Rate types in lookup: {len(rate_type_lookup)}")
    print(f"✓ Rate type lookup ready for use in rate creation!")
    print("\nFiles created:")
    print(f"  - billing_rate_types_download.json (raw rate type data)")
    print(f"  - billing_rate_type_lookup.json (type name → ID mapping)")

if __name__ == "__main__":
    main()
