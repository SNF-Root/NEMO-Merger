#!/usr/bin/env python3
"""
Script to download billing rate types from NEMO API and save them locally.
This will help map different rate types for projects and billing entities.
"""

import requests
import json
import os
from dotenv import load_dotenv
from typing import List, Dict, Any

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

def download_rate_types() -> List[Dict[str, Any]]:
    """Download all billing rate types from the NEMO API."""
    try:
        print("Downloading billing rate types from NEMO API...")
        response = requests.get(NEMO_RATE_TYPES_API_URL, headers=API_HEADERS)
        
        if response.status_code == 200:
            response_data = response.json()
            print(f"✓ Successfully downloaded response with {response_data.get('count', 0)} billing rate types")
            
            # Extract the results array from the paginated response
            if 'results' in response_data and isinstance(response_data['results'], list):
                rate_types = response_data['results']
                print(f"✓ Extracted {len(rate_types)} rate types from results")
                return rate_types
            else:
                print("✗ No 'results' array found in response")
                return []
        else:
            print(f"✗ Failed to download billing rate types: HTTP {response.status_code} - {response.text}")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error downloading billing rate types: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"✗ Error parsing JSON response: {e}")
        return []

def save_rate_types_to_file(rate_types: List[Dict[str, Any]], filename: str = "nemo_billing_rate_types.json"):
    """Save billing rate types to a local JSON file."""
    try:
        with open(filename, 'w') as f:
            json.dump(rate_types, f, indent=2)
        print(f"✓ Successfully saved {len(rate_types)} billing rate types to {filename}")
    except Exception as e:
        print(f"✗ Error saving billing rate types to file: {e}")

def create_rate_type_lookup(rate_types: List[Dict[str, Any]]) -> Dict[str, int]:
    """Create a lookup dictionary mapping rate type names to IDs."""
    lookup = {}
    for rate_type in rate_types:
        if 'type' in rate_type and 'id' in rate_type:
            lookup[rate_type['type']] = rate_type['id']
    
    print(f"✓ Created lookup for {len(lookup)} billing rate types")
    return lookup

def main():
    """Main function to download and save billing rate types."""
    print("Starting billing rate types download from NEMO API...")
    print(f"API Endpoint: {NEMO_RATE_TYPES_API_URL}")
    print("-" * 60)
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Download billing rate types
    rate_types = download_rate_types()
    
    if not rate_types:
        print("No billing rate types downloaded. Cannot proceed.")
        return
    
    # Save billing rate types to file
    save_rate_types_to_file(rate_types)
    
    # Create and save rate type lookup
    rate_type_lookup = create_rate_type_lookup(rate_types)
    
    # Save lookup to a separate file for easy access
    with open("billing_rate_type_lookup.json", 'w') as f:
        json.dump(rate_type_lookup, f, indent=2)
    print("✓ Saved billing rate type lookup to billing_rate_type_lookup.json")
    
    # Show the final lookup
    print("\nFinal billing rate type lookup:")
    for name, rt_id in rate_type_lookup.items():
        print(f"  {name} → ID {rt_id}")
    
    print(f"\n✓ Billing rate types download complete! {len(rate_types)} types saved locally.")
    print("You can now use these rate types for project creation and billing configuration.")

if __name__ == "__main__":
    main()
