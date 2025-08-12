#!/usr/bin/env python3
"""
Script to create a new billing rate type in NEMO.
This creates a TOOL_STAFF_CHARGE rate type that is both item_specific and category_specific.
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

def create_rate_type_payload() -> Dict[str, Any]:
    """Create a payload for the new TOOL_STAFF_CHARGE rate type."""
    payload = {
        "type": "TOOL_STAFF_CHARGE",
        "category_specific": True,
        "item_specific": True
    }
    return payload

def create_rate_type(api_url: str, payload: Dict[str, Any]) -> bool:
    """Create a new rate type via the NEMO API."""
    print("Creating new rate type: TOOL_STAFF_CHARGE...")
    
    try:
        response = requests.post(api_url, json=payload, headers=API_HEADERS)
        
        if response.status_code == 200:  # Created
            print("✓ Successfully created TOOL_STAFF_CHARGE rate type")
            response_data = response.json()
            if 'id' in response_data:
                print(f"  → New rate type ID: {response_data['id']}")
            return True
        elif response.status_code == 400:
            print(f"✗ Bad request: {response.text}")
            return False
        elif response.status_code == 401:
            print("✗ Authentication failed: Check your NEMO_TOKEN")
            return False
        elif response.status_code == 403:
            print("✗ Permission denied: Check your API permissions")
            return False
        elif response.status_code == 409:
            print("⚠ Rate type 'TOOL_STAFF_CHARGE' already exists (conflict)")
            return False
        else:
            print(f"✗ Failed to create rate type: HTTP {response.status_code} - {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error creating rate type: {e}")
        return False

def main():
    """Main function to create the new rate type."""
    print("Starting creation of new billing rate type...")
    print(f"API Endpoint: {NEMO_RATE_TYPES_API_URL}")
    print("-" * 60)
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Create the rate type payload
    payload = create_rate_type_payload()
    print(f"Rate type payload: {json.dumps(payload, indent=2)}")
    
    # Create the rate type
    if create_rate_type(NEMO_RATE_TYPES_API_URL, payload):
        print("\n" + "=" * 60)
        print("RATE TYPE CREATION SUCCESS")
        print("=" * 60)
        print("✓ TOOL_STAFF_CHARGE rate type created successfully!")
        print("✓ This rate type is both item_specific and category_specific")
        print("✓ You can now use this for tool-specific staff charges")
        print("\nNext steps:")
        print("1. Run download_rate_types.py to get the updated list")
        print("2. Update create_rates.py to use TOOL_STAFF_CHARGE instead of STAFF_CHARGE")
    else:
        print("\n" + "=" * 60)
        print("RATE TYPE CREATION FAILED")
        print("=" * 60)
        print("✗ Could not create the new rate type")
        print("Check the error messages above for details")

if __name__ == "__main__":
    main()
