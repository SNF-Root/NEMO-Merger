#!/usr/bin/env python3
"""
Script to download account types from NEMO API and save them to JSON.
"""

import requests
import json
import os
from dotenv import load_dotenv
from typing import List, Dict, Any

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoint for account types
NEMO_ACCOUNT_TYPES_API_URL = "https://nemo.stanford.edu/api/account_types/"

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
        response = requests.get(NEMO_ACCOUNT_TYPES_API_URL, headers=API_HEADERS)
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

def download_account_types() -> List[Dict[str, Any]]:
    """Download all account types from the NEMO API."""
    try:
        print("Downloading account types from NEMO API...")
        response = requests.get(NEMO_ACCOUNT_TYPES_API_URL, headers=API_HEADERS)
        
        if response.status_code == 200:
            account_types = response.json()
            print(f"✓ Successfully downloaded {len(account_types)} account types")
            return account_types
        else:
            print(f"✗ Failed to download account types: HTTP {response.status_code} - {response.text}")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error downloading account types: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"✗ Error parsing JSON response: {e}")
        return []

def save_account_types_to_file(account_types: List[Dict[str, Any]], filename: str = "nemo_account_types.json"):
    """Save account types to a local JSON file."""
    try:
        with open(filename, 'w') as f:
            json.dump(account_types, f, indent=2)
        print(f"✓ Successfully saved {len(account_types)} account types to {filename}")
    except Exception as e:
        print(f"✗ Error saving account types to file: {e}")

def main():
    """Main function to download and save account types."""
    print("Starting account type download from NEMO API...")
    print(f"API Endpoint: {NEMO_ACCOUNT_TYPES_API_URL}")
    print("-" * 60)
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Download account types
    account_types = download_account_types()
    
    if not account_types:
        print("No account types downloaded. Cannot proceed.")
        return
    
    # Save account types to file
    save_account_types_to_file(account_types)
    
    # Show the downloaded account types
    print("\nDownloaded account types:")
    for account_type in account_types:
        if 'name' in account_type and 'id' in account_type:
            print(f"  ID {account_type['id']}: {account_type['name']}")
    
    print(f"\n✓ Account type download complete! {len(account_types)} types saved to nemo_account_types.json")

if __name__ == "__main__":
    main()

