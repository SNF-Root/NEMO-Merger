#!/usr/bin/env python3
"""
Script to download rate categories from NEMO API and create a mapping for account types.
This will help map Excel types to the correct NEMO rate category IDs.
"""

import requests
import json
import os
from dotenv import load_dotenv
from typing import List, Dict, Any

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoint for rate categories
NEMO_RATE_CATEGORIES_API_URL = "https://nemo-plan.stanford.edu/api/billing/rate_categories/"

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
        response = requests.get(NEMO_RATE_CATEGORIES_API_URL, headers=API_HEADERS)
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

def download_rate_categories() -> List[Dict[str, Any]]:
    """Download all rate categories from the NEMO API."""
    try:
        print("Downloading rate categories from NEMO API...")
        response = requests.get(NEMO_RATE_CATEGORIES_API_URL, headers=API_HEADERS)
        
        if response.status_code == 200:
            rate_categories = response.json()
            print(f"✓ Successfully downloaded {len(rate_categories)} rate categories")
            return rate_categories
        else:
            print(f"✗ Failed to download rate categories: HTTP {response.status_code} - {response.text}")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error downloading rate categories: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"✗ Error parsing JSON response: {e}")
        return []

def save_rate_categories_to_file(rate_categories: List[Dict[str, Any]], filename: str = "nemo_rate_categories.json"):
    """Save rate categories to a local JSON file."""
    try:
        with open(filename, 'w') as f:
            json.dump(rate_categories, f, indent=2)
        print(f"✓ Successfully saved {len(rate_categories)} rate categories to {filename}")
    except Exception as e:
        print(f"✗ Error saving rate categories to file: {e}")

def create_rate_category_mapping(rate_categories: List[Dict[str, Any]]) -> Dict[str, int]:
    """Create a mapping from Excel type values to NEMO rate category IDs."""
    mapping = {}
    
    # Show all available rate categories
    print("\nAvailable rate categories:")
    for category in rate_categories:
        if 'name' in category and 'id' in category:
            print(f"  ID {category['id']}: {category['name']}")
    
    # Create the mapping based on the user's requirements
    excel_to_nemo_mapping = {
        "local": "Academic",
        "industrial": "Industry", 
        "no charge": "No Charge",
        "other academic": "Academic",  # Default to Academic
        "industrial-sbir": "Industry",  # Default to Industry
        "foreign": "Academic"  # Default to Academic
    }
    
    # Find the IDs for each category name
    for excel_type, nem_name in excel_to_nemo_mapping.items():
        found = False
        for category in rate_categories:
            if category.get('name') == nem_name:
                mapping[excel_type] = category['id']
                found = True
                print(f"✓ Mapped '{excel_type}' → '{nem_name}' (ID: {category['id']})")
                break
        
        if not found:
            print(f"⚠ Warning: Could not find rate category '{nem_name}' for Excel type '{excel_type}'")
            # Default to Academic if not found
            for category in rate_categories:
                if category.get('name') == "Academic":
                    mapping[excel_type] = category['id']
                    print(f"  → Defaulting '{excel_type}' to Academic (ID: {category['id']})")
                    break
    
    return mapping

def main():
    """Main function to download and save rate categories."""
    print("Starting rate category download from NEMO API...")
    print(f"API Endpoint: {NEMO_RATE_CATEGORIES_API_URL}")
    print("-" * 60)
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Download rate categories
    rate_categories = download_rate_categories()
    
    if not rate_categories:
        print("No rate categories downloaded. Cannot proceed.")
        return
    
    # Save rate categories to file
    save_rate_categories_to_file(rate_categories)
    
    # Create and save rate category mapping
    rate_mapping = create_rate_category_mapping(rate_categories)
    
    # Save mapping to a separate file for easy access
    with open("rate_category_mapping.json", 'w') as f:
        json.dump(rate_mapping, f, indent=2)
    print("\n✓ Saved rate category mapping to rate_category_mapping.json")
    
    # Show the final mapping
    print("\nFinal Excel to NEMO rate category mapping:")
    for excel_type, nem_id in rate_mapping.items():
        print(f"  {excel_type} → ID {nem_id}")
    
    print(f"\n✓ Rate category download complete! {len(rate_categories)} categories saved locally.")
    print("You can now run create_accounts.py to create accounts with the correct rate categories.")

if __name__ == "__main__":
    main()
