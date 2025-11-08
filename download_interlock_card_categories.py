#!/usr/bin/env python3
"""
Script to download all interlock card categories from the NEMO API and create a lookup mapping.
This creates a mapping from category names to category IDs for use in interlock creation.
"""

import requests
import json
import os
from dotenv import load_dotenv
from typing import Dict, Any

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoint for interlock card categories
NEMO_INTERLOCK_CARD_CATEGORIES_API_URL = "https://nemo.stanford.edu/api/interlock_card_categories/"

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
        response = requests.get(NEMO_INTERLOCK_CARD_CATEGORIES_API_URL, headers=API_HEADERS)
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

def download_interlock_card_categories(api_url: str) -> list:
    """Download all interlock card categories from the NEMO API."""
    print("Downloading interlock card categories from NEMO API...")
    
    all_categories = []
    page = 1
    
    while True:
        try:
            # Add pagination parameters
            params = {'page': page}
            response = requests.get(api_url, headers=API_HEADERS, params=params)
            
            if response.status_code == 200:
                response_data = response.json()
                
                # Check if this is a paginated response
                if 'results' in response_data:
                    categories = response_data['results']
                    print(f"  Page {page}: Retrieved {len(categories)} categories")
                else:
                    # Direct list response
                    categories = response_data
                    print(f"  Retrieved {len(categories)} categories (no pagination)")
                
                if not categories:
                    break
                
                all_categories.extend(categories)
                
                # Check if there are more pages
                if 'next' in response_data and response_data['next']:
                    page += 1
                else:
                    break
                    
            elif response.status_code == 401:
                print("✗ Authentication failed: Check your NEMO_TOKEN")
                return []
            elif response.status_code == 403:
                print("✗ Permission denied: Check your API permissions")
                return []
            else:
                print(f"✗ Failed to download categories: HTTP {response.status_code}")
                return []
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Network error downloading categories: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"✗ Error parsing API response: {e}")
            return []
    
    print(f"✓ Total categories downloaded: {len(all_categories)}")
    return all_categories

def save_categories_to_json(categories: list, filename: str = "interlock_card_categories_download.json"):
    """Save the downloaded categories to a JSON file."""
    try:
        with open(filename, 'w') as f:
            json.dump(categories, f, indent=2)
        print(f"✓ Categories saved to {filename}")
    except Exception as e:
        print(f"✗ Error saving categories to {filename}: {e}")

def create_category_lookup(categories: list) -> Dict[str, int]:
    """Create a lookup mapping from category names to category IDs."""
    category_lookup = {}
    
    for category in categories:
        if 'name' in category and 'id' in category:
            category_name = category['name']
            category_id = category['id']
            category_lookup[category_name] = category_id
    
    print(f"✓ Created category lookup with {len(category_lookup)} categories")
    return category_lookup

def save_category_lookup(category_lookup: Dict[str, int], filename: str = "interlock_card_category_lookup.json"):
    """Save the category lookup to a JSON file."""
    try:
        with open(filename, 'w') as f:
            json.dump(category_lookup, f, indent=2)
        print(f"✓ Category lookup saved to {filename}")
    except Exception as e:
        print(f"✗ Error saving category lookup to {filename}: {e}")

def main():
    """Main function to download categories and create lookup."""
    print("Starting interlock card category download from NEMO API...")
    print(f"API Endpoint: {NEMO_INTERLOCK_CARD_CATEGORIES_API_URL}")
    print("-" * 60)
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Download categories
    categories = download_interlock_card_categories(NEMO_INTERLOCK_CARD_CATEGORIES_API_URL)
    
    if not categories:
        print("No categories downloaded. Cannot proceed.")
        return
    
    # Save raw categories data
    save_categories_to_json(categories)
    
    # Create and save category lookup
    category_lookup = create_category_lookup(categories)
    save_category_lookup(category_lookup)
    
    # Show the complete lookup
    print("\nComplete interlock card category lookup:")
    for category_name, category_id in category_lookup.items():
        print(f"  {category_name} → ID {category_id}")
    
    print("\n" + "=" * 60)
    print("INTERLOCK CARD CATEGORY DOWNLOAD SUMMARY")
    print("=" * 60)
    print(f"Total categories downloaded: {len(categories)}")
    print(f"Categories in lookup: {len(category_lookup)}")
    print(f"✓ Category lookup ready for use in interlock creation!")
    print("\nFiles created:")
    print(f"  - interlock_card_categories_download.json (raw category data)")
    print(f"  - interlock_card_category_lookup.json (name → ID mapping)")

if __name__ == "__main__":
    main()

