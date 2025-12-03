#!/usr/bin/env python3
"""
Script to download all consumables from NEMO API and check for duplicate names.
"""

import requests
import json
import os
from typing import List, Dict, Any
from collections import defaultdict
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoint
NEMO_API_URL = "https://nemo.stanford.edu/api/consumables/"

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

def test_api_connection() -> bool:
    """Test the API connection and authentication."""
    try:
        response = requests.get(NEMO_API_URL, headers=API_HEADERS)
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

def download_all_consumables() -> List[Dict[str, Any]]:
    """Download all consumables from the NEMO API."""
    print("Downloading consumables from NEMO API...")
    
    all_consumables = []
    page = 1
    
    while True:
        try:
            # Add pagination parameters
            params = {'page': page}
            response = requests.get(NEMO_API_URL, headers=API_HEADERS, params=params)
            
            if response.status_code == 200:
                response_data = response.json()
                
                # Check if this is a paginated response
                if 'results' in response_data:
                    consumables = response_data['results']
                    print(f"  Page {page}: Retrieved {len(consumables)} consumables")
                else:
                    # Direct list response
                    consumables = response_data
                    print(f"  Retrieved {len(consumables)} consumables (no pagination)")
                
                if not consumables:
                    break
                
                all_consumables.extend(consumables)
                
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
                print(f"✗ Failed to download consumables: HTTP {response.status_code} - {response.text}")
                return []
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Network error downloading consumables: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"✗ Error parsing JSON response: {e}")
            return []
    
    print(f"✓ Successfully downloaded {len(all_consumables)} consumables")
    return all_consumables

def find_duplicate_names(consumables: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Find consumables with duplicate names."""
    name_to_consumables = defaultdict(list)
    
    for consumable in consumables:
        name = consumable.get('name', '').strip()
        if name:  # Only process consumables with names
            name_to_consumables[name].append({
                'id': consumable.get('id'),
                'name': name,
                'quantity': consumable.get('quantity'),
                'reusable': consumable.get('reusable'),
                'visible': consumable.get('visible'),
            })
    
    # Filter to only return duplicates (names that appear more than once)
    duplicates = {name: items for name, items in name_to_consumables.items() if len(items) > 1}
    
    return duplicates

def main():
    """Main function to check for duplicate consumables."""
    print("=" * 60)
    print("NEMO Consumables Duplicate Checker")
    print("=" * 60)
    print()
    
    # Test API connection
    if not test_api_connection():
        print("Exiting due to API connection failure.")
        exit(1)
    
    print()
    
    # Download all consumables
    consumables = download_all_consumables()
    
    if not consumables:
        print("No consumables found. Exiting.")
        exit(1)
    
    print()
    print("Checking for duplicate names...")
    print()
    
    # Find duplicates
    duplicates = find_duplicate_names(consumables)
    
    # Report results
    print("=" * 60)
    print("Duplicate Name Check Results")
    print("=" * 60)
    print(f"Total consumables: {len(consumables)}")
    print(f"Unique consumable names: {len(set(c.get('name', '').strip() for c in consumables if c.get('name', '').strip()))}")
    print(f"Consumables with duplicate names: {len(duplicates)}")
    print()
    
    if duplicates:
        print("⚠️  DUPLICATE NAMES FOUND:")
        print()
        for name, items in sorted(duplicates.items()):
            print(f"Name: '{name}' ({len(items)} occurrences)")
            for item in items:
                print(f"  - ID: {item['id']}, Quantity: {item['quantity']}, "
                      f"Reusable: {item['reusable']}, Visible: {item['visible']}")
            print()
        
        # Save duplicates to JSON file
        output_file = "duplicate_consumables.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(duplicates, f, indent=2)
        print(f"✓ Duplicate consumables saved to: {output_file}")
    else:
        print("✓ No duplicate names found!")
    
    print("=" * 60)

if __name__ == "__main__":
    main()

