#!/usr/bin/env python3
"""
Script to download all interlock cards from the NEMO API and create a lookup file.
This creates a mapping from interlock identifiers (server:port or name) to interlock IDs
for use in checking if interlocks already exist before creating new ones.
"""

import requests
import json
import os
from dotenv import load_dotenv
from typing import Dict, Any, List

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoint for interlock cards
NEMO_INTERLOCK_CARDS_API_URL = "https://nemo.stanford.edu/api/interlock_cards/"

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
        response = requests.get(NEMO_INTERLOCK_CARDS_API_URL, headers=API_HEADERS)
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

def download_interlock_cards(api_url: str) -> List[Dict[str, Any]]:
    """Download all interlock cards from the NEMO API."""
    print("Downloading interlock cards from NEMO API...")
    
    all_cards = []
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
                    cards = response_data['results']
                    print(f"  Page {page}: Retrieved {len(cards)} interlock cards")
                else:
                    # Direct list response
                    cards = response_data
                    print(f"  Retrieved {len(cards)} interlock cards (no pagination)")
                
                if not cards:
                    break
                
                all_cards.extend(cards)
                
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
                print(f"✗ Failed to download interlock cards: HTTP {response.status_code}")
                return []
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Network error downloading interlock cards: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"✗ Error parsing API response: {e}")
            return []
    
    print(f"✓ Total interlock cards downloaded: {len(all_cards)}")
    return all_cards

def save_cards_to_json(cards: List[Dict[str, Any]], filename: str = "interlock_cards_download.json"):
    """Save the downloaded interlock cards to a JSON file."""
    try:
        with open(filename, 'w') as f:
            json.dump(cards, f, indent=2)
        print(f"✓ Interlock cards saved to {filename}")
    except Exception as e:
        print(f"✗ Error saving interlock cards to {filename}: {e}")

def create_interlock_lookup(cards: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Create a lookup mapping from interlock identifiers to interlock IDs.
    Uses server:port combination as the primary key (matching the duplicate check logic).
    Also includes name as an alternative key if available.
    """
    lookup = {}
    
    for card in cards:
        if 'id' not in card:
            continue
        
        card_id = card['id']
        server = card.get('server', '')
        port = card.get('port', '')
        name = card.get('name', '')
        
        # Primary lookup key: server:port (matching duplicate check in create script)
        if server and port is not None:
            server_port_key = f"{server}:{port}"
            if server_port_key not in lookup:
                lookup[server_port_key] = card_id
            else:
                # If duplicate server:port, keep the first one (or could log a warning)
                print(f"⚠ Warning: Duplicate server:port found: {server_port_key} (IDs: {lookup[server_port_key]}, {card_id})")
        
        # Alternative lookup key: name (if available and unique)
        if name:
            if name not in lookup:
                lookup[name] = card_id
            else:
                # If duplicate name, prefer the one with server:port key
                existing_id = lookup[name]
                existing_card = next((c for c in cards if c.get('id') == existing_id), None)
                current_card = card
                
                # If current card has server:port but existing doesn't, update
                if server and port is not None:
                    existing_server = existing_card.get('server', '') if existing_card else ''
                    existing_port = existing_card.get('port', '') if existing_card else None
                    if not (existing_server and existing_port is not None):
                        lookup[name] = card_id
    
    print(f"✓ Created interlock lookup with {len(lookup)} entries")
    return lookup

def save_interlock_lookup(lookup: Dict[str, int], filename: str = "interlock_card_lookup.json"):
    """Save the interlock lookup to a JSON file."""
    try:
        with open(filename, 'w') as f:
            json.dump(lookup, f, indent=2)
        print(f"✓ Interlock lookup saved to {filename}")
    except Exception as e:
        print(f"✗ Error saving interlock lookup to {filename}: {e}")

def main():
    """Main function to download interlock cards and create lookup."""
    print("Starting interlock card download from NEMO API...")
    print(f"API Endpoint: {NEMO_INTERLOCK_CARDS_API_URL}")
    print("-" * 60)
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Download interlock cards
    cards = download_interlock_cards(NEMO_INTERLOCK_CARDS_API_URL)
    
    if not cards:
        print("No interlock cards downloaded. Cannot proceed.")
        return
    
    # Save raw interlock cards data
    save_cards_to_json(cards)
    
    # Create and save interlock lookup
    interlock_lookup = create_interlock_lookup(cards)
    save_interlock_lookup(interlock_lookup)
    
    # Show sample of lookup entries
    print("\nSample interlock lookup entries:")
    sample_count = 0
    for key, card_id in interlock_lookup.items():
        if sample_count < 10:
            print(f"  {key} → ID {card_id}")
            sample_count += 1
        else:
            break
    
    if len(interlock_lookup) > 10:
        print(f"  ... and {len(interlock_lookup) - 10} more entries")
    
    print("\n" + "=" * 60)
    print("INTERLOCK CARD DOWNLOAD SUMMARY")
    print("=" * 60)
    print(f"Total interlock cards downloaded: {len(cards)}")
    print(f"Lookup entries created: {len(interlock_lookup)}")
    print(f"✓ Interlock lookup ready for use in interlock creation!")
    print("\nFiles created:")
    print(f"  - interlock_cards_download.json (raw interlock card data)")
    print(f"  - interlock_card_lookup.json (identifier → ID mapping)")

if __name__ == "__main__":
    main()

