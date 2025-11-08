#!/usr/bin/env python3
"""
Script to download all accounts from NEMO API and save them locally.
This is needed before creating projects since projects must be associated with accounts.
"""

import requests
import json
import os
import csv
from dotenv import load_dotenv
from typing import List, Dict, Any

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoint for accounts
NEMO_ACCOUNTS_API_URL = "https://nemo.stanford.edu/api/accounts/"

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
        response = requests.get(NEMO_ACCOUNTS_API_URL, headers=API_HEADERS)
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

def download_accounts() -> List[Dict[str, Any]]:
    """Download all accounts from the NEMO API."""
    try:
        print("Downloading accounts from NEMO API...")
        response = requests.get(NEMO_ACCOUNTS_API_URL, headers=API_HEADERS)
        
        if response.status_code == 200:
            accounts = response.json()
            print(f"✓ Successfully downloaded {len(accounts)} accounts")
            return accounts
        else:
            print(f"✗ Failed to download accounts: HTTP {response.status_code} - {response.text}")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error downloading accounts: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"✗ Error parsing JSON response: {e}")
        return []

def save_accounts_to_file(accounts: List[Dict[str, Any]], filename: str = "nemo_accounts.json"):
    """Save accounts to a local JSON file."""
    try:
        with open(filename, 'w') as f:
            json.dump(accounts, f, indent=2)
        print(f"✓ Successfully saved {len(accounts)} accounts to {filename}")
    except Exception as e:
        print(f"✗ Error saving accounts to file: {e}")

def save_accounts_to_csv(accounts: List[Dict[str, Any]], filename: str = "nemo_accounts.csv"):
    """Save accounts to a CSV file, sorted by ID in ascending order."""
    if not accounts:
        print("No accounts to save to CSV")
        return
    
    try:
        # Sort accounts by ID in ascending order
        sorted_accounts = sorted(accounts, key=lambda x: x.get('id', 0))
        
        # Get all unique keys from all accounts to create comprehensive headers
        all_keys = set()
        for account in sorted_accounts:
            all_keys.update(account.keys())
        
        # Define column order (ID first, then others alphabetically)
        fieldnames = ['id'] + sorted([k for k in all_keys if k != 'id'])
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for account in sorted_accounts:
                # Convert None values to empty strings for CSV
                row = {k: ('' if v is None else v) for k, v in account.items()}
                writer.writerow(row)
        
        print(f"✓ Successfully saved {len(sorted_accounts)} accounts to {filename} (sorted by ID)")
    except Exception as e:
        print(f"✗ Error saving accounts to CSV: {e}")

def create_account_lookup(accounts: List[Dict[str, Any]]) -> Dict[str, int]:
    """Create a lookup dictionary mapping account names to IDs."""
    lookup = {}
    for account in accounts:
        if 'name' in account and 'id' in account:
            lookup[account['name']] = account['id']
    
    print(f"✓ Created lookup for {len(lookup)} accounts")
    return lookup

def main():
    """Main function to download and save accounts."""
    print("Starting account download from NEMO API...")
    print(f"API Endpoint: {NEMO_ACCOUNTS_API_URL}")
    print("-" * 60)
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Download accounts
    accounts = download_accounts()
    
    if not accounts:
        print("No accounts downloaded. Cannot proceed.")
        return
    
    # Save accounts to file
    save_accounts_to_file(accounts)
    
    # Save accounts to CSV (sorted by ID)
    save_accounts_to_csv(accounts)
    
    # Create and save account lookup
    account_lookup = create_account_lookup(accounts)
    
    # Save lookup to a separate file for easy access
    with open("account_lookup.json", 'w') as f:
        json.dump(account_lookup, f, indent=2)
    print("✓ Saved account lookup to account_lookup.json")
    
    # Show sample of accounts
    print("\nSample accounts:")
    for i, account in enumerate(accounts[:5]):
        print(f"  {i+1}. ID: {account.get('id', 'N/A')}, Name: {account.get('name', 'N/A')}, Type: {account.get('type', 'N/A')}")
    
    if len(accounts) > 5:
        print(f"  ... and {len(accounts) - 5} more accounts")
    
    print(f"\n✓ Account download complete! {len(accounts)} accounts saved locally.")
    print("You can now run create_projects.py to create projects with these accounts.")

if __name__ == "__main__":
    main()
