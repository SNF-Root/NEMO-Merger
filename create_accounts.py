#!/usr/bin/env python3
"""
Script to create NEMO accounts from SNSF User Information Excel file.
Maps PI emails to accounts with name and type fields.
"""

import pandas as pd
import requests
import json
import os
from dotenv import load_dotenv
from typing import List, Dict, Any
import time

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoint for accounts
NEMO_ACCOUNTS_API_URL = "https://nemo-plan.stanford.edu/api/accounts/"

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

# Template for account data
ACCOUNT_TEMPLATE = {
    "id": None,  # Will be assigned by API
    "name": "",  # Will be filled from PI name
    "start_date": None,
    "active": True,
    "type": None  # Will be mapped from Excel type column using rate categories
}

def load_rate_category_mapping(filename: str = "rate_category_mapping.json") -> Dict[str, int]:
    """Load the rate category mapping from the downloaded rate categories."""
    try:
        with open(filename, 'r') as f:
            mapping = json.load(f)
        print(f"✓ Loaded rate category mapping with {len(mapping)} categories")
        return mapping
    except FileNotFoundError:
        print(f"✗ Rate category mapping file {filename} not found!")
        print("Please run download_rate_categories.py first to download rate categories from NEMO.")
        return {}
    except Exception as e:
        print(f"✗ Error loading rate category mapping: {e}")
        return {}

def read_pi_info_from_excel(file_path: str) -> List[Dict[str, str]]:
    """Read PI information from Excel file."""
    try:
        df = pd.read_excel(file_path)
        if 'pi email' in df.columns and 'type' in df.columns:
            # The 'pi email' column actually contains PI names
            # Filter out rows with missing PI names (NaN values)
            df_filtered = df.dropna(subset=['pi email'])
            
            # Get unique PI info with type
            pi_info = df_filtered[['pi email', 'type']].drop_duplicates()
            
            # Additional filtering to remove any remaining problematic values
            clean_pi_info = []
            for record in pi_info.to_dict('records'):
                pi_name = str(record['pi email']).strip()  # This is actually the PI name
                if pi_name and pi_name.lower() != 'nan' and pi_name != ',':
                    clean_pi_info.append(record)
            
            print(f"Filtered out {len(pi_info) - len(clean_pi_info)} rows with invalid PI names")
            return clean_pi_info
        else:
            print(f"Warning: Required columns not found in {file_path}")
            print(f"Available columns: {df.columns.tolist()}")
            return []
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []

def create_account_payload(pi_info: Dict[str, str], rate_mapping: Dict[str, int]) -> Dict[str, Any]:
    """Create an account payload with the given PI info."""
    payload = ACCOUNT_TEMPLATE.copy()
    
    # Clean and validate PI name
    pi_name = str(pi_info['pi email']).strip()  # This is actually the PI name
    if not pi_name or pi_name.lower() == 'nan' or pi_name == ',':
        raise ValueError(f"Invalid PI name: '{pi_name}'")
    
    payload["name"] = pi_name
    
    # Map the type from Excel to NEMO rate category ID
    excel_type = str(pi_info['type']).lower().strip()
    if excel_type in rate_mapping:
        payload["type"] = rate_mapping[excel_type]
        print(f"  → Mapped '{excel_type}' to rate category ID: {rate_mapping[excel_type]}")
    else:
        print(f"Warning: Unknown type '{excel_type}' for PI '{pi_name}', defaulting to Academic")
        # Find Academic in the mapping and use that as default
        for type_name, type_id in rate_mapping.items():
            if "academic" in type_name.lower():
                payload["type"] = type_id
                print(f"  → Defaulted to Academic rate category ID: {type_id}")
                break
        else:
            # If no Academic found, use the first available
            first_id = list(rate_mapping.values())[0] if rate_mapping else 1
            payload["type"] = first_id
            print(f"  → Defaulted to first available rate category ID: {first_id}")
    
    return payload

def push_account_to_api(pi_info: Dict[str, str], api_url: str, rate_mapping: Dict[str, int]) -> bool:
    """Push a single account to the NEMO API."""
    payload = create_account_payload(pi_info, rate_mapping)
    
    try:
        response = requests.post(api_url, json=payload, headers=API_HEADERS)
        
        if response.status_code == 200:  # Created
            print(f"✓ Successfully created account for: {pi_info['pi email']}")
            return True
        elif response.status_code == 400:
            print(f"✗ Bad request for account '{pi_info['pi email']}': {response.text}")
            return False
        elif response.status_code == 401:
            print(f"✗ Authentication failed: Check your NEMO_TOKEN")
            return False
        elif response.status_code == 403:
            print(f"✗ Permission denied: Check your API permissions")
            return False
        elif response.status_code == 409:
            print(f"⚠ Account for '{pi_info['pi email']}' already exists")
            return False
        else:
            print(f"✗ Failed to create account for '{pi_info['pi email']}': HTTP {response.status_code}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error creating account for '{pi_info['pi email']}': {e}")
        return False

def main():
    """Main function to read PI info and create accounts."""
    print("Starting account creation in NEMO...")
    print(f"API Endpoint: {NEMO_ACCOUNTS_API_URL}")
    print("-" * 50)
    
    # Load rate category mapping
    print("Loading rate category mapping...")
    rate_mapping = load_rate_category_mapping()
    
    if not rate_mapping:
        print("Cannot proceed without rate category mapping. Please run download_rate_categories.py first.")
        return
    
    # Read PI info from Excel
    pi_info_list = read_pi_info_from_excel("SNSF-Data/User Information.xlsx")
    
    if not pi_info_list:
        print("No PI information found!")
        return
        
    print(f"\nFound {len(pi_info_list)} unique PIs")
    print("Rate category mapping:")
    for excel_type, nem_id in rate_mapping.items():
        print(f"  {excel_type} → ID {nem_id}")
    print("-" * 50)
    
    # Create accounts
    successful = 0
    failed = 0
    
    for pi_info in pi_info_list:
        try:
            if push_account_to_api(pi_info, NEMO_ACCOUNTS_API_URL, rate_mapping):
                successful += 1
            else:
                failed += 1
        except ValueError as e:
            print(f"✗ Skipping invalid PI info: {e}")
            failed += 1
        except Exception as e:
            print(f"✗ Unexpected error processing PI info: {e}")
            failed += 1
        
        time.sleep(0.5)  # Small delay between requests
    
    # Summary
    print("\n" + "=" * 50)
    print("ACCOUNT CREATION SUMMARY")
    print("=" * 50)
    print(f"Total PIs processed: {len(pi_info_list)}")
    print(f"Successfully created: {successful}")
    print(f"Failed to create: {failed}")
    
if __name__ == "__main__":
    main()
