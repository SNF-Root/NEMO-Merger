#!/usr/bin/env python3
"""
Script to create NEMO accounts from SNSF Excel files.
Maps account names to accounts with name and type fields.
Supports both old format ('pi email', 'type') and new format ('Account', 'project_type').
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

# Template for account data
ACCOUNT_TEMPLATE = {
    "id": None,  # Will be assigned by API
    "name": "",  # Will be filled from account name
    "start_date": None,
    "active": True,
    "type": None  # Will be mapped from Excel type column using account types
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

def load_account_types(filename: str = "nemo_account_types.json") -> List[Dict[str, Any]]:
    """Load account types from the downloaded JSON file."""
    try:
        with open(filename, 'r') as f:
            account_types = json.load(f)
        print(f"✓ Loaded {len(account_types)} account types from {filename}")
        return account_types
    except FileNotFoundError:
        print(f"✗ Account types file {filename} not found!")
        print("Please run download_account_types.py first to download account types from NEMO.")
        return []
    except Exception as e:
        print(f"✗ Error loading account types: {e}")
        return []

def create_account_type_name_to_id_lookup(account_types: List[Dict[str, Any]]) -> Dict[str, int]:
    """Create a lookup dictionary mapping account type names to their IDs."""
    lookup = {}
    for account_type in account_types:
        if 'name' in account_type and 'id' in account_type:
            name = account_type['name']
            lookup[name] = account_type['id']
    return lookup

def create_excel_to_nemo_name_mapping() -> Dict[str, str]:
    """
    Create a mapping from Excel type language to NEMO account type names.
    This is where you configure how Excel types map to NEMO account type names.
    
    Returns:
        Dictionary mapping Excel type (lowercase) to NEMO account type name
    """
    # TODO: Update this mapping based on your Excel file's type values
    # Excel type values (from your Excel file) -> NEMO account type names (from nemo_account_types.json)
    excel_to_nemo_name = {
        "local": "Local",
        "industrial": "Industrial",
        "no charge": "No Charge",
        "other academic": "Other Academic",
        "industrial-sbir": "Industrial-SBIR",
        "foreign": "Other Academic",  # Default foreign to Other Academic
    }
    return excel_to_nemo_name

def build_account_type_mapping(account_types: List[Dict[str, Any]], excel_to_nemo_name: Dict[str, str]) -> Dict[str, int]:
    """
    Build the final mapping from Excel type values to account type IDs.
    
    Args:
        account_types: List of account type dictionaries from JSON
        excel_to_nemo_name: Mapping from Excel types to NEMO account type names
    
    Returns:
        Dictionary mapping Excel type (lowercase) to account type ID
    """
    # Create name-to-ID lookup
    name_to_id = create_account_type_name_to_id_lookup(account_types)
    
    # Build Excel-to-ID mapping
    excel_to_id = {}
    unmapped_excel_types = []
    
    for excel_type, nemo_name in excel_to_nemo_name.items():
        if nemo_name in name_to_id:
            excel_to_id[excel_type] = name_to_id[nemo_name]
            print(f"  ✓ Mapped '{excel_type}' → '{nemo_name}' (ID: {name_to_id[nemo_name]})")
        else:
            unmapped_excel_types.append((excel_type, nemo_name))
            print(f"  ⚠ Warning: Account type name '{nemo_name}' not found in account types for Excel type '{excel_type}'")
    
    # Handle unmapped types
    if unmapped_excel_types:
        print("\n  Attempting to find defaults for unmapped types...")
        # Try to find "Other Academic" or "Local" as defaults
        default_names = ["Other Academic", "Local"]
        default_id = None
        for default_name in default_names:
            if default_name in name_to_id:
                default_id = name_to_id[default_name]
                print(f"  → Using '{default_name}' (ID: {default_id}) as default")
                break
        
        if default_id:
            for excel_type, nemo_name in unmapped_excel_types:
                excel_to_id[excel_type] = default_id
                print(f"  → Defaulted '{excel_type}' to ID {default_id}")
        else:
            # Last resort: use first available ID
            if name_to_id:
                first_id = list(name_to_id.values())[0]
                for excel_type, nemo_name in unmapped_excel_types:
                    excel_to_id[excel_type] = first_id
                    print(f"  → Defaulted '{excel_type}' to first available ID {first_id}")
    
    return excel_to_id

def load_account_type_mapping(account_types_file: str = "nemo_account_types.json") -> Dict[str, int]:
    """
    Load account types and create the mapping from Excel types to account type IDs.
    
    This function:
    1. Loads account types from JSON
    2. Creates Excel-to-NEMO name mapping (configurable)
    3. Builds the final Excel-to-ID mapping
    
    Returns:
        Dictionary mapping Excel type (lowercase) to account type ID
    """
    # Load account types from JSON
    account_types = load_account_types(account_types_file)
    if not account_types:
        return {}
    
    # Show available account types
    print("\nAvailable account types in NEMO:")
    for account_type in account_types:
        if 'name' in account_type and 'id' in account_type:
            print(f"  ID {account_type['id']}: {account_type['name']}")
    
    # Get Excel-to-NEMO name mapping (this is where you configure the mapping)
    excel_to_nemo_name = create_excel_to_nemo_name_mapping()
    
    # Build the final mapping
    print("\nBuilding Excel type to account type ID mapping...")
    mapping = build_account_type_mapping(account_types, excel_to_nemo_name)
    
    print(f"\n✓ Created mapping for {len(mapping)} Excel types")
    return mapping

def load_account_lookup(filename: str = "account_lookup.json") -> Dict[str, int]:
    """Load the account lookup from the downloaded accounts."""
    try:
        with open(filename, 'r') as f:
            lookup = json.load(f)
        print(f"✓ Loaded account lookup with {len(lookup)} accounts")
        return lookup
    except FileNotFoundError:
        print(f"✗ Account lookup file {filename} not found!")
        print("Please run download_accounts.py first to download accounts from NEMO.")
        return {}
    except Exception as e:
        print(f"✗ Error loading account lookup: {e}")
        return {}

def download_existing_accounts() -> Dict[str, int]:
    """Download all existing accounts from NEMO API and return a lookup dictionary."""
    try:
        print("Downloading existing accounts from NEMO API...")
        response = requests.get(NEMO_ACCOUNTS_API_URL, headers=API_HEADERS)
        
        if response.status_code == 200:
            accounts = response.json()
            # Create lookup dictionary mapping account names to IDs
            account_lookup = {}
            for account in accounts:
                if 'name' in account and 'id' in account:
                    account_name = str(account['name']).strip()
                    if account_name:
                        account_lookup[account_name] = account['id']
            
            print(f"✓ Successfully downloaded {len(accounts)} accounts")
            print(f"✓ Found {len(account_lookup)} unique account names")
            return account_lookup
        else:
            print(f"✗ Failed to download accounts: HTTP {response.status_code} - {response.text}")
            return {}
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error downloading accounts: {e}")
        return {}
    except json.JSONDecodeError as e:
        print(f"✗ Error parsing JSON response: {e}")
        return {}
    except Exception as e:
        print(f"✗ Error processing accounts: {e}")
        return {}

def filter_existing_accounts(account_info_list: List[Dict[str, str]], account_lookup: Dict[str, int]) -> List[Dict[str, str]]:
    """Filter out accounts that already exist in NEMO based on account name comparison."""
    new_accounts = []
    duplicate_accounts = []
    
    for account_info in account_info_list:
        account_name = str(account_info['account']).strip()
        
        if account_name in account_lookup:
            duplicate_accounts.append(account_info)
        else:
            new_accounts.append(account_info)
    
    if duplicate_accounts:
        print(f"⚠ Filtered out {len(duplicate_accounts)} duplicate accounts (already exist in NEMO):")
        for dup in duplicate_accounts[:10]:  # Show first 10
            print(f"  - {dup['account']}")
        if len(duplicate_accounts) > 10:
            print(f"  ... and {len(duplicate_accounts) - 10} more duplicates")
    
    return new_accounts

def read_pi_info_from_excel(file_path: str) -> List[Dict[str, str]]:
    """Read account information from Excel file. Supports both old and new formats."""
    try:
        df = pd.read_excel(file_path)
        
        # Determine which columns to use (support both old and new format)
        account_col = None
        type_col = None
        
        # Check for new format first: 'Account' and 'project_type'
        if 'Account' in df.columns and 'project_type' in df.columns:
            account_col = 'Account'
            type_col = 'project_type'
            print("Using new format: 'Account' and 'project_type' columns")
        # Check for old format: 'pi email' and 'type'
        elif 'pi email' in df.columns and 'type' in df.columns:
            account_col = 'pi email'
            type_col = 'type'
            print("Using old format: 'pi email' and 'type' columns")
        else:
            print(f"Warning: Required columns not found in {file_path}")
            print(f"Available columns: {df.columns.tolist()}")
            print("Expected either: ('Account', 'project_type') or ('pi email', 'type')")
            return []
        
        # Filter out rows with missing account names (NaN values)
        df_filtered = df.dropna(subset=[account_col])
        
        # Get unique account info with type
        account_info = df_filtered[[account_col, type_col]].drop_duplicates()
        
        # Additional filtering to remove any remaining problematic values
        clean_account_info = []
        for record in account_info.to_dict('records'):
            account_name = str(record[account_col]).strip()
            if account_name and account_name.lower() != 'nan' and account_name != ',':
                # Normalize the record to use consistent keys
                clean_record = {
                    'account': account_name,
                    'type': str(record[type_col]).strip()
                }
                clean_account_info.append(clean_record)
        
        print(f"Filtered out {len(account_info) - len(clean_account_info)} rows with invalid account names")
        return clean_account_info
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []

def create_account_payload(account_info: Dict[str, str], account_type_mapping: Dict[str, int]) -> Dict[str, Any]:
    """Create an account payload with the given account info."""
    payload = ACCOUNT_TEMPLATE.copy()
    
    # Clean and validate account name
    account_name = str(account_info['account']).strip()
    if not account_name or account_name.lower() == 'nan' or account_name == ',':
        raise ValueError(f"Invalid account name: '{account_name}'")
    
    payload["name"] = account_name
    
    # Map the type from Excel to NEMO account type ID
    excel_type = str(account_info['type']).lower().strip()
    if excel_type in account_type_mapping:
        payload["type"] = account_type_mapping[excel_type]
        print(f"  → Mapped '{excel_type}' to account type ID: {account_type_mapping[excel_type]}")
    else:
        print(f"Warning: Unknown type '{excel_type}' for account '{account_name}', defaulting to Academic")
        # Find Academic in the mapping and use that as default
        for type_name, type_id in account_type_mapping.items():
            if "academic" in type_name.lower():
                payload["type"] = type_id
                print(f"  → Defaulted to Academic account type ID: {type_id}")
                break
        else:
            # If no Academic found, use the first available
            first_id = list(account_type_mapping.values())[0] if account_type_mapping else 1
            payload["type"] = first_id
            print(f"  → Defaulted to first available account type ID: {first_id}")
    
    return payload

def push_account_to_api(account_info: Dict[str, str], api_url: str, account_type_mapping: Dict[str, int]) -> bool:
    """Push a single account to the NEMO API."""
    payload = create_account_payload(account_info, account_type_mapping)
    
    try:
        response = requests.post(api_url, json=payload, headers=API_HEADERS)
        
        if response.status_code == 200:  # Created
            print(f"✓ Successfully created account for: {account_info['account']}")
            return True
        elif response.status_code == 400:
            print(f"✗ Bad request for account '{account_info['account']}': {response.text}")
            return False
        elif response.status_code == 401:
            print(f"✗ Authentication failed: Check your NEMO_TOKEN")
            return False
        elif response.status_code == 403:
            print(f"✗ Permission denied: Check your API permissions")
            return False
        elif response.status_code == 409:
            print(f"⚠ Account for '{account_info['account']}' already exists")
            return False
        else:
            print(f"✗ Failed to create account for '{account_info['account']}': HTTP {response.status_code}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error creating account for '{account_info['account']}': {e}")
        return False

def main():
    """Main function to read account info and create accounts."""
    print("Starting account creation in NEMO...")
    print(f"API Endpoint: {NEMO_ACCOUNTS_API_URL}")
    print("-" * 50)
    
    # Load account type mapping
    print("Loading account type mapping...")
    account_type_mapping = load_account_type_mapping()
    
    if not account_type_mapping:
        print("Cannot proceed without account type mapping. Please run download_account_types.py first.")
        return
    
    # Load or download existing accounts
    print("\nLoading existing accounts...")
    account_lookup = load_account_lookup()
    
    # If lookup file doesn't exist, download from API
    if not account_lookup:
        print("Account lookup file not found. Downloading existing accounts from API...")
        account_lookup = download_existing_accounts()
        if not account_lookup:
            print("⚠ Warning: Could not download existing accounts. Proceeding without duplicate check.")
    
    # Read account info from Excel
    excel_file = "SNSF-Data/Copy of SNSF PTAs for Alex Denton.xlsx"
    if not os.path.exists(excel_file):
        print(f"✗ Error: File not found: {excel_file}")
        return
    print(f"Using file: {excel_file}")
    
    account_info_list = read_pi_info_from_excel(excel_file)
    
    if not account_info_list:
        print("No account information found!")
        return
        
    print(f"\nFound {len(account_info_list)} unique accounts from Excel")
    
    # Filter out existing accounts
    print("\nFiltering out duplicate accounts...")
    filtered_account_info = filter_existing_accounts(account_info_list, account_lookup)
    
    if not filtered_account_info:
        print("No new accounts to create! All accounts already exist in NEMO.")
        return
    
    print(f"\n✓ {len(filtered_account_info)} new accounts to create (filtered out {len(account_info_list) - len(filtered_account_info)} duplicates)")
    
    print("\nAccount type mapping:")
    for excel_type, nem_id in account_type_mapping.items():
        print(f"  {excel_type} → ID {nem_id}")
    print("-" * 50)
    
    # Create accounts
    successful = 0
    failed = 0
    
    for account_info in filtered_account_info:
        try:
            if push_account_to_api(account_info, NEMO_ACCOUNTS_API_URL, account_type_mapping):
                successful += 1
            else:
                failed += 1
        except ValueError as e:
            print(f"✗ Skipping invalid account info: {e}")
            failed += 1
        except Exception as e:
            print(f"✗ Unexpected error processing account info: {e}")
            failed += 1
        
        time.sleep(0.5)  # Small delay between requests
    
    # Summary
    print("\n" + "=" * 50)
    print("ACCOUNT CREATION SUMMARY")
    print("=" * 50)
    print(f"Total accounts in Excel: {len(account_info_list)}")
    print(f"Duplicate accounts (already exist): {len(account_info_list) - len(filtered_account_info)}")
    print(f"New accounts to create: {len(filtered_account_info)}")
    print(f"Successfully created: {successful}")
    print(f"Failed to create: {failed}")
    if len(filtered_account_info) > 0:
        print(f"Success rate: {(successful/len(filtered_account_info)*100):.1f}%")
    
if __name__ == "__main__":
    main()
