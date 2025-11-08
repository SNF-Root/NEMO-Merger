#!/usr/bin/env python3
"""
Script to remove NEMO accounts by ID from a CSV file.
Reads account IDs from removed_accounts.csv and deletes them from NEMO.
"""

import pandas as pd
import requests
import os
from dotenv import load_dotenv
from typing import List, Dict, Tuple
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

def read_account_ids_from_csv(file_path: str) -> List[Dict[str, str]]:
    """Read account IDs from CSV file."""
    try:
        df = pd.read_csv(file_path)
        
        if 'ID' not in df.columns:
            print(f"Error: 'ID' column not found in {file_path}")
            print(f"Available columns: {df.columns.tolist()}")
            return []
        
        # Filter out rows with empty/NaN IDs
        df_filtered = df[df['ID'].notna()]
        
        # Convert to list of dictionaries
        account_list = []
        for _, row in df_filtered.iterrows():
            account_id_value = row['ID']
            account_name = str(row.get('Removed Account', 'Unknown')).strip() if 'Removed Account' in row else 'Unknown'
            
            # Handle both numeric (int/float) and string IDs
            try:
                # Convert to int (handles both int and float like 425.0)
                account_id = int(float(account_id_value))
                account_list.append({
                    'id': account_id,
                    'name': account_name
                })
            except (ValueError, TypeError):
                # Skip invalid IDs
                continue
        
        print(f"✓ Read {len(account_list)} account IDs from {file_path}")
        print(f"  Filtered out {len(df) - len(account_list)} rows with invalid or empty IDs")
        
        return account_list
    except FileNotFoundError:
        print(f"✗ File not found: {file_path}")
        return []
    except Exception as e:
        print(f"✗ Error reading {file_path}: {e}")
        return []

def delete_account_by_id(account_id: int, account_name: str) -> Tuple[bool, str]:
    """
    Delete a single account from NEMO API by ID.
    
    Returns:
        Tuple of (success: bool, status: str) where status is 'success', 'not_found', or 'error'
    """
    delete_url = f"{NEMO_ACCOUNTS_API_URL}{account_id}/"
    
    try:
        response = requests.delete(delete_url, headers=API_HEADERS)
        
        if response.status_code == 204:  # No Content (successful deletion)
            print(f"✓ Successfully deleted account ID {account_id} ({account_name})")
            return (True, 'success')
        elif response.status_code == 404:
            print(f"⚠ Account ID {account_id} ({account_name}) not found - may already be deleted")
            return (False, 'not_found')
        elif response.status_code == 400:
            print(f"✗ Bad request for account ID {account_id} ({account_name}): {response.text}")
            return (False, 'error')
        elif response.status_code == 401:
            print(f"✗ Authentication failed: Check your NEMO_TOKEN")
            return (False, 'error')
        elif response.status_code == 403:
            print(f"✗ Permission denied for account ID {account_id} ({account_name}): Check your API permissions")
            return (False, 'error')
        else:
            print(f"✗ Failed to delete account ID {account_id} ({account_name}): HTTP {response.status_code}")
            if response.text:
                print(f"  Response: {response.text}")
            return (False, 'error')
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error deleting account ID {account_id} ({account_name}): {e}")
        return (False, 'error')

def main():
    """Main function to read account IDs and delete accounts."""
    print("Starting account removal from NEMO...")
    print(f"API Endpoint: {NEMO_ACCOUNTS_API_URL}")
    print("-" * 50)
    
    # Read account IDs from CSV
    csv_file = "removed_accounts.csv"
    account_list = read_account_ids_from_csv(csv_file)
    
    if not account_list:
        print("No account IDs found to remove!")
        return
    
    print(f"\nFound {len(account_list)} accounts to remove")
    print("-" * 50)
    
    # Confirm before proceeding
    print("\n⚠ WARNING: This will permanently delete accounts from NEMO!")
    print(f"About to delete {len(account_list)} accounts.")
    response = input("Do you want to continue? (yes/no): ")
    
    if response.lower() not in ['yes', 'y']:
        print("Cancelled. No accounts were deleted.")
        return
    
    # Delete accounts
    successful = 0
    failed = 0
    not_found = 0
    
    for account in account_list:
        account_id = account['id']
        account_name = account['name']
        
        success, status = delete_account_by_id(account_id, account_name)
        
        if success:
            successful += 1
        elif status == 'not_found':
            not_found += 1
        else:
            failed += 1
        
        time.sleep(0.5)  # Small delay between requests
    
    # Summary
    print("\n" + "=" * 50)
    print("ACCOUNT REMOVAL SUMMARY")
    print("=" * 50)
    print(f"Total accounts to remove: {len(account_list)}")
    print(f"Successfully deleted: {successful}")
    print(f"Not found (may already be deleted): {not_found}")
    print(f"Failed to delete: {failed}")
    if len(account_list) > 0:
        print(f"Success rate: {(successful/len(account_list)*100):.1f}%")

if __name__ == "__main__":
    main()

