#!/usr/bin/env python3
"""
Script to create billing rates in NEMO by mapping SNSF rate names to NEMO rate types.
Maps equipment rates from SNSF to the appropriate NEMO rate type IDs.
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

# NEMO API endpoint for billing rates
NEMO_RATES_API_URL = "https://nemo-plan.stanford.edu/api/billing/rates/"

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

# Mappings will be loaded from JSON files at runtime

# Template for billing rate data
RATE_TEMPLATE = {
    "id": None,  # Will be assigned by API
    "type": None,  # Will be filled from mapping
    "category": None,  # Will be filled manually
    "item": None,  # Will be filled manually
    "rate": None,  # Will be filled from Excel
    "flat_rate": False,
    "start_date": None,
    "end_date": None,
    "notes": "",
    "active": True
}

def test_api_connection():
    """Test the API connection and authentication."""
    try:
        response = requests.get(NEMO_RATES_API_URL, headers=API_HEADERS)
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

def load_rate_type_lookup(filename: str = "billing_rate_type_lookup.json") -> Dict[str, int]:
    """Load the rate type lookup from the downloaded rate types."""
    try:
        with open(filename, 'r') as f:
            lookup = json.load(f)
        print(f"✓ Loaded rate type lookup with {len(lookup)} types")
        return lookup
    except FileNotFoundError:
        print(f"✗ Rate type lookup file {filename} not found!")
        print("Please run download_rate_types.py first to download rate types from NEMO.")
        return {}
    except Exception as e:
        print(f"✗ Error loading rate type lookup: {e}")
        return {}

def load_rate_class_mapping(filename: str = "rate_category_mapping.json") -> Dict[str, int]:
    """Load the rate class mapping from the downloaded rate categories."""
    try:
        with open(filename, 'r') as f:
            mapping = json.load(f)
        print(f"✓ Loaded rate class mapping with {len(mapping)} categories")
        return mapping
    except FileNotFoundError:
        print(f"✗ Rate class mapping file {filename} not found!")
        print("Please run download_rate_categories.py first to download rate categories from NEMO.")
        return {}
    except Exception as e:
        print(f"✗ Error loading rate class mapping: {e}")
        return {}

def create_snsf_to_nemo_rate_mapping(rate_type_lookup: Dict[str, int]) -> Dict[str, str]:
    """Create a mapping from SNSF rate names to NEMO rate type names."""
    # This mapping connects SNSF rate names to NEMO rate type names
    snsf_to_nemo = {
        "equipment_hourly_rate": "TOOL_USAGE",
        "equipment_staff_rate": "STAFF_CHARGE", 
        "equipment_training_rate": "TOOL_TRAINING_INDIVIDUAL"
    }
    
    # Verify all mapped NEMO types exist in the lookup
    verified_mapping = {}
    for snsf_name, nemo_type in snsf_to_nemo.items():
        if nemo_type in rate_type_lookup:
            verified_mapping[snsf_name] = nemo_type
            print(f"✓ Verified mapping: {snsf_name} → {nemo_type}")
        else:
            print(f"⚠ Warning: NEMO rate type '{nemo_type}' not found in lookup")
    
    return verified_mapping

def read_snsf_rates_from_excel(file_path: str, rate_type_lookup: Dict[str, int], rate_class_mapping: Dict[str, int], snsf_to_nemo_mapping: Dict[str, str]) -> List[Dict[str, Any]]:
    """Read SNSF rates from Excel file."""
    try:
        df = pd.read_excel(file_path)
        print(f"Successfully read {file_path}")
        print(f"Shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        
        # Look for required columns
        rate_name_col = None
        rate_class_col = None
        rate_value_col = None
        
        for col in df.columns:
            if 'rate name' in col.lower():
                rate_name_col = col
            elif 'rate class' in col.lower():
                rate_class_col = col
            elif col.lower() == 'rate':
                rate_value_col = col
        
        if not rate_name_col:
            print("Warning: No 'rate name' column found")
            print("Available columns:", df.columns.tolist())
            return []
        
        if not rate_class_col:
            print("Warning: No 'rate class' column found")
            print("Available columns:", df.columns.tolist())
            return []
        
        if not rate_value_col:
            print("Warning: No 'rate' column found")
            print("Available columns:", df.columns.tolist())
            return []
        
        print(f"Using columns: {rate_name_col}, {rate_class_col}, {rate_value_col}")
        
        # Extract rates with their classes and values
        rates = []
        seen_combinations = set()
        
        for _, row in df.iterrows():
            rate_name = str(row[rate_name_col]).strip()
            rate_class = str(row[rate_class_col]).strip()
            rate_value = row[rate_value_col]
            
            # Skip if any required field is missing
            if (pd.isna(rate_name) or pd.isna(rate_class) or pd.isna(rate_value) or
                rate_name.lower() == 'nan' or rate_class.lower() == 'nan'):
                continue
            
            # Create unique combination key
            combination_key = f"{rate_name}_{rate_class}"
            if combination_key in seen_combinations:
                continue
            seen_combinations.add(combination_key)
            
            # Check if we have mappings for both rate type and rate class
            if rate_name in snsf_to_nemo_mapping and rate_class.lower() in rate_class_mapping:
                nemo_type_name = snsf_to_nemo_mapping[rate_name]
                rate_type_id = rate_type_lookup[nemo_type_name]
                rate_class_id = rate_class_mapping[rate_class.lower()]
                
                rates.append({
                    'rate_name': rate_name,
                    'rate_class': rate_class,
                    'rate_value': float(rate_value),
                    'rate_type_id': rate_type_id,
                    'rate_class_id': rate_class_id,
                    'nemo_type_name': nemo_type_name,
                    'nemo_category_name': get_nemo_category_name(rate_class_id, rate_class_mapping)
                })
                print(f"✓ Mapped '{rate_name}' + '{rate_class}' → Type: {nemo_type_name}, Category: {get_nemo_category_name(rate_class_id, rate_class_mapping)}")
            else:
                if rate_name not in snsf_to_nemo_mapping:
                    print(f"⚠ No mapping found for rate type: {rate_name}")
                if rate_class.lower() not in rate_class_mapping:
                    print(f"⚠ No mapping found for rate class: {rate_class}")
        
        return rates
        
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []

def get_nemo_type_name(type_id: int, rate_type_lookup: Dict[str, int]) -> str:
    """Get the NEMO rate type name from the ID using the lookup."""
    for name, rt_id in rate_type_lookup.items():
        if rt_id == type_id:
            return name
    return f"Unknown Type {type_id}"

def get_nemo_category_name(category_id: int, rate_class_mapping: Dict[str, int]) -> str:
    """Get the NEMO category name from the ID using the mapping."""
    for name, cat_id in rate_class_mapping.items():
        if cat_id == category_id:
            return name
    return f"Unknown Category {category_id}"

def create_rate_payload(rate_data: Dict[str, Any]) -> Dict[str, Any]:
    """Create a rate payload with the given data."""
    payload = RATE_TEMPLATE.copy()
    payload["type"] = rate_data['rate_type_id']
    payload["category"] = rate_data['rate_class_id']
    payload["rate"] = rate_data['rate_value']
    
    # Set notes with detailed information
    payload["notes"] = f"Migrated from SNSF: {rate_data['rate_name']} + {rate_data['rate_class']} = ${rate_data['rate_value']}"
    
    return payload

def push_rate_to_api(rate_data: Dict[str, Any], api_url: str) -> bool:
    """Push a single rate to the NEMO API."""
    payload = create_rate_payload(rate_data)
    
    try:
        response = requests.post(api_url, json=payload, headers=API_HEADERS)
        
        if response.status_code == 201:  # Created
            print(f"✓ Successfully created rate: {rate_data['rate_name']} → {rate_data['nemo_type_name']}")
            return True
        elif response.status_code == 400:
            print(f"✗ Bad request for rate '{rate_data['rate_name']}': {response.text}")
            return False
        elif response.status_code == 401:
            print(f"✗ Authentication failed for rate '{rate_data['rate_name']}': Check your NEMO_TOKEN")
            return False
        elif response.status_code == 403:
            print(f"✗ Permission denied for rate '{rate_data['rate_name']}': Check your API permissions")
            return False
        elif response.status_code == 409:
            print(f"⚠ Rate for '{rate_data['rate_name']}' already exists (conflict)")
            return False
        else:
            print(f"✗ Failed to create rate '{rate_data['rate_name']}': HTTP {response.status_code} - {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error creating rate '{rate_data['rate_name']}': {e}")
        return False

def main():
    """Main function to read SNSF rates and create NEMO billing rates."""
    print("Starting billing rate creation from SNSF data...")
    print(f"API Endpoint: {NEMO_RATES_API_URL}")
    print("-" * 60)
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Load the mappings from JSON files
    print("Loading rate type lookup...")
    rate_type_lookup = load_rate_type_lookup()
    
    if not rate_type_lookup:
        print("Cannot proceed without rate type lookup. Please run download_rate_types.py first.")
        return
    
    print("Loading rate class mapping...")
    rate_class_mapping = load_rate_class_mapping()
    
    if not rate_class_mapping:
        print("Cannot proceed without rate class mapping. Please run download_rate_categories.py first.")
        return
    
    # Create SNSF to NEMO rate type mapping
    print("Creating SNSF to NEMO rate type mapping...")
    snsf_to_nemo_mapping = create_snsf_to_nemo_rate_mapping(rate_type_lookup)
    
    if not snsf_to_nemo_mapping:
        print("Cannot proceed without SNSF to NEMO rate type mapping.")
        return
    
    # Show the loaded mappings
    print("\nLoaded Rate Type Mapping:")
    for name, rt_id in rate_type_lookup.items():
        print(f"  {name} → ID {rt_id}")
    
    print("\nLoaded Rate Class Mapping:")
    for name, cat_id in rate_class_mapping.items():
        print(f"  {name} → ID {cat_id}")
    
    print("\nSNSF to NEMO Rate Type Mapping:")
    for snsf_name, nemo_type in snsf_to_nemo_mapping.items():
        print(f"  {snsf_name} → {nemo_type}")
    print("-" * 60)
    
    # Find and read SNSF rates from Excel files
    snsf_data_dir = "SNSF-Data"
    if not os.path.exists(snsf_data_dir):
        print(f"Directory not found: {snsf_data_dir}")
        print("Please check the directory path.")
        return
    
    # Look for rates report files
    rates_files = []
    for file in os.listdir(snsf_data_dir):
        if file.endswith('rates_report.xlsx'):
            rates_files.append(os.path.join(snsf_data_dir, file))
    
    if not rates_files:
        print(f"No *rates_report.xlsx files found in {snsf_data_dir}")
        print("Please ensure you have rates report files in the SNSF-Data folder.")
        return
    
    print(f"Found {len(rates_files)} rates report file(s):")
    for file in rates_files:
        print(f"  - {file}")
    
    # Process all rates files
    all_rates = []
    for excel_file in rates_files:
        print(f"\nProcessing file: {excel_file}")
        rates = read_snsf_rates_from_excel(excel_file, rate_type_lookup, rate_class_mapping, snsf_to_nemo_mapping)
        all_rates.extend(rates)
        print(f"  Extracted {len(rates)} rates from this file")
    
    rates = all_rates
    
    if not rates:
        print("No rates found to create!")
        return
    
    print(f"\nReady to create {len(rates)} billing rates...")
    
    # Create rates via API
    successful_creations = 0
    failed_creations = 0
    
    for i, rate_data in enumerate(rates, 1):
        print(f"\n[{i}/{len(rates)}] Creating rate: {rate_data['rate_name']} + {rate_data['rate_class']}")
        print(f"  → NEMO Type: {rate_data['nemo_type_name']} (ID: {rate_data['rate_type_id']})")
        print(f"  → NEMO Category: {rate_data['nemo_category_name']} (ID: {rate_data['rate_class_id']})")
        print(f"  → Rate: ${rate_data['rate_value']}")
        
        if push_rate_to_api(rate_data, NEMO_RATES_API_URL):
            successful_creations += 1
        else:
            failed_creations += 1
        
        # Add a small delay to avoid overwhelming the API
        time.sleep(0.5)
    
    # Summary
    print("\n" + "=" * 60)
    print("RATE CREATION SUMMARY")
    print("=" * 60)
    print(f"Total rates processed: {len(rates)}")
    print(f"Successfully created: {successful_creations}")
    print(f"Failed to create: {failed_creations}")
    print(f"Success rate: {(successful_creations/len(rates)*100):.1f}%")
    
    if successful_creations > 0:
        print(f"\n✓ {successful_creations} rates were created with actual values from SNSF!")
        print("The rates include proper type, category, and pricing information.")
        print("You may still need to assign specific tools/items to these rates.")

if __name__ == "__main__":
    main()
