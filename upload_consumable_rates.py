#!/usr/bin/env python3
"""
Script to upload consumable rates from CSV file to NEMO API endpoint.
Downloads consumables to create a name-to-ID lookup, then uploads rates from CSV.
"""

import requests
import csv
import json
import os
from typing import List, Dict, Any, Tuple, Optional
import time
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoints
NEMO_CONSUMABLES_API_URL = "https://nemo.stanford.edu/api/consumables/"
NEMO_RATES_API_URL = "https://nemo.stanford.edu/api/billing/rates/"

# Get NEMO token from environment
NEMO_TOKEN = os.getenv('NEMO_TOKEN')
if not NEMO_TOKEN:
    print("Error: NEMO_TOKEN not found in environment variables or .env file")
    print("Please create a .env file with: NEMO_TOKEN=your_token_here")
    print("Or set the environment variable: export NEMO_TOKEN=your_token_here")
    exit(1)
else:
    print("NEMO_TOKEN found in environment")

# API headers with authentication
API_HEADERS = {
    'Authorization': f'Token {NEMO_TOKEN}',
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

def convert_boolean(value: str) -> bool:
    """Convert CSV boolean string (TRUE/FALSE) to Python boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.upper() == 'TRUE'
    return False

def test_api_connection(api_url: str, logger: logging.Logger) -> bool:
    """Test the API connection and authentication."""
    try:
        response = requests.get(api_url, headers=API_HEADERS)
        if response.status_code == 200:
            print(f"✓ API connection successful: {api_url}")
            logger.info(f"API connection test: SUCCESS - {api_url}")
            return True
        elif response.status_code == 401:
            print(f"✗ Authentication failed: Check your NEMO_TOKEN")
            logger.error(f"API connection test: AUTHENTICATION FAILED - {api_url}")
            return False
        elif response.status_code == 403:
            print(f"✗ Permission denied: Check your API permissions")
            logger.error(f"API connection test: PERMISSION DENIED - {api_url}")
            return False
        else:
            print(f"✗ API connection failed: HTTP {response.status_code}")
            logger.error(f"API connection test: FAILED - HTTP {response.status_code} - {api_url}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error connecting to API: {e}")
        logger.error(f"API connection test: NETWORK ERROR - {e}")
        return False

def download_all_consumables(logger: logging.Logger) -> List[Dict[str, Any]]:
    """Download all consumables from the NEMO API."""
    print("Downloading consumables from NEMO API...")
    logger.info("Downloading consumables from NEMO API")
    
    all_consumables = []
    page = 1
    
    while True:
        try:
            # Add pagination parameters
            params = {'page': page}
            response = requests.get(NEMO_CONSUMABLES_API_URL, headers=API_HEADERS, params=params)
            
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
                logger.error("Authentication failed while downloading consumables")
                return []
            elif response.status_code == 403:
                print("✗ Permission denied: Check your API permissions")
                logger.error("Permission denied while downloading consumables")
                return []
            else:
                print(f"✗ Failed to download consumables: HTTP {response.status_code} - {response.text}")
                logger.error(f"Failed to download consumables: HTTP {response.status_code}")
                return []
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Network error downloading consumables: {e}")
            logger.error(f"Network error downloading consumables: {e}", exc_info=True)
            return []
        except json.JSONDecodeError as e:
            print(f"✗ Error parsing JSON response: {e}")
            logger.error(f"Error parsing JSON response: {e}")
            return []
    
    print(f"✓ Total consumables downloaded: {len(all_consumables)}")
    logger.info(f"Total consumables downloaded: {len(all_consumables)}")
    return all_consumables

def create_consumable_lookup(consumables: List[Dict[str, Any]]) -> Dict[str, int]:
    """Create a lookup dictionary mapping consumable names to IDs."""
    lookup = {}
    duplicates = []
    
    for consumable in consumables:
        name = consumable.get('name', '').strip()
        consumable_id = consumable.get('id')
        
        if name and consumable_id:
            if name in lookup:
                # Handle duplicates - keep the first one, but track duplicates
                if name not in [d['name'] for d in duplicates]:
                    duplicates.append({
                        'name': name,
                        'ids': [lookup[name], consumable_id]
                    })
            else:
                lookup[name] = consumable_id
    
    if duplicates:
        print(f"⚠ Warning: Found {len(duplicates)} duplicate consumable names:")
        for dup in duplicates:
            print(f"  - '{dup['name']}': IDs {dup['ids']}")
        logger.warning(f"Found {len(duplicates)} duplicate consumable names")
    
    return lookup

def read_rates_from_csv(file_path: str) -> List[Dict[str, Any]]:
    """Read consumable rates from CSV file."""
    rates = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Skip empty rows
                if not row.get('consumable_name', '').strip():
                    continue
                rates.append(row)
        
        print(f"Found {len(rates)} rates in {file_path}")
        return rates
    except FileNotFoundError:
        print(f"Error: File {file_path} not found")
        return []
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []

def create_rate_payload(rate: Dict[str, Any], consumable_id: int) -> Dict[str, Any]:
    """Create a rate payload for the API."""
    payload = {
        "type_name": "Consumable/Supply",
        "category_name": None,
        "tool_name": None,
        "area_name": None,
        "consumable_name": rate.get('consumable_name', '').strip(),
        "amount": round(float(rate.get('amount', '0.00')), 2),
        "effective_date": None,
        "flat": convert_boolean(rate.get('flat', 'TRUE')),
        "daily": convert_boolean(rate.get('daily', 'FALSE')),
        "daily_split_multi_day_charges": False,
        "minimum_charge": None,
        "service_fee": None,
        "deleted": convert_boolean(rate.get('deleted', 'FALSE')),
        "type": 7,  # Consumable type
        "time": None,
        "category": None,
        "tool": None,
        "area": None,
        "consumable": consumable_id
    }
    
    return payload

def push_rate_to_api(rate: Dict[str, Any], consumable_id: int, logger: logging.Logger) -> Tuple[bool, Dict[str, Any]]:
    """Push a single rate to the NEMO API.
    
    Returns:
        Tuple of (success: bool, response_data: Dict)
    """
    consumable_name = rate.get('consumable_name', 'Unknown')
    payload = create_rate_payload(rate, consumable_id)
    
    try:
        response = requests.post(NEMO_RATES_API_URL, json=payload, headers=API_HEADERS)
        
        if response.status_code == 201:  # Created
            response_data = response.json()
            rate_id = response_data.get('id', 'Unknown')
            print(f"✓ Successfully pushed rate for consumable: {consumable_name}")
            logger.info(f"SUCCESS - Rate for consumable '{consumable_name}' created with ID: {rate_id}")
            logger.debug(f"Rate payload: {json.dumps(payload, indent=2)}")
            logger.debug(f"API response: {json.dumps(response_data, indent=2)}")
            return True, response_data
        elif response.status_code == 200:  # OK (sometimes used for creation)
            response_data = response.json()
            rate_id = response_data.get('id', 'Unknown')
            print(f"✓ Successfully pushed rate for consumable: {consumable_name}")
            logger.info(f"SUCCESS - Rate for consumable '{consumable_name}' created with ID: {rate_id}")
            logger.debug(f"Rate payload: {json.dumps(payload, indent=2)}")
            logger.debug(f"API response: {json.dumps(response_data, indent=2)}")
            return True, response_data
        elif response.status_code == 400:
            error_msg = response.text
            print(f"✗ Bad request for rate '{consumable_name}': {error_msg}")
            logger.error(f"BAD REQUEST - Rate for consumable '{consumable_name}': {error_msg}")
            logger.debug(f"Rate payload: {json.dumps(payload, indent=2)}")
            return False, {}
        elif response.status_code == 401:
            error_msg = "Authentication failed: Check your NEMO_TOKEN"
            print(f"✗ Authentication failed for rate '{consumable_name}': Check your NEMO_TOKEN")
            logger.error(f"AUTHENTICATION FAILED - Rate for consumable '{consumable_name}': {error_msg}")
            return False, {}
        elif response.status_code == 403:
            error_msg = "Permission denied: Check your API permissions"
            print(f"✗ Permission denied for rate '{consumable_name}': Check your API permissions")
            logger.error(f"PERMISSION DENIED - Rate for consumable '{consumable_name}': {error_msg}")
            return False, {}
        elif response.status_code == 409:
            error_msg = response.text
            print(f"✗ Conflict for rate '{consumable_name}': {error_msg}")
            logger.warning(f"CONFLICT - Rate for consumable '{consumable_name}': {error_msg}")
            return False, {}
        else:
            error_msg = response.text
            print(f"✗ Unexpected error for rate '{consumable_name}': HTTP {response.status_code} - {error_msg}")
            logger.error(f"UNEXPECTED ERROR - Rate for consumable '{consumable_name}': HTTP {response.status_code} - {error_msg}")
            logger.debug(f"Rate payload: {json.dumps(payload, indent=2)}")
            return False, {}
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error for rate '{consumable_name}': {e}")
        logger.error(f"NETWORK ERROR - Rate for consumable '{consumable_name}': {e}", exc_info=True)
        return False, {}

def setup_logging() -> Tuple[logging.Logger, str]:
    """Set up logging to file with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"consumable_rates_upload_log_{timestamp}.log"
    log_dir = "logs"
    
    # Create logs directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_path = os.path.join(log_dir, log_filename)
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_path}")
    
    return logger, log_path

def main():
    """Main function to upload consumable rates."""
    logger, log_path = setup_logging()
    
    print("=" * 60)
    print("NEMO Consumable Rates Upload Script")
    print("=" * 60)
    print(f"Log file: {log_path}")
    print()
    
    # Test API connections
    print("Testing API connections...")
    if not test_api_connection(NEMO_CONSUMABLES_API_URL, logger):
        print("Exiting due to consumables API connection failure.")
        exit(1)
    
    if not test_api_connection(NEMO_RATES_API_URL, logger):
        print("Exiting due to rates API connection failure.")
        exit(1)
    
    print()
    
    # Download consumables to create lookup table
    consumables = download_all_consumables(logger)
    
    if not consumables:
        print("No consumables found. Cannot create lookup table. Exiting.")
        logger.warning("No consumables found. Cannot create lookup table.")
        exit(1)
    
    print()
    print("Creating consumable name-to-ID lookup table...")
    consumable_lookup = create_consumable_lookup(consumables)
    print(f"✓ Created lookup table with {len(consumable_lookup)} consumables")
    logger.info(f"Created lookup table with {len(consumable_lookup)} consumables")
    
    print()
    
    # Read rates from CSV
    csv_file_path = "SNSF-Data/SNSF consumables rate upload.csv"
    rates = read_rates_from_csv(csv_file_path)
    
    if not rates:
        print("No rates found to upload. Exiting.")
        logger.warning("No rates found to upload")
        exit(1)
    
    print()
    print(f"Uploading {len(rates)} consumable rates to {NEMO_RATES_API_URL}...")
    print()
    
    # Track results
    successful_uploads = []
    failed_uploads = []
    not_found_consumables = []
    
    # Upload each rate
    for i, rate in enumerate(rates, 1):
        consumable_name = rate.get('consumable_name', '').strip()
        print(f"[{i}/{len(rates)}] Processing: {consumable_name}")
        
        # Look up consumable ID
        consumable_id = consumable_lookup.get(consumable_name)
        
        if not consumable_id:
            print(f"  ⚠ Consumable '{consumable_name}' not found in lookup table")
            logger.warning(f"Consumable '{consumable_name}' not found in lookup table")
            not_found_consumables.append(consumable_name)
            failed_uploads.append({
                'consumable_name': consumable_name,
                'reason': 'Consumable not found',
                'original_data': rate
            })
            continue
        
        print(f"  → Found consumable ID: {consumable_id}")
        
        success, response_data = push_rate_to_api(rate, consumable_id, logger)
        
        if success:
            successful_uploads.append({
                'consumable_name': consumable_name,
                'consumable_id': consumable_id,
                'rate_id': response_data.get('id'),
                'amount': rate.get('amount'),
                'original_data': rate
            })
        else:
            failed_uploads.append({
                'consumable_name': consumable_name,
                'consumable_id': consumable_id,
                'reason': 'API upload failed',
                'original_data': rate
            })
        
        # Small delay to avoid overwhelming the API
        if i < len(rates):
            time.sleep(0.5)
    
    # Summary
    print()
    print("=" * 60)
    print("Upload Summary")
    print("=" * 60)
    print(f"Total rates: {len(rates)}")
    print(f"Successful uploads: {len(successful_uploads)}")
    print(f"Failed uploads: {len(failed_uploads)}")
    print(f"Consumables not found: {len(not_found_consumables)}")
    print()
    
    if not_found_consumables:
        print("Consumables not found in lookup table:")
        for name in not_found_consumables:
            print(f"  - {name}")
        print()
        logger.warning(f"Consumables not found: {not_found_consumables}")
    
    # Save successful uploads to JSON file
    if successful_uploads:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"created_consumable_rates_{timestamp}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(successful_uploads, f, indent=2)
        print(f"✓ Successfully uploaded rates saved to: {output_file}")
        logger.info(f"Successfully uploaded rates saved to: {output_file}")
    
    # Log failed uploads
    if failed_uploads:
        logger.warning(f"Failed to upload {len(failed_uploads)} rates")
        for failed in failed_uploads:
            logger.warning(f"Failed: {failed['consumable_name']} - {failed.get('reason', 'Unknown error')}")
    
    print()
    print(f"Detailed log saved to: {log_path}")
    print("=" * 60)

if __name__ == "__main__":
    main()

