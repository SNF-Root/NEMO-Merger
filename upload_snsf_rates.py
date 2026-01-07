#!/usr/bin/env python3
"""
Script to upload SNSF rates from CSV file to NEMO API endpoint.
Reads rates from CSV with amount, category, and tool IDs and uploads to billing rates API.
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
NEMO_RATES_API_URL = "https://nemo.stanford.edu/api/billing/rates/"
NEMO_RATE_TYPES_API_URL = "https://nemo.stanford.edu/api/billing/rate_types/"

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

def convert_date(date_str: str) -> Optional[str]:
    """Convert date string from CSV to ISO format or return None."""
    if not date_str or date_str.strip() == '':
        return None
    
    try:
        # Try parsing M/D/YYYY format
        date_obj = datetime.strptime(date_str.strip(), '%m/%d/%Y')
        return date_obj.strftime('%Y-%m-%d')
    except ValueError:
        try:
            # Try parsing YYYY-MM-DD format
            date_obj = datetime.strptime(date_str.strip(), '%Y-%m-%d')
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            print(f"⚠ Warning: Could not parse date '{date_str}', using None")
            return None

def convert_int_or_none(value: str) -> Optional[int]:
    """Convert string to integer or return None if empty."""
    if not value or value.strip() == '':
        return None
    try:
        return int(value.strip())
    except ValueError:
        return None

def convert_float(value: str) -> float:
    """Convert string to float."""
    if not value or value.strip() == '':
        return 0.0
    try:
        return float(value.strip())
    except ValueError:
        return 0.0

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

def read_rates_from_csv(file_path: str) -> List[Dict[str, Any]]:
    """Read rates from CSV file."""
    rates = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Skip rows with missing required fields
                if not row.get('amount', '').strip() or not row.get('tool', '').strip():
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

def create_rate_payload(rate: Dict[str, Any]) -> Dict[str, Any]:
    """Create a rate payload for the API based on the example structure."""
    payload = {
        "amount": str(round(convert_float(rate.get('amount', '0')), 2)),
        "effective_date": convert_date(rate.get('effective_date', '')),
        "flat": convert_boolean(rate.get('flat', 'FALSE')),
        "daily": convert_boolean(rate.get('daily', 'FALSE')),
        "daily_split_multi_day_charges": False,
        "minimum_charge": None,
        "service_fee": None,
        "deleted": convert_boolean(rate.get('deleted', 'FALSE')),
        "type": convert_int_or_none(rate.get('type', '')),
        "time": convert_int_or_none(rate.get('time', '')),
        "category": convert_int_or_none(rate.get('category', '')),
        "tool": convert_int_or_none(rate.get('tool', '')),
        "area": None,
        "consumable": None
    }
    
    return payload

def push_rate_to_api(rate: Dict[str, Any], logger: logging.Logger) -> Tuple[bool, Dict[str, Any]]:
    """Push a single rate to the NEMO API.
    
    Returns:
        Tuple of (success: bool, response_data: Dict)
    """
    tool_id = rate.get('tool', 'Unknown')
    category_id = rate.get('category', 'Unknown')
    amount = rate.get('amount', 'Unknown')
    payload = create_rate_payload(rate)
    
    try:
        response = requests.post(NEMO_RATES_API_URL, json=payload, headers=API_HEADERS)
        
        if response.status_code == 201:  # Created
            response_data = response.json()
            rate_id = response_data.get('id', 'Unknown')
            print(f"✓ Successfully pushed rate: Tool {tool_id}, Category {category_id}, Amount {amount}")
            logger.info(f"SUCCESS - Rate created with ID: {rate_id} (Tool: {tool_id}, Category: {category_id}, Amount: {amount})")
            logger.debug(f"Rate payload: {json.dumps(payload, indent=2)}")
            logger.debug(f"API response: {json.dumps(response_data, indent=2)}")
            return True, response_data
        elif response.status_code == 200:  # OK (sometimes used for creation)
            response_data = response.json()
            rate_id = response_data.get('id', 'Unknown')
            print(f"✓ Successfully pushed rate: Tool {tool_id}, Category {category_id}, Amount {amount}")
            logger.info(f"SUCCESS - Rate created with ID: {rate_id} (Tool: {tool_id}, Category: {category_id}, Amount: {amount})")
            logger.debug(f"Rate payload: {json.dumps(payload, indent=2)}")
            logger.debug(f"API response: {json.dumps(response_data, indent=2)}")
            return True, response_data
        elif response.status_code == 400:
            error_msg = response.text
            print(f"✗ Bad request for rate (Tool {tool_id}, Category {category_id}): {error_msg}")
            logger.error(f"BAD REQUEST - Rate (Tool: {tool_id}, Category: {category_id}): {error_msg}")
            logger.debug(f"Rate payload: {json.dumps(payload, indent=2)}")
            return False, {}
        elif response.status_code == 401:
            error_msg = "Authentication failed: Check your NEMO_TOKEN"
            print(f"✗ Authentication failed: Check your NEMO_TOKEN")
            logger.error(f"AUTHENTICATION FAILED - {error_msg}")
            return False, {}
        elif response.status_code == 403:
            error_msg = "Permission denied: Check your API permissions"
            print(f"✗ Permission denied: Check your API permissions")
            logger.error(f"PERMISSION DENIED - {error_msg}")
            return False, {}
        elif response.status_code == 409:
            error_msg = response.text
            print(f"✗ Conflict for rate (Tool {tool_id}, Category {category_id}): {error_msg}")
            logger.warning(f"CONFLICT - Rate (Tool: {tool_id}, Category: {category_id}): {error_msg}")
            return False, {}
        else:
            error_msg = response.text
            print(f"✗ Unexpected error for rate (Tool {tool_id}, Category {category_id}): HTTP {response.status_code} - {error_msg}")
            logger.error(f"UNEXPECTED ERROR - Rate (Tool: {tool_id}, Category: {category_id}): HTTP {response.status_code} - {error_msg}")
            logger.debug(f"Rate payload: {json.dumps(payload, indent=2)}")
            return False, {}
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error for rate (Tool {tool_id}, Category {category_id}): {e}")
        logger.error(f"NETWORK ERROR - Rate (Tool: {tool_id}, Category: {category_id}): {e}", exc_info=True)
        return False, {}

def setup_logging() -> Tuple[logging.Logger, str]:
    """Set up logging to file with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"snsf_rates_upload_log_{timestamp}.log"
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
    """Main function to upload SNSF rates."""
    logger, log_path = setup_logging()
    
    print("=" * 60)
    print("NEMO SNSF Rates Upload Script")
    print("=" * 60)
    print(f"Log file: {log_path}")
    print()
    
    # Test API connections
    print("Testing API connections...")
    if not test_api_connection(NEMO_RATES_API_URL, logger):
        print("Exiting due to rates API connection failure.")
        exit(1)
    
    print()
    
    # Read rates from CSV
    csv_file_path = "SNSF-Data/SNSF rates upload.csv"
    rates = read_rates_from_csv(csv_file_path)
    
    if not rates:
        print("No rates found to upload. Exiting.")
        logger.warning("No rates found to upload")
        exit(1)
    
    print()
    print(f"Uploading {len(rates)} rates to {NEMO_RATES_API_URL}...")
    print()
    
    # Track results
    successful_uploads = []
    failed_uploads = []
    
    # Upload each rate
    for i, rate in enumerate(rates, 1):
        tool_id = rate.get('tool', 'Unknown')
        category_id = rate.get('category', 'Unknown')
        amount = rate.get('amount', 'Unknown')
        type_id = rate.get('type', 'Unknown')
        
        print(f"[{i}/{len(rates)}] Processing: Tool {tool_id}, Category {category_id}, Type {type_id}, Amount {amount}")
        
        success, response_data = push_rate_to_api(rate, logger)
        
        if success:
            successful_uploads.append({
                'tool_id': tool_id,
                'category_id': category_id,
                'type_id': type_id,
                'amount': amount,
                'rate_id': response_data.get('id'),
                'original_data': rate
            })
        else:
            failed_uploads.append({
                'tool_id': tool_id,
                'category_id': category_id,
                'type_id': type_id,
                'amount': amount,
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
    print()
    
    # Save successful uploads to JSON file
    if successful_uploads:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"created_snsf_rates_{timestamp}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(successful_uploads, f, indent=2)
        print(f"✓ Successfully uploaded rates saved to: {output_file}")
        logger.info(f"Successfully uploaded rates saved to: {output_file}")
    
    # Log failed uploads
    if failed_uploads:
        logger.warning(f"Failed to upload {len(failed_uploads)} rates")
        for failed in failed_uploads:
            logger.warning(f"Failed: Tool {failed['tool_id']}, Category {failed['category_id']} - {failed.get('reason', 'Unknown error')}")
    
    print()
    print(f"Detailed log saved to: {log_path}")
    print("=" * 60)

if __name__ == "__main__":
    main()

