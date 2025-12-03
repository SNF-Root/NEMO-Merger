#!/usr/bin/env python3
"""
Script to upload consumables from CSV file to NEMO API endpoint.
Reads consumables from SNSF-Data/SNSF consumables upload.csv and pushes them to the API.
"""

import requests
import csv
import json
import os
from typing import List, Dict, Any, Tuple
import time
import logging
from datetime import datetime
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

def read_consumables_from_csv(file_path: str) -> List[Dict[str, Any]]:
    """Read consumables from CSV file."""
    consumables = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                consumables.append(row)
        
        print(f"Found {len(consumables)} consumables in {file_path}")
        return consumables
    except FileNotFoundError:
        print(f"Error: File {file_path} not found")
        return []
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []

def clean_consumable_payload(consumable: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and prepare consumable payload for API."""
    payload = {
        'name': consumable.get('name', '').strip(),
        'quantity': int(consumable.get('quantity', 0)),
        'reusable': convert_boolean(consumable.get('reusable', 'TRUE')),
        'visible': convert_boolean(consumable.get('visible', 'FALSE')),
        'allow_self_checkout': convert_boolean(consumable.get('allow_self_checkout', 'TRUE')),
        'reminder_threshold': int(consumable.get('reminder_threshold', 50)) if consumable.get('reminder_threshold') else 50,
        'reminder_email': consumable.get('reminder_email', '').strip() if consumable.get('reminder_email') else 'shaog@stanford.edu',
        'category': None,
        'reminder_threshold_reached': False,
        'self_checkout_only_users': []
    }
    
    # Handle notes field - can be empty
    notes = consumable.get('notes', '').strip()
    if notes:
        payload['notes'] = notes
    else:
        payload['notes'] = ''
    
    return payload

def push_consumable_to_api(consumable: Dict[str, Any], api_url: str, logger: logging.Logger) -> Tuple[bool, Dict[str, Any]]:
    """Push a single consumable to the NEMO API.
    
    Returns:
        Tuple of (success: bool, response_data: Dict)
    """
    consumable_name = consumable.get('name', 'Unknown')
    payload = clean_consumable_payload(consumable)
    
    try:
        response = requests.post(api_url, json=payload, headers=API_HEADERS)
        
        if response.status_code == 201:  # Created
            response_data = response.json()
            consumable_id = response_data.get('id', 'Unknown')
            print(f"✓ Successfully pushed consumable: {consumable_name}")
            logger.info(f"SUCCESS - Consumable '{consumable_name}' created with ID: {consumable_id}")
            logger.debug(f"Consumable payload: {json.dumps(payload, indent=2)}")
            logger.debug(f"API response: {json.dumps(response_data, indent=2)}")
            return True, response_data
        elif response.status_code == 400:
            error_msg = response.text
            print(f"✗ Bad request for consumable '{consumable_name}': {error_msg}")
            logger.error(f"BAD REQUEST - Consumable '{consumable_name}': {error_msg}")
            logger.debug(f"Consumable payload: {json.dumps(payload, indent=2)}")
            return False, {}
        elif response.status_code == 401:
            error_msg = "Authentication failed: Check your NEMO_TOKEN"
            print(f"✗ Authentication failed for consumable '{consumable_name}': Check your NEMO_TOKEN")
            logger.error(f"AUTHENTICATION FAILED - Consumable '{consumable_name}': {error_msg}")
            return False, {}
        elif response.status_code == 403:
            error_msg = "Permission denied: Check your API permissions"
            print(f"✗ Permission denied for consumable '{consumable_name}': Check your API permissions")
            logger.error(f"PERMISSION DENIED - Consumable '{consumable_name}': {error_msg}")
            return False, {}
        elif response.status_code == 409:
            error_msg = response.text
            print(f"✗ Conflict for consumable '{consumable_name}': {error_msg}")
            logger.warning(f"CONFLICT - Consumable '{consumable_name}': {error_msg}")
            return False, {}
        else:
            error_msg = response.text
            print(f"✗ Unexpected error for consumable '{consumable_name}': HTTP {response.status_code} - {error_msg}")
            logger.error(f"UNEXPECTED ERROR - Consumable '{consumable_name}': HTTP {response.status_code} - {error_msg}")
            logger.debug(f"Consumable payload: {json.dumps(payload, indent=2)}")
            return False, {}
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error for consumable '{consumable_name}': {e}")
        logger.error(f"NETWORK ERROR - Consumable '{consumable_name}': {e}", exc_info=True)
        return False, {}

def test_api_connection(logger: logging.Logger) -> bool:
    """Test the API connection and authentication."""
    try:
        response = requests.get(NEMO_API_URL, headers=API_HEADERS)
        if response.status_code == 200:
            print("✓ API connection successful")
            logger.info("API connection test: SUCCESS")
            return True
        elif response.status_code == 401:
            print("✗ Authentication failed: Check your NEMO_TOKEN")
            logger.error("API connection test: AUTHENTICATION FAILED")
            return False
        elif response.status_code == 403:
            print("✗ Permission denied: Check your API permissions")
            logger.error("API connection test: PERMISSION DENIED")
            return False
        else:
            print(f"✗ API connection failed: HTTP {response.status_code}")
            logger.error(f"API connection test: FAILED - HTTP {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error connecting to API: {e}")
        logger.error(f"API connection test: NETWORK ERROR - {e}")
        return False

def setup_logging() -> Tuple[logging.Logger, str]:
    """Set up logging to file with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"consumables_upload_log_{timestamp}.log"
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
    """Main function to upload consumables."""
    logger, log_path = setup_logging()
    
    print("=" * 60)
    print("NEMO Consumables Upload Script")
    print("=" * 60)
    print(f"Log file: {log_path}")
    print()
    
    # Test API connection
    if not test_api_connection(logger):
        print("Exiting due to API connection failure.")
        exit(1)
    
    print()
    
    # Read consumables from CSV
    csv_file_path = "SNSF-Data/SNSF consumables upload.csv"
    consumables = read_consumables_from_csv(csv_file_path)
    
    if not consumables:
        print("No consumables found to upload. Exiting.")
        logger.warning("No consumables found to upload")
        exit(1)
    
    print()
    print(f"Uploading {len(consumables)} consumables to {NEMO_API_URL}...")
    print()
    
    # Track results
    successful_uploads = []
    failed_uploads = []
    
    # Upload each consumable
    for i, consumable in enumerate(consumables, 1):
        consumable_name = consumable.get('name', 'Unknown')
        print(f"[{i}/{len(consumables)}] Processing: {consumable_name}")
        
        success, response_data = push_consumable_to_api(consumable, NEMO_API_URL, logger)
        
        if success:
            successful_uploads.append({
                'name': consumable_name,
                'id': response_data.get('id'),
                'original_data': consumable
            })
        else:
            failed_uploads.append({
                'name': consumable_name,
                'original_data': consumable
            })
        
        # Small delay to avoid overwhelming the API
        if i < len(consumables):
            time.sleep(0.5)
    
    # Summary
    print()
    print("=" * 60)
    print("Upload Summary")
    print("=" * 60)
    print(f"Total consumables: {len(consumables)}")
    print(f"Successful uploads: {len(successful_uploads)}")
    print(f"Failed uploads: {len(failed_uploads)}")
    print()
    
    # Save successful uploads to JSON file
    if successful_uploads:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"created_consumables_{timestamp}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(successful_uploads, f, indent=2)
        print(f"✓ Successfully uploaded consumables saved to: {output_file}")
        logger.info(f"Successfully uploaded consumables saved to: {output_file}")
    
    # Log failed uploads
    if failed_uploads:
        logger.warning(f"Failed to upload {len(failed_uploads)} consumables")
        for failed in failed_uploads:
            logger.warning(f"Failed: {failed['name']}")
    
    print()
    print(f"Detailed log saved to: {log_path}")
    print("=" * 60)

if __name__ == "__main__":
    main()

