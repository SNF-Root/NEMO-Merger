#!/usr/bin/env python3
"""
Script to make consumables visible if the reminder_email matches specific emails.
Makes consumables visible if reminder_email is: ajbarnum@stanford.edu, cnewcomb@stanford.edu, or shaog@stanford.edu
"""

import requests
import os
import logging
import json
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Any, Tuple

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoint for consumables
NEMO_CONSUMABLES_API_URL = "https://nemo.stanford.edu/api/consumables/"

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

# Target emails for filtering consumables
TARGET_EMAILS = [
    'ajbarnum@stanford.edu',
    'cnewcomb@stanford.edu',
    'shaog@stanford.edu'
]

def setup_logging() -> Tuple[logging.Logger, str]:
    """
    Set up logging to write messages to both a file and the console.
    
    This creates a log file with a timestamp in its name so each run creates a new log.
    Log messages will appear both in the file and printed to the console.
    
    Returns:
        Tuple[logging.Logger, str]: A tuple containing:
            - logger: The logging.Logger object to use for logging
            - log_path: The full path to the log file that was created
    """
    # Get the current date and time, format it as YYYYMMDD_HHMMSS
    # Example: 20251202_143207 (December 2, 2025 at 14:32:07)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Create a filename for the log file with the timestamp
    log_filename = f"make_consumables_visible_log_{timestamp}.log"
    # Define the directory where log files will be stored
    log_dir = "logs"
    
    # Check if the logs directory exists
    if not os.path.exists(log_dir):
        # If it doesn't exist, create it
        # makedirs() creates the directory and any parent directories needed
        os.makedirs(log_dir)
    
    # Combine the directory path and filename to create the full file path
    # Example: logs/make_consumables_visible_log_20251202_143207.log
    log_path = os.path.join(log_dir, log_filename)
    
    # Configure the logging system with these settings:
    logging.basicConfig(
        level=logging.DEBUG,  # Log all messages (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        # Format for log messages: timestamp - level - message
        # Example: 2025-12-02 14:32:07,123 - INFO - Logging initialized
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path),  # Write logs to the file
            logging.StreamHandler()          # Also print logs to console (stdout)
        ]
    )
    
    # Get a logger object for this module
    # __name__ is the module name (e.g., '__main__' or 'make_consumables_visible')
    logger = logging.getLogger(__name__)
    # Log that logging has been initialized
    logger.info("=" * 60)
    logger.info("MAKE CONSUMABLES VISIBLE SESSION STARTED")
    logger.info("=" * 60)
    logger.info(f"Log file: {log_path}")
    
    return logger, log_path

def test_api_connection(logger: logging.Logger) -> bool:
    """Test the API connection and authentication."""
    try:
        response = requests.get(NEMO_CONSUMABLES_API_URL, headers=API_HEADERS)
        if response.status_code == 200:
            logger.info("✓ API connection successful")
            return True
        elif response.status_code == 401:
            logger.error("✗ Authentication failed: Check your NEMO_TOKEN")
            return False
        else:
            logger.error(f"✗ API connection failed: HTTP {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"✗ Network error connecting to API: {e}")
        return False

def download_consumables(logger: logging.Logger) -> List[Dict[str, Any]]:
    """Download all consumables from NEMO API with pagination support."""
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
                    logger.info(f"  Page {page}: Retrieved {len(consumables)} consumables")
                else:
                    # Direct list response
                    consumables = response_data
                    logger.info(f"  Retrieved {len(consumables)} consumables (no pagination)")
                
                if not consumables:
                    break
                
                all_consumables.extend(consumables)
                
                # Check if there are more pages
                if 'next' in response_data and response_data['next']:
                    page += 1
                else:
                    break
                    
            elif response.status_code == 401:
                logger.error("✗ Authentication failed: Check your NEMO_TOKEN")
                return []
            elif response.status_code == 403:
                logger.error("✗ Permission denied: Check your API permissions")
                return []
            else:
                logger.error(f"✗ Failed to download consumables: HTTP {response.status_code} - {response.text}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ Network error downloading consumables: {e}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"✗ Error parsing JSON response: {e}")
            return []
    
    logger.info(f"✓ Successfully downloaded {len(all_consumables)} consumables")
    return all_consumables

def find_consumables_to_update(consumables: List[Dict[str, Any]], logger: logging.Logger) -> List[Dict[str, Any]]:
    """Find consumables that need to be made visible based on reminder_email."""
    consumables_to_update = []
    
    for consumable in consumables:
        reminder_email_raw = consumable.get('reminder_email')
        # Handle None or empty values safely
        reminder_email = reminder_email_raw.strip() if reminder_email_raw else ''
        
        # Check if the reminder_email matches any of our target emails
        if reminder_email in TARGET_EMAILS:
            # Only update if currently not visible
            if consumable.get('visible') == False:
                consumables_to_update.append(consumable)
                logger.info(f"Found consumable to update: {consumable['name']} (ID: {consumable['id']}, Email: {reminder_email})")
    
    return consumables_to_update

def update_consumable_visibility(consumable_id: int, payload: Dict[str, Any], logger: logging.Logger) -> bool:
    """Update a consumable's visibility via the NEMO API."""
    update_url = f"{NEMO_CONSUMABLES_API_URL}{consumable_id}/"
    try:
        logger.debug(f"  Sending payload: {payload}")
        response = requests.patch(update_url, json=payload, headers=API_HEADERS)
        if response.status_code == 200:
            logger.debug(f"  ✓ API response: {response.json()}")
            return True
        else:
            logger.error(f"✗ Failed to update consumable {consumable_id}: HTTP {response.status_code} - {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"✗ Network error updating consumable {consumable_id}: {e}")
        return False

def main():
    """Main function to make consumables visible based on reminder_email."""
    # Set up logging first
    logger, log_path = setup_logging()
    
    logger.info("Starting consumable visibility update...")
    logger.info(f"API Endpoint: {NEMO_CONSUMABLES_API_URL}")
    logger.info(f"Target emails: {', '.join(TARGET_EMAILS)}")
    logger.info("-" * 60)

    # Test API connection first
    if not test_api_connection(logger):
        logger.error("Cannot proceed without valid API connection.")
        return

    # Download consumables
    consumables = download_consumables(logger)
    if not consumables:
        logger.error("No consumables downloaded. Cannot proceed.")
        return

    # Find consumables that need updating
    consumables_to_update = find_consumables_to_update(consumables, logger)
    
    if not consumables_to_update:
        logger.info("✓ No consumables found that need visibility updates.")
        return

    logger.info(f"\nFound {len(consumables_to_update)} consumables that need updating.")
    
    # Update consumables
    success_count = 0
    for consumable in consumables_to_update:
        logger.info(f"\nUpdating consumable {consumable['id']}: {consumable['name']}")
        if update_consumable_visibility(consumable['id'], {'visible': True}, logger):
            success_count += 1
            logger.info(f"✓ Updated consumable {consumable['id']}: {consumable['name']} → visible: True")
        else:
            logger.error(f"✗ Failed to update consumable {consumable['id']}: {consumable['name']}")

    logger.info("\n" + "=" * 60)
    logger.info("CONSUMABLE VISIBILITY UPDATE SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total consumables found needing updates: {len(consumables_to_update)}")
    logger.info(f"Successfully updated: {success_count}")
    logger.info(f"Failed updates: {len(consumables_to_update) - success_count}")

if __name__ == "__main__":
    main()

