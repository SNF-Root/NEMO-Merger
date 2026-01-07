#!/usr/bin/env python3
"""
Script to enable all interlock cards in NEMO.
Sets the 'enabled' field to True for all interlock cards.
"""

import requests
import json
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Any, Tuple

# Load environment variables from .env file
load_dotenv()

# API Configuration
API_BASE_URL = "https://nemo.stanford.edu/api"
INTERLOCK_CARDS_ENDPOINT = f"{API_BASE_URL}/interlock_cards/"

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
    log_filename = f"enable_interlocks_log_{timestamp}.log"
    # Define the directory where log files will be stored
    log_dir = "logs"
    
    # Check if the logs directory exists
    if not os.path.exists(log_dir):
        # If it doesn't exist, create it
        # makedirs() creates the directory and any parent directories needed
        os.makedirs(log_dir)
    
    # Combine the directory path and filename to create the full file path
    # Example: logs/enable_interlocks_log_20251202_143207.log
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
    # __name__ is the module name (e.g., '__main__' or 'enable_interlocks')
    logger = logging.getLogger(__name__)
    # Log that logging has been initialized
    logger.info("=" * 60)
    logger.info("ENABLE INTERLOCKS SESSION STARTED")
    logger.info("=" * 60)
    logger.info(f"Log file: {log_path}")
    
    return logger, log_path

def test_api_connection(logger: logging.Logger) -> bool:
    """Test the API connection and authentication."""
    try:
        response = requests.get(INTERLOCK_CARDS_ENDPOINT, headers=API_HEADERS)
        if response.status_code == 200:
            logger.info("✓ API connection successful")
            return True
        elif response.status_code == 401:
            logger.error("✗ Authentication failed: Check your NEMO_TOKEN")
            return False
        elif response.status_code == 403:
            logger.error("✗ Permission denied: Check your API permissions")
            return False
        else:
            logger.error(f"✗ API connection failed: HTTP {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"✗ Network error connecting to API: {e}")
        return False

def get_interlock_cards(logger: logging.Logger) -> List[Dict[Any, Any]]:
    """
    Fetch all interlock cards from the API.
    
    Args:
        logger: Logger instance for logging messages
        
    Returns:
        List of interlock card dictionaries
    """
    logger.info(f"Fetching interlock cards from {INTERLOCK_CARDS_ENDPOINT}...")
    
    try:
        response = requests.get(INTERLOCK_CARDS_ENDPOINT, headers=API_HEADERS)
        response.raise_for_status()
        
        data = response.json()
        # Handle pagination if results are paginated
        if isinstance(data, dict) and 'results' in data:
            cards = data['results']
            logger.info(f"Found {len(cards)} interlock cards (page 1)")
            
            # Fetch additional pages if they exist
            while data.get('next'):
                next_url = data['next']
                response = requests.get(next_url, headers=API_HEADERS)
                response.raise_for_status()
                data = response.json()
                cards.extend(data.get('results', []))
                logger.info(f"Found {len(cards)} total interlock cards...")
            
            return cards
        elif isinstance(data, list):
            logger.info(f"Found {len(data)} interlock cards")
            return data
        else:
            logger.warning(f"Unexpected response format: {type(data)}")
            return []
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching interlock cards: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response: {e.response.text}")
        return []

def update_interlock_card(card: Dict[Any, Any], logger: logging.Logger) -> bool:
    """
    Update a single interlock card to set enabled=True.
    
    Args:
        card: Interlock card dictionary
        logger: Logger instance for logging messages
        
    Returns:
        True if update was successful, False otherwise
    """
    # Get the card ID and URL
    card_id = card.get('id')
    if not card_id:
        logger.warning(f"Card missing ID: {card}")
        return False
    
    # Use the URL from the card if available, otherwise construct it
    update_url = card.get('url') or f"{INTERLOCK_CARDS_ENDPOINT}{card_id}/"
    
    # Prepare the update payload - set enabled to True
    payload = {
        **card,  # Include all existing fields
        "enabled": True
    }
    
    try:
        logger.debug(f"Updating card {card_id} with payload: {payload}")
        response = requests.patch(update_url, headers=API_HEADERS, json=payload)
        response.raise_for_status()
        logger.debug(f"✓ API response: {response.json()}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Error updating card {card_id}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response: {e.response.text}")
        return False

def main():
    """Main function to enable all interlock cards."""
    # Set up logging first
    logger, log_path = setup_logging()
    
    logger.info("Starting interlock card enablement...")
    logger.info(f"API Endpoint: {INTERLOCK_CARDS_ENDPOINT}")
    logger.info("-" * 60)

    # Test API connection first
    if not test_api_connection(logger):
        logger.error("Cannot proceed without valid API connection.")
        return
    
    # Fetch all interlock cards
    cards = get_interlock_cards(logger)
    
    if not cards:
        logger.warning("No interlock cards found.")
        return
    
    logger.info(f"Found {len(cards)} interlock cards")
    
    # Filter cards that are currently disabled
    disabled_cards = [card for card in cards if not card.get('enabled', False)]
    logger.info(f"Cards currently disabled: {len(disabled_cards)}")
    logger.info(f"Cards already enabled: {len(cards) - len(disabled_cards)}")
    
    if not disabled_cards:
        logger.info("All interlock cards are already enabled!")
        return
    
    # Confirm before proceeding
    print(f"\nAbout to enable {len(disabled_cards)} interlock card(s)...")
    logger.info(f"About to enable {len(disabled_cards)} interlock card(s)...")
    response = input("Continue? (yes/no): ").strip().lower()
    
    if response not in ['yes', 'y']:
        logger.info("User aborted the operation.")
        return
    
    # Update each disabled card
    logger.info("Updating interlock cards...")
    success_count = 0
    fail_count = 0
    
    for card in disabled_cards:
        card_id = card.get('id')
        card_name = card.get('name', f'Card {card_id}')
        logger.info(f"Enabling: {card_name} (ID: {card_id})...")
        
        if update_interlock_card(card, logger):
            logger.info(f"✓ Successfully enabled: {card_name} (ID: {card_id})")
            success_count += 1
        else:
            logger.error(f"✗ Failed to enable: {card_name} (ID: {card_id})")
            fail_count += 1
    
    # Summary
    logger.info("=" * 60)
    logger.info("ENABLE INTERLOCKS SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Successfully enabled: {success_count}")
    logger.info(f"Failed: {fail_count}")
    logger.info(f"Total processed: {success_count + fail_count}")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()