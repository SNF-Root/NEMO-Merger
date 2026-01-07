#!/usr/bin/env python3
"""
Script to remove 'Allen/' prefix from tools with specific categories.
This removes 'Allen/' from tools with categories: Allen/Shriram, Allen/Moore, Allen/Spilker
and any nested subcategories under these main categories.
"""

import requests
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Any, Tuple

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoint for tools
NEMO_TOOLS_API_URL = "https://nemo.stanford.edu/api/tools/"

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
    log_filename = f"remove_allen_prefix_log_{timestamp}.log"
    # Define the directory where log files will be stored
    log_dir = "logs"
    
    # Check if the logs directory exists
    if not os.path.exists(log_dir):
        # If it doesn't exist, create it
        # makedirs() creates the directory and any parent directories needed
        os.makedirs(log_dir)
    
    # Combine the directory path and filename to create the full file path
    # Example: logs/remove_allen_prefix_log_20251202_143207.log
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
    # __name__ is the module name (e.g., '__main__' or 'remove_allen_prefix')
    logger = logging.getLogger(__name__)
    # Log that logging has been initialized
    logger.info("=" * 60)
    logger.info("ALLEN PREFIX REMOVAL SESSION STARTED")
    logger.info("=" * 60)
    logger.info(f"Log file: {log_path}")
    
    return logger, log_path

def test_api_connection(logger: logging.Logger) -> bool:
    """Test the API connection and authentication."""
    try:
        response = requests.get(NEMO_TOOLS_API_URL, headers=API_HEADERS)
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

def download_tools(logger: logging.Logger) -> List[Dict[str, Any]]:
    """Download the list of tools from NEMO API."""
    try:
        response = requests.get(NEMO_TOOLS_API_URL, headers=API_HEADERS)
        if response.status_code == 200:
            tools = response.json()
            logger.info(f"✓ Successfully downloaded {len(tools)} tools")
            return tools
        else:
            logger.error(f"✗ Failed to download tools: HTTP {response.status_code}")
            return []
    except requests.exceptions.RequestException as e:
        logger.error(f"✗ Network error downloading tools: {e}")
        return []

def find_tools_to_fix(tools: List[Dict[str, Any]], logger: logging.Logger) -> List[Dict[str, Any]]:
    """Find tools that need the 'Allen/' prefix removed."""
    categories_to_fix = ['Allen/McCullough']
    tools_to_fix = []

    for tool in tools:
        if '_category' in tool and tool['_category']:
            category = tool['_category']
            # Check if the category starts with any of our target prefixes
            for prefix in categories_to_fix:
                if category.startswith(prefix):
                    # Remove 'Allen/' prefix from any nested structure
                    new_category = category.replace('Allen/', '', 1)  # Only replace first occurrence
                    tool['new_category'] = new_category
                    tools_to_fix.append(tool)
                    logger.info(f"Found tool to fix: {tool['name']} - {category} → {new_category}")
                    break  # Found a match, no need to check other prefixes

    return tools_to_fix

def update_tool(tool_id: int, payload: Dict[str, Any], logger: logging.Logger) -> bool:
    """Update a tool's information via the NEMO API."""
    update_url = f"{NEMO_TOOLS_API_URL}{tool_id}/"
    try:
        logger.debug(f"  Sending payload: {payload}")
        response = requests.patch(update_url, json=payload, headers=API_HEADERS)
        if response.status_code == 200:
            logger.debug(f"  ✓ API response: {response.json()}")
            return True
        else:
            logger.error(f"✗ Failed to update tool {tool_id}: HTTP {response.status_code} - {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"✗ Network error updating tool {tool_id}: {e}")
        return False

def main():
    """Main function to remove 'Allen/' prefix from specific tools."""
    # Set up logging first
    logger, log_path = setup_logging()
    
    logger.info("Starting removal of 'Allen/' prefix from specific tools...")
    logger.info(f"API Endpoint: {NEMO_TOOLS_API_URL}")
    logger.info("-" * 60)

    # Test API connection first
    if not test_api_connection(logger):
        logger.error("Cannot proceed without valid API connection.")
        return

    # Download tools
    tools = download_tools(logger)
    if not tools:
        logger.error("No tools downloaded. Cannot proceed.")
        return

    # Find tools that need fixing
    tools_to_fix = find_tools_to_fix(tools, logger)
    
    if not tools_to_fix:
        logger.info("✓ No tools found that need the 'Allen/' prefix removed.")
        return

    logger.info(f"\nFound {len(tools_to_fix)} tools that need fixing.")
    
    # Update tools
    success_count = 0
    for tool in tools_to_fix:
        logger.info(f"\nFixing tool {tool['id']}: {tool['name']}")
        if update_tool(tool['id'], {'_category': tool['new_category']}, logger):
            success_count += 1
            logger.info(f"✓ Updated tool {tool['id']}: {tool['name']} → {tool['new_category']}")
        else:
            logger.error(f"✗ Failed to update tool {tool['id']}: {tool['name']}")

    logger.info("\n" + "=" * 60)
    logger.info("ALLEN PREFIX REMOVAL SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total tools found needing fixes: {len(tools_to_fix)}")
    logger.info(f"Successfully updated: {success_count}")
    logger.info(f"Failed updates: {len(tools_to_fix) - success_count}")

if __name__ == "__main__":
    main()
