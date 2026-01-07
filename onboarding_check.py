#!/usr/bin/env python3
"""
Script to update user onboarding phases in NEMO for SNSF external users.
Reads users from CSV and Excel files and sets onboarding_phases to [2] for matched users.
Matches users by email address (primary) or name (fallback).
"""

# Import the requests library to make HTTP API calls
import requests
# Import json to parse and create JSON data
import json
# Import os to access environment variables and file system operations
import os
# Import type hints to make code more readable and help with IDE autocomplete
from typing import List, Dict, Any, Tuple, Optional
# Import time to add delays between API calls
import time
# Import logging to create log files and track what the script does
import logging
# Import datetime to work with dates and times
from datetime import datetime
# Import dotenv to load environment variables from a .env file
from dotenv import load_dotenv
# Import pandas to read Excel files
import pandas as pd

# Load environment variables from .env file (if it exists)
# This reads variables like NEMO_TOKEN from a .env file in the same directory
load_dotenv()

# Define the base URL for the NEMO users API endpoint
# This is where we'll make GET requests to download users and PATCH requests to update them
NEMO_USERS_API_URL = "https://nemo.stanford.edu/api/users/"

# Get the NEMO authentication token from environment variables
# The token is required to authenticate API requests
NEMO_TOKEN = os.getenv('NEMO_TOKEN')
# Check if the token was found in the environment
if not NEMO_TOKEN:
    # If no token found, print error messages and exit the script
    print("Error: NEMO_TOKEN not found in environment variables or .env file")
    print("Please create a .env file with: NEMO_TOKEN=your_token_here")
    print("Or set the environment variable: export NEMO_TOKEN=your_token_here")
    exit(1)  # Exit with error code 1
else:
    # If token was found, print confirmation message
    print("NEMO_TOKEN found in environment")

# Create HTTP headers that will be sent with every API request
# These headers tell the server:
# - Authorization: We're using token-based authentication (the NEMO_TOKEN)
# - Content-Type: We're sending JSON data
# - Accept: We want to receive JSON responses
API_HEADERS = {
    'Authorization': f'Token {NEMO_TOKEN}',  # Format: "Token <actual_token_value>"
    'Content-Type': 'application/json',       # Tell server we're sending JSON
    'Accept': 'application/json'              # Tell server we want JSON back
}

def test_api_connection(logger: logging.Logger) -> bool:
    """
    Test the API connection and authentication before proceeding.
    
    This function makes a simple GET request to verify:
    1. We can reach the API server
    2. Our authentication token is valid
    3. We have permission to access the API
    
    Args:
        logger: A logging.Logger object to write log messages
        
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        # Make a GET request to the users API endpoint
        # This is a simple test to see if we can connect and authenticate
        response = requests.get(NEMO_USERS_API_URL, headers=API_HEADERS)
        
        # Check the HTTP status code returned by the server
        if response.status_code == 200:
            # 200 = Success! The API is reachable and our token works
            print("✓ API connection successful")
            logger.info("API connection test: SUCCESS")
            return True
        elif response.status_code == 401:
            # 401 = Unauthorized - our token is invalid or missing
            print("✗ Authentication failed: Check your NEMO_TOKEN")
            logger.error("API connection test: AUTHENTICATION FAILED")
            return False
        elif response.status_code == 403:
            # 403 = Forbidden - our token is valid but we don't have permission
            print("✗ Permission denied: Check your API permissions")
            logger.error("API connection test: PERMISSION DENIED")
            return False
        else:
            # Any other status code means something unexpected happened
            print(f"✗ API connection failed: HTTP {response.status_code}")
            logger.error(f"API connection test: FAILED - HTTP {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        # This catches network errors like "can't reach server" or "timeout"
        # This is different from HTTP error codes - it means we couldn't even connect
        print(f"✗ Network error connecting to API: {e}")
        logger.error(f"API connection test: NETWORK ERROR - {e}")
        return False

def download_all_users(logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Download all users from the NEMO API.
    
    The NEMO API uses pagination, meaning it returns users in pages (e.g., 100 users per page).
    This function loops through all pages until it has downloaded every user.
    
    Args:
        logger: A logging.Logger object to write log messages
        
    Returns:
        List[Dict[str, Any]]: A list of user dictionaries, each containing user data
                              Returns empty list [] if download fails
    """
    print("Downloading users from NEMO API...")
    logger.info("Downloading users from NEMO API")
    
    # Initialize an empty list to store all users we download
    all_users = []
    # Start at page 1 (first page of results)
    page = 1
    
    # Loop forever until we break out (when there are no more pages)
    while True:
        try:
            # Create parameters for the API request
            # The 'page' parameter tells the API which page of results we want
            params = {'page': page}
            # Make GET request to download users from the current page
            response = requests.get(NEMO_USERS_API_URL, headers=API_HEADERS, params=params)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response into a Python dictionary
                response_data = response.json()
                
                # The API can return data in two formats:
                # Format 1: Paginated response with 'results' key containing the users
                if 'results' in response_data:
                    # Extract the list of users from the 'results' key
                    users = response_data['results']
                    print(f"  Page {page}: Retrieved {len(users)} users")
                else:
                    # Format 2: Direct list response (no pagination wrapper)
                    # The response_data IS the list of users
                    users = response_data
                    print(f"  Retrieved {len(users)} users (no pagination)")
                
                # If we got an empty list, we've reached the end
                if not users:
                    break  # Exit the while loop
                
                # Add the users from this page to our master list
                # extend() adds all items from the list to all_users
                all_users.extend(users)
                
                # Check if there are more pages to download
                # Paginated responses include a 'next' field with the URL for the next page
                if 'next' in response_data and response_data['next']:
                    # If 'next' exists and is not None/empty, there's another page
                    page += 1  # Move to the next page
                else:
                    # No 'next' field means we've reached the last page
                    break  # Exit the while loop
                    
            elif response.status_code == 401:
                # 401 = Authentication failed - token is invalid
                print("✗ Authentication failed: Check your NEMO_TOKEN")
                logger.error("Authentication failed while downloading users")
                return []  # Return empty list to indicate failure
            elif response.status_code == 403:
                # 403 = Permission denied - token is valid but no permission
                print("✗ Permission denied: Check your API permissions")
                logger.error("Permission denied while downloading users")
                return []  # Return empty list to indicate failure
            else:
                # Any other HTTP status code means an error occurred
                print(f"✗ Failed to download users: HTTP {response.status_code} - {response.text}")
                logger.error(f"Failed to download users: HTTP {response.status_code}")
                return []  # Return empty list to indicate failure
                
        except requests.exceptions.RequestException as e:
            # Catch network errors (can't reach server, timeout, etc.)
            print(f"✗ Network error downloading users: {e}")
            logger.error(f"Network error downloading users: {e}", exc_info=True)
            return []  # Return empty list to indicate failure
        except json.JSONDecodeError as e:
            # Catch errors when trying to parse the response as JSON
            # This happens if the server returns invalid JSON
            print(f"✗ Error parsing JSON response: {e}")
            logger.error(f"Error parsing JSON response: {e}")
            return []  # Return empty list to indicate failure
    
    # After the loop finishes, print and log the total number of users downloaded
    print(f"✓ Total users downloaded: {len(all_users)}")
    logger.info(f"Total users downloaded: {len(all_users)}")
    # Return the complete list of all users
    return all_users

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
    log_filename = f"add_snsf_external_user_onboarding_log_{timestamp}.log"
    # Define the directory where log files will be stored
    log_dir = "logs"
    
    # Check if the logs directory exists
    if not os.path.exists(log_dir):
        # If it doesn't exist, create it
        # makedirs() creates the directory and any parent directories needed
        os.makedirs(log_dir)
    
    # Combine the directory path and filename to create the full file path
    # Example: logs/add_snsf_external_user_onboarding_log_20251202_143207.log
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
    # __name__ is the module name (e.g., '__main__' or 'add_snsf_external_user_onboarding')
    logger = logging.getLogger(__name__)
    # Log that logging has been initialized
    logger.info(f"Logging initialized. Log file: {log_path}")
    
    # Return both the logger and the log file path
    return logger, log_path

def main():
    """
    Main function to update user onboarding phases for SNSF users (internal and external).
    
    This function orchestrates the entire process:
    1. Sets up logging
    2. Tests API connection
    3. Reads users from CSV files (internal and external)
    4. Downloads all users from NEMO API
    5. Creates lookup dictionaries for matching
    6. Matches users by email (primary) or name (fallback)
    7. Updates matched users' onboarding_phases to [2]
    8. Provides a summary and saves results
    """
    # Set up logging and get the logger object and log file path
    logger, log_path = setup_logging()
    
    # Print a header banner for the script output
    print("=" * 60)  # Print 60 equals signs as a separator line
    print("NEMO Update SNSF User Onboarding Phases Script")
    print("=" * 60)
    print(f"Log file: {log_path}")  # Show where the log file is saved
    print()  # Print a blank line
    
    # Test the API connection before proceeding
    # If the connection fails, exit the script immediately
    if not test_api_connection(logger):
        print("Exiting due to API connection failure.")
        exit(1)  # Exit with error code 1
    
    print()  # Print a blank line
    
    # Read users from CSV files
    
    # Download all users from NEMO API
    nemo_users = download_all_users(logger)
    
    if not nemo_users:
        print("No users found in NEMO. Cannot proceed. Exiting.")
        logger.warning("No users found in NEMO. Cannot proceed.")
        exit(1)  # Exit with error code 1
    
    # Initialize lists to track the results of processing users
    successful_updates = []  # Users we successfully updated
    failed_updates = []      # Users we tried to update but failed
    not_found_users = []     # Users we couldn't find in NEMO
    skipped_users = []       # Users we skipped (already updated, etc.)
    processed_user_ids = set()  # Track which user IDs we've already processed
    
    # Get the total number of users to process
    total_users = len(nemo_users)
    print(f"Processing {total_users} users...")
    print()  # Print a blank line
    
    # Initialize counters and dictionaries BEFORE the loop
    SNSF_count = 0
    SNF_count = 0
    neither_count = 0
    neither_dict = {}
    both_count = 0
    both_dict = {}
    
    # Loop through each user
    for i, user in enumerate(nemo_users, 1):
        email = user.get('email')
        first_name = user.get('first_name')
        last_name = user.get('last_name')
        user_id = user.get('id')
        current_onboarding = user.get('onboarding_phases', [])
        
        # Check if user has both onboarding phases
        if 1 in current_onboarding and 2 in current_onboarding:
            both_count += 1
            both_dict[user_id] = user
        elif current_onboarding == [1]:
            SNF_count += 1
        elif current_onboarding == [2]:
            SNSF_count += 1
        else:
            neither_count += 1
            neither_dict[user_id] = user
    
    # Print a summary of the results

    print(f"neither dict: {neither_dict}")
    print(f"both dict: {both_dict}")
    print(f"SNSF count: {SNSF_count}/{total_users}")
    print(f"SNF count: {SNF_count}/{total_users}")
    print(f"neither count: {neither_count}/{total_users}")
    print(f"both count: {both_count}/{total_users}")

# This code only runs if the script is executed directly (not imported as a module)
# When Python runs a file, it sets __name__ to "__main__" for the main file
# If this file were imported by another script, __name__ would be the module name instead
if __name__ == "__main__":
    # Call the main() function to start the script
    main()

