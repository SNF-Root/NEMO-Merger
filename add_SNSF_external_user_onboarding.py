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

EXTERNAL_USERS_FILE_PATH = "/Users/adenton/Desktop/NEMO-Merger/SNSF-Data/Copy-external-users.csv"
INTERNAL_USERS_FILE_PATH = "/Users/adenton/Desktop/NEMO-Merger/SNSF-Data/Internal User Tracking and Emails.csv"

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

def create_user_lookups(users: List[Dict[str, Any]], logger: logging.Logger) -> Dict[str, Dict[str, Any]]:
    """
    Create lookup dictionaries for matching users by email and name.
    
    Args:
        users: List of user dictionaries from NEMO API
        logger: A logging.Logger object to write log messages
        
    Returns:
        Dict containing:
            - email_lookup: email (lowercase) -> user dict
            - name_lookup: "first_name|last_name" (lowercase) -> user dict or list of user dicts
    """
    email_lookup = {}
    name_lookup = {}
    
    for user in users:
        # Create email lookup (normalize to lowercase)
        email = user.get('email', '').strip().lower() if user.get('email') else None
        if email and '@' in email:
            if email in email_lookup:
                logger.warning(f"Duplicate email found: {email}")
            email_lookup[email] = user
        
        # Create name lookup (normalize to lowercase)
        first_name = user.get('first_name', '').strip().lower() if user.get('first_name') else None
        last_name = user.get('last_name', '').strip().lower() if user.get('last_name') else None
        
        if first_name and last_name:
            name_key = f"{first_name}|{last_name}"
            if name_key in name_lookup:
                # Handle duplicates - convert to list if not already
                if not isinstance(name_lookup[name_key], list):
                    name_lookup[name_key] = [name_lookup[name_key]]
                name_lookup[name_key].append(user)
            else:
                name_lookup[name_key] = user
    
    print(f"✓ Created email lookup with {len(email_lookup)} entries")
    print(f"✓ Created name lookup with {len(name_lookup)} entries")
    logger.info(f"Created email lookup with {len(email_lookup)} entries")
    logger.info(f"Created name lookup with {len(name_lookup)} entries")
    
    return {
        'email_lookup': email_lookup,
        'name_lookup': name_lookup
    }

def read_external_users(file_path: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Read external users from CSV file.
    
    Args:
        file_path: Path to the CSV file
        logger: A logging.Logger object to write log messages
        
    Returns:
        List of dictionaries containing user information (email, first name, last name)
    """
    print(f"Reading external users CSV file: {file_path}...")
    logger.info(f"Reading external users CSV file: {file_path}")
    
    try:
        if not os.path.exists(file_path):
            print(f"✗ Error: File not found: {file_path}")
            logger.error(f"File not found: {file_path}")
            return []
        
        # Read CSV file
        df = pd.read_csv(file_path)
        print(f"✓ Read {len(df)} rows from CSV")
        logger.info(f"Read {len(df)} rows from CSV")
        print(f"  Columns: {', '.join(df.columns.tolist())}")
        
        # Find email, first name, and last name columns
        email_col = None
        first_col = None
        last_col = None
        
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if col_lower == 'email' and not email_col:
                email_col = col
            elif (col_lower == 'first' or col_lower == 'first name') and not first_col:
                first_col = col
            elif (col_lower == 'last' or col_lower == 'last name') and not last_col:
                last_col = col
        
        if not email_col and (not first_col or not last_col):
            print("✗ Error: Could not find Email or First/Last name columns")
            logger.error("Could not find Email or First/Last name columns")
            return []
        
        # Extract user information
        users = []
        for idx, row in df.iterrows():
            email = None
            first_name = None
            last_name = None
            
            if email_col and pd.notna(row[email_col]):
                email = str(row[email_col]).strip().lower()
                if not email or email == 'nan':
                    email = None
            
            if first_col and pd.notna(row[first_col]):
                first_name = str(row[first_col]).strip()
                if first_name.lower() == 'nan':
                    first_name = None
            
            if last_col and pd.notna(row[last_col]):
                last_name = str(row[last_col]).strip()
                if last_name.lower() == 'nan':
                    last_name = None
            
            # Only add if we have at least email or both first and last name
            if email or (first_name and last_name):
                users.append({
                    'email': email,
                    'first_name': first_name,
                    'last_name': last_name,
                    'source': 'External CSV',
                    'source_row': idx + 2  # +2 because idx is 0-based and CSV has header
                })
        
        print(f"✓ Extracted {len(users)} external users from CSV")
        logger.info(f"Extracted {len(users)} external users from CSV")
        return users
        
    except Exception as e:
        print(f"✗ Error reading CSV file: {e}")
        logger.error(f"Error reading CSV file {file_path}: {e}", exc_info=True)
        return []

def read_internal_users(file_path: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Read internal users from CSV file.
    
    Args:
        file_path: Path to the CSV file
        logger: A logging.Logger object to write log messages
        
    Returns:
        List of dictionaries containing user information (email, first name, last name)
    """
    print(f"Reading internal users CSV file: {file_path}...")
    logger.info(f"Reading internal users CSV file: {file_path}")
    
    try:
        if not os.path.exists(file_path):
            print(f"✗ Error: File not found: {file_path}")
            logger.error(f"File not found: {file_path}")
            return []
        
        # Read CSV file
        df = pd.read_csv(file_path)
        print(f"✓ Read {len(df)} rows from CSV")
        logger.info(f"Read {len(df)} rows from CSV")
        print(f"  Columns: {', '.join(df.columns.tolist())}")
        
        # Find email, first name, and last name columns
        email_col = None
        first_col = None
        last_col = None
        
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if col_lower == 'email' and not email_col:
                email_col = col
            elif (col_lower == 'first' or col_lower == 'first name') and not first_col:
                first_col = col
            elif (col_lower == 'last' or col_lower == 'last name') and not last_col:
                last_col = col
        
        if not email_col and (not first_col or not last_col):
            print("✗ Error: Could not find Email or First/Last name columns")
            logger.error("Could not find Email or First/Last name columns")
            return []
        
        # Extract user information
        users = []
        for idx, row in df.iterrows():
            email = None
            first_name = None
            last_name = None
            
            if email_col and pd.notna(row[email_col]):
                email = str(row[email_col]).strip().lower()
                if not email or email == 'nan':
                    email = None
            
            if first_col and pd.notna(row[first_col]):
                first_name = str(row[first_col]).strip()
                if first_name.lower() == 'nan':
                    first_name = None
            
            if last_col and pd.notna(row[last_col]):
                last_name = str(row[last_col]).strip()
                if last_name.lower() == 'nan':
                    last_name = None
            
            # Only add if we have at least email or both first and last name
            if email or (first_name and last_name):
                users.append({
                    'email': email,
                    'first_name': first_name,
                    'last_name': last_name,
                    'source': 'Internal CSV',
                    'source_row': idx + 2  # +2 because idx is 0-based and CSV has header
                })
        
        print(f"✓ Extracted {len(users)} internal users from CSV")
        logger.info(f"Extracted {len(users)} internal users from CSV")
        return users
        
    except Exception as e:
        print(f"✗ Error reading CSV file: {e}")
        logger.error(f"Error reading CSV file {file_path}: {e}", exc_info=True)
        return []


def update_user_onboarding(user_id: int, email: str, username: str, logger: logging.Logger) -> bool:
    """
    Update a user's onboarding_phases field to [2] via PATCH request.
    
    PATCH is used instead of PUT because:
    - PATCH updates only the specified fields (onboarding_phases)
    - PUT would require sending the entire user object, which could overwrite other fields
    
    Args:
        user_id: The unique ID of the user to update (integer)
        email: The user's email address (for logging purposes)
        username: The user's username (for logging purposes)
        logger: A logging.Logger object to write log messages
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    # Construct the URL for updating this specific user
    # Format: https://nemo.stanford.edu/api/users/123/ (where 123 is the user_id)
    update_url = f"{NEMO_USERS_API_URL}{user_id}/"
    
    # Create the data payload to send in the PATCH request
    # We're only updating the onboarding_phases field, setting it to a list containing 2
    # The value [2] means the user has completed onboarding phase 2
    payload = {'onboarding_phases': [2]}
    
    try:
        # Make a PATCH request to update the user
        # PATCH updates only the fields we specify in the payload
        # json=payload automatically converts the dictionary to JSON format
        response = requests.patch(update_url, json=payload, headers=API_HEADERS)
        
        # Check the HTTP status code to see if the update was successful
        if response.status_code == 200:
            # 200 = Success! The user was updated successfully
            logger.info(f"SUCCESS: Updated user {user_id} ({email}) with onboarding_phases=[2]")
            return True
        elif response.status_code == 400:
            # 400 = Bad Request - the data we sent was invalid (e.g., wrong format)
            error_msg = response.text  # Get the error message from the server
            logger.error(f"FAILED: Bad request for user {user_id} ({email}) - {error_msg}")
            # Log the payload we tried to send for debugging
            logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
            return False
        elif response.status_code == 401:
            # 401 = Authentication failed - our token is invalid
            logger.error(f"FAILED: Authentication failed for user {user_id} ({email})")
            return False
        elif response.status_code == 403:
            # 403 = Permission denied - we don't have permission to update users
            logger.error(f"FAILED: Permission denied for user {user_id} ({email})")
            return False
        elif response.status_code == 404:
            # 404 = Not Found - the user with this ID doesn't exist
            logger.error(f"FAILED: User {user_id} ({email}) not found")
            return False
        else:
            # Any other status code means an unexpected error occurred
            error_msg = response.text  # Get the error message from the server
            logger.error(f"FAILED: HTTP {response.status_code} for user {user_id} ({email}) - {error_msg}")
            return False
            
    except requests.exceptions.RequestException as e:
        # Catch network errors (can't reach server, timeout, etc.)
        logger.error(f"FAILED: Network error for user {user_id} ({email}) - {str(e)}")
        return False

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
    internal_users = read_internal_users(INTERNAL_USERS_FILE_PATH, logger)
    external_users = read_external_users(EXTERNAL_USERS_FILE_PATH, logger)
    
    # Combine users from both sources
    all_users = internal_users + external_users
    
    if not all_users:
        print("No users found in CSV files. Cannot proceed. Exiting.")
        logger.warning("No users found in CSV files. Cannot proceed.")
        exit(1)  # Exit with error code 1
    
    print(f"✓ Total users to process: {len(all_users)}")
    print()  # Print a blank line
    
    # Download all users from NEMO API
    nemo_users = download_all_users(logger)
    
    if not nemo_users:
        print("No users found in NEMO. Cannot proceed. Exiting.")
        logger.warning("No users found in NEMO. Cannot proceed.")
        exit(1)  # Exit with error code 1
    
    print()  # Print a blank line
    
    # Create lookup dictionaries for matching
    lookups = create_user_lookups(nemo_users, logger)
    email_lookup = lookups['email_lookup']
    name_lookup = lookups['name_lookup']
    
    print()  # Print a blank line
    
    # Initialize lists to track the results of processing users
    successful_updates = []  # Users we successfully updated
    failed_updates = []      # Users we tried to update but failed
    not_found_users = []     # Users we couldn't find in NEMO
    skipped_users = []       # Users we skipped (already updated, etc.)
    processed_user_ids = set()  # Track which user IDs we've already processed
    
    # Get the total number of users to process
    total_users = len(all_users)
    print(f"Processing {total_users} users...")
    print()  # Print a blank line
    
    # Loop through each user
    for i, user in enumerate(all_users, 1):
        email = user.get('email')
        first_name = user.get('first_name')
        last_name = user.get('last_name')
        source = user.get('source', 'Unknown')
        source_row = user.get('source_row', 'Unknown')
        
        # Try to match user by email first
        matched_user = None
        match_method = None
        
        if email:
            matched_user = email_lookup.get(email)
            if matched_user:
                match_method = 'email'
        
        # If not found by email, try to match by name
        if not matched_user and first_name and last_name:
            first_lower = first_name.strip().lower()
            last_lower = last_name.strip().lower()
            name_key = f"{first_lower}|{last_lower}"
            
            name_match = name_lookup.get(name_key)
            if name_match:
                if isinstance(name_match, list):
                    # Multiple users with same name - warn and use first one
                    logger.warning(f"Multiple users found with name '{first_name} {last_name}' (source: {source}, row: {source_row}) - using first match")
                    matched_user = name_match[0]
                    match_method = f'name (first of {len(name_match)} matches)'
                else:
                    matched_user = name_match
                    match_method = 'name'
        
        # If we couldn't find the user, add to not_found list
        if not matched_user:
            identifier = email if email else f"{first_name} {last_name}" if (first_name and last_name) else "unknown"
            not_found_users.append({
                'identifier': identifier,
                'email': email,
                'first_name': first_name,
                'last_name': last_name,
                'source': source,
                'source_row': source_row
            })
            if i % 50 == 0:
                print(f"[{i}/{total_users}] Processed... (found: {len(successful_updates) + len(skipped_users)}, not found: {len(not_found_users)}, failed: {len(failed_updates)})")
            continue
        
        # Get user information
        user_id = matched_user.get('id')
        user_email = matched_user.get('email', 'N/A')
        user_username = matched_user.get('username', 'N/A')
        current_onboarding = matched_user.get('onboarding_phases', [])
        
        # Check if this user already has onboarding_phases set to [2]
        if current_onboarding == [2]:
            skipped_users.append({
                'user_id': user_id,
                'email': user_email,
                'username': user_username,
                'match_method': match_method,
                'source': source,
                'source_row': source_row,
                'reason': 'Already has onboarding_phases=[2]'
            })
            if i % 50 == 0:
                print(f"[{i}/{total_users}] Processed... (found: {len(successful_updates) + len(skipped_users)}, not found: {len(not_found_users)}, failed: {len(failed_updates)})")
            continue
        
        # Check if we've already processed this user ID (avoid duplicates)
        if user_id in processed_user_ids:
            skipped_users.append({
                'user_id': user_id,
                'email': user_email,
                'username': user_username,
                'match_method': match_method,
                'source': source,
                'source_row': source_row,
                'reason': 'Already processed (duplicate in source files)'
            })
            if i % 50 == 0:
                print(f"[{i}/{total_users}] Processed... (found: {len(successful_updates) + len(skipped_users)}, not found: {len(not_found_users)}, failed: {len(failed_updates)})")
            continue
        
        # Mark this user ID as processed
        processed_user_ids.add(user_id)
        
        # Update the user's onboarding_phases
        identifier = email if email else f"{first_name} {last_name}" if (first_name and last_name) else f"user_{user_id}"
        print(f"[{i}/{total_users}] Updating user {user_id} ({identifier}) - matched by {match_method}")
        success = update_user_onboarding(user_id, user_email, user_username, logger)
        
        # Check if the update was successful
        if success:
            # Add to successful updates list with user details
            successful_updates.append({
                'user_id': user_id,
                'email': user_email,
                'username': user_username,
                'match_method': match_method,
                'source': source,
                'source_row': source_row,
                'external_email': email,
                'external_first_name': first_name,
                'external_last_name': last_name
            })
            print(f"  ✓ Updated successfully")
        else:
            # Add to failed updates list with reason
            failed_updates.append({
                'user_id': user_id,
                'email': user_email,
                'username': user_username,
                'match_method': match_method,
                'source': source,
                'source_row': source_row,
                'external_email': email,
                'external_first_name': first_name,
                'external_last_name': last_name,
                'reason': 'API update failed'
            })
            print(f"  ✗ Update failed")
        
        # Add a small delay (0.5 seconds) between API calls
        # This prevents overwhelming the API server with too many requests at once
        time.sleep(0.5)
    
    # Print a summary of the results
    print()  # Print a blank line
    print("=" * 60)  # Print separator line
    print("Update Summary")
    print("=" * 60)
    print(f"Total users processed: {total_users}")
    print(f"Successfully updated: {len(successful_updates)}")
    print(f"Skipped: {len(skipped_users)}")
    print(f"Not found in NEMO: {len(not_found_users)}")
    print(f"Failed updates: {len(failed_updates)}")
    print()  # Print a blank line
    
    # Save the list of successfully updated users to a JSON file
    if successful_updates:
        # Create a timestamp for the filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Create filename with timestamp
        output_file = f"updated_snsf_external_user_onboarding_{timestamp}.json"
        # Open the file for writing (creates file if it doesn't exist)
        with open(output_file, 'w', encoding='utf-8') as f:
            # Write the list as JSON with nice formatting (indent=2 makes it readable)
            json.dump(successful_updates, f, indent=2)
        print(f"✓ Successfully updated users saved to: {output_file}")
        logger.info(f"Successfully updated users saved to: {output_file}")
    
    # Save the list of not found users to a JSON file
    if not_found_users:
        # Create a timestamp for the filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Create filename with timestamp
        output_file = f"not_found_snsf_external_users_{timestamp}.json"
        # Open the file for writing (creates file if it doesn't exist)
        with open(output_file, 'w', encoding='utf-8') as f:
            # Write the list as JSON with nice formatting (indent=2 makes it readable)
            json.dump(not_found_users, f, indent=2)
        print(f"✓ Not found users saved to: {output_file}")
        logger.info(f"Not found users saved to: {output_file}")
    
    # Log details about any failed updates
    if failed_updates:
        # Log the total count of failures
        logger.warning(f"Failed to update {len(failed_updates)} users")
        # Log each failed update with its reason
        for failed in failed_updates:
            logger.warning(f"Failed: User {failed['user_id']} ({failed['email']}) - {failed.get('reason', 'Unknown error')}")
    
    # Print final information
    print()  # Print a blank line
    print(f"Detailed log saved to: {log_path}")
    print("=" * 60)  # Print separator line

# This code only runs if the script is executed directly (not imported as a module)
# When Python runs a file, it sets __name__ to "__main__" for the main file
# If this file were imported by another script, __name__ would be the module name instead
if __name__ == "__main__":
    # Call the main() function to start the script
    main()

