#!/usr/bin/env python3
"""
Script to update user onboarding phases in NEMO.
Downloads all users and adds onboarding phase 1 to their onboarding_phases list
if they have tool 191 (Safety Tour Tool) in their qualifications. 
Preserves any existing phases.
"""

# Import the requests library to make HTTP API calls
import requests
# Import json to parse and create JSON data
import json
# Import os to access environment variables and file system operations
import os
# Import type hints to make code more readable and help with IDE autocomplete
from typing import List, Dict, Any, Tuple
# Import time to add delays between API calls
import time
# Import logging to create log files and track what the script does
import logging
# Import datetime to work with dates and times
from datetime import datetime
# Import dotenv to load environment variables from a .env file
from dotenv import load_dotenv

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



def update_user_onboarding(user_id: int, email: str, username: str, current_onboarding: List[int], logger: logging.Logger) -> bool:
    """
    Update a user's onboarding_phases field by ADDING phase 1 if it's not already present.
    
    This function preserves existing phases and only adds phase 1 if it's missing.
    For example:
    - If user has [2, 3], it becomes [2, 3, 1]
    - If user has [1, 2], it stays [1, 2] (no change needed)
    - If user has [], it becomes [1]
    
    PATCH is used instead of PUT because:
    - PATCH updates only the specified fields (onboarding_phases)
    - PUT would require sending the entire user object, which could overwrite other fields
    
    Args:
        user_id: The unique ID of the user to update (integer)
        email: The user's email address (for logging purposes)
        username: The user's username (for logging purposes)
        current_onboarding: The current list of onboarding phases for this user
        logger: A logging.Logger object to write log messages
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    # Construct the URL for updating this specific user
    # Format: https://nemo.stanford.edu/api/users/123/ (where 123 is the user_id)
    update_url = f"{NEMO_USERS_API_URL}{user_id}/"
    
    # Ensure current_onboarding is a list (handle None or other types)
    if not isinstance(current_onboarding, list):
        current_onboarding = []
    
    # Check if phase 1 is already in the list
    # If it is, we don't need to update (though this should be caught earlier)
    if 1 in current_onboarding:
        logger.debug(f"User {user_id} ({email}) already has phase 1 in onboarding_phases: {current_onboarding}")
        return True  # Already has phase 1, no update needed
    
    # Create the updated list by adding phase 1 to existing phases
    # This preserves any existing phases (e.g., [2, 3] becomes [2, 3, 1])
    updated_onboarding = current_onboarding + [1]
    
    # Create the data payload to send in the PATCH request
    # We're updating the onboarding_phases field with the new list that includes phase 1
    payload = {'onboarding_phases': updated_onboarding}
    
    try:
        # Make a PATCH request to update the user
        # PATCH updates only the fields we specify in the payload
        # json=payload automatically converts the dictionary to JSON format
        response = requests.patch(update_url, json=payload, headers=API_HEADERS)
        
        # Check the HTTP status code to see if the update was successful
        if response.status_code == 200:
            # 200 = Success! The user was updated successfully
            logger.info(f"SUCCESS: Added phase 1 to user {user_id} ({email}) - updated onboarding_phases from {current_onboarding} to {updated_onboarding}")
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
    log_filename = f"add_user_onboarding_log_{timestamp}.log"
    # Define the directory where log files will be stored
    log_dir = "logs"
    
    # Check if the logs directory exists
    if not os.path.exists(log_dir):
        # If it doesn't exist, create it
        # makedirs() creates the directory and any parent directories needed
        os.makedirs(log_dir)
    
    # Combine the directory path and filename to create the full file path
    # Example: logs/add_user_onboarding_log_20251202_143207.log
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
    # __name__ is the module name (e.g., '__main__' or 'add_user_onboarding')
    logger = logging.getLogger(__name__)
    # Log that logging has been initialized
    logger.info(f"Logging initialized. Log file: {log_path}")
    
    # Return both the logger and the log file path
    return logger, log_path

def main():
    """
    Main function to add onboarding phase 1 to users based on tool 191 qualification.
    
    This function orchestrates the entire process:
    1. Sets up logging
    2. Tests API connection
    3. Downloads all users
    4. Checks each user's qualifications for tool 191 (Safety Tour Tool)
    5. Adds phase 1 to onboarding_phases for users who have tool 191 in their qualifications
       (preserves any existing phases)
    6. Provides a summary and saves results
    """
    # Set up logging and get the logger object and log file path
    logger, log_path = setup_logging()
    
    # Print a header banner for the script output
    print("=" * 60)  # Print 60 equals signs as a separator line
    print("NEMO Update User Onboarding Phases Script")
    print("=" * 60)
    print(f"Log file: {log_path}")  # Show where the log file is saved
    print()  # Print a blank line
    
    # Test the API connection before proceeding
    # If the connection fails, exit the script immediately
    if not test_api_connection(logger):
        print("Exiting due to API connection failure.")
        exit(1)  # Exit with error code 1
    
    print()  # Print a blank line
    
    # Download all users from the NEMO API
    # This returns a list of user dictionaries
    all_users = download_all_users(logger)
    
    # Check if we successfully downloaded any users
    if not all_users:
        # If the list is empty, we can't proceed
        print("No users found. Cannot proceed. Exiting.")
        logger.warning("No users found. Cannot proceed.")
        exit(1)  # Exit with error code 1
    
    print()  # Print a blank line
    
    # Users will have phase 1 added to their onboarding_phases if:
    # They have tool 191 (Safety Tour Tool) in their qualifications
    # (preserving any existing phases)
    print("Users will have phase 1 added to their onboarding_phases if:")
    print("  - They have tool 191 (Safety Tour Tool) qualification")
    print("(existing phases will be preserved)")
    print()  # Print a blank line
    
    # Initialize lists to track the results of processing users
    successful_updates = []  # Users we successfully updated
    failed_updates = []      # Users we tried to update but failed
    skipped_users = []       # Users we skipped (already updated, no date, etc.)
    
    # Get the total number of users to process
    total_users = len(all_users)
    print(f"Processing {total_users} lab member(s)...")
    print()  # Print a blank line
    
    # Loop through each user in the list
    # enumerate() gives us both the index (i) and the user object
    # Starting at 1 instead of 0 for more readable output (1st user, 2nd user, etc.)
    for i, user in enumerate(all_users, 1):
        # Extract user information from the user dictionary
        # .get() safely gets a value, returning None if the key doesn't exist
        user_id = user.get('id')                    # User's unique ID
        user_email = user.get('email', 'N/A')        # User's email (default 'N/A' if missing)
        user_username = user.get('username', 'N/A') # User's username (default 'N/A' if missing)
        # Get current onboarding_phases, default to empty list [] if not set
        current_onboarding = user.get('onboarding_phases', [])
        # Ensure it's a list (handle None or other types)
        if not isinstance(current_onboarding, list):
            current_onboarding = []
        
        # Check if user has tool 191 (Safety Tour Tool) in their qualifications
        # Qualifications is a list of qualification objects, each with a 'tool' field
        qualifications = user.get('qualifications', [])
        if not isinstance(qualifications, list):
            qualifications = []
        # Check if any qualification has tool ID 191
        has_safety_tour_tool = False
        for qual in qualifications:
            # Handle both dict format (qual['tool'] or qual['tool']['id']) and direct ID format
            if isinstance(qual, dict):
                tool_id = qual.get('tool')
                # If tool is a dict, get its ID
                if isinstance(tool_id, dict):
                    tool_id = tool_id.get('id')
                # If tool_id is an integer and equals 191
                if tool_id == 191:
                    has_safety_tour_tool = True
                    break
            elif isinstance(qual, int) and qual == 191:
                # If qualifications is a list of tool IDs directly
                has_safety_tour_tool = True
                break
        
        # Check if this user already has phase 1 in their onboarding_phases
        # If so, we don't need to update them (preserves existing phases like [1, 2] or [2, 1])
        if 1 in current_onboarding:
            # Add to skipped list with reason
            skipped_users.append({
                'user_id': user_id,
                'email': user_email,
                'username': user_username,
                'reason': f'Already has phase 1 in onboarding_phases: {current_onboarding}'
            })
            # Every 100 users, print a progress update
            # % is the modulo operator - checks if i is divisible by 100
            if i % 100 == 0:
                print(f"[{i}/{total_users}] Processed... (skipped: {len(skipped_users)}, updated: {len(successful_updates)}, failed: {len(failed_updates)})")
            continue  # Skip to the next user
        
        # Check if user has tool 191 (Safety Tour Tool) in qualifications
        # If not, skip this user
        if not has_safety_tour_tool:
            # Add to skipped list with reason
            skipped_users.append({
                'user_id': user_id,
                'email': user_email,
                'username': user_username,
                'reason': 'No tool 191 (Safety Tour Tool) qualification'
            })
            # Print progress every 100 users
            if i % 100 == 0:
                print(f"[{i}/{total_users}] Processed... (skipped: {len(skipped_users)}, updated: {len(successful_updates)}, failed: {len(failed_updates)})")
            continue  # Skip to the next user
        
        # If we reach here, user has tool 191 qualification, so we should add phase 1
        print(f"[{i}/{total_users}] Adding phase 1 to user {user_id} ({user_email}) - has tool 191 qualification - current phases: {current_onboarding}")
        # Call the function to update the user via API (adds phase 1, preserves existing phases)
        success = update_user_onboarding(user_id, user_email, user_username, current_onboarding, logger)
        
        # Check if the update was successful
        if success:
            # Add to successful updates list with user details
            update_record = {
                'user_id': user_id,
                'email': user_email,
                'username': user_username,
                'has_tool_191': has_safety_tour_tool,  # Track if user has Safety Tour Tool qualification
                'update_reason': 'has tool 191 (Safety Tour Tool) qualification'  # Why phase 1 was added
            }
            successful_updates.append(update_record)
            print(f"  ✓ Updated successfully")
        else:
            # Add to failed updates list with reason
            failed_updates.append({
                'user_id': user_id,
                'email': user_email,
                'username': user_username,
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
    print(f"Total lab members processed: {total_users}")
    print(f"Lab members successfully updated: {len(successful_updates)}")
    print(f"Lab members skipped: {len(skipped_users)}")
    print(f"Lab members with failed updates: {len(failed_updates)}")
    print()  # Print a blank line
    # Highlight the number of lab members updated
    if successful_updates:
        print(f"✓ {len(successful_updates)} lab member(s) had phase 1 added to their onboarding phases")
    print()  # Print a blank line
    
    # Save the list of successfully updated users to a JSON file
    if successful_updates:
        # Create a timestamp for the filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Create filename with timestamp
        output_file = f"updated_user_onboarding_{timestamp}.json"
        # Open the file for writing (creates file if it doesn't exist)
        with open(output_file, 'w', encoding='utf-8') as f:
            # Write the list as JSON with nice formatting (indent=2 makes it readable)
            json.dump(successful_updates, f, indent=2)
        print(f"✓ Successfully updated lab members saved to: {output_file}")
        logger.info(f"Successfully updated lab members ({len(successful_updates)} total) saved to: {output_file}")
    
    # Log details about any failed updates
    if failed_updates:
        # Log the total count of failures
        logger.warning(f"Failed to update {len(failed_updates)} lab member(s)")
        # Log each failed update with its reason
        for failed in failed_updates:
            logger.warning(f"Failed: Lab member {failed['user_id']} ({failed['email']}) - {failed.get('reason', 'Unknown error')}")
    
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

