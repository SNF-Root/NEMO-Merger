#!/usr/bin/env python3
"""
Script to add University ID to user notes field in NEMO.
Downloads users from API, reads CSV file, and updates user notes with ID:{university ID}.
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

# NEMO API endpoint for users
NEMO_USERS_API_URL = "https://nemo.stanford.edu/api/users/"

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

def test_api_connection(logger: logging.Logger) -> bool:
    """Test the API connection and authentication."""
    try:
        response = requests.get(NEMO_USERS_API_URL, headers=API_HEADERS)
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

def download_all_users(logger: logging.Logger) -> List[Dict[str, Any]]:
    """Download all users from the NEMO API."""
    print("Downloading users from NEMO API...")
    logger.info("Downloading users from NEMO API")
    
    all_users = []
    page = 1
    
    while True:
        try:
            # Add pagination parameters
            params = {'page': page}
            response = requests.get(NEMO_USERS_API_URL, headers=API_HEADERS, params=params)
            
            if response.status_code == 200:
                response_data = response.json()
                
                # Check if this is a paginated response
                if 'results' in response_data:
                    users = response_data['results']
                    print(f"  Page {page}: Retrieved {len(users)} users")
                else:
                    # Direct list response
                    users = response_data
                    print(f"  Retrieved {len(users)} users (no pagination)")
                
                if not users:
                    break
                
                all_users.extend(users)
                
                # Check if there are more pages
                if 'next' in response_data and response_data['next']:
                    page += 1
                else:
                    break
                    
            elif response.status_code == 401:
                print("✗ Authentication failed: Check your NEMO_TOKEN")
                logger.error("Authentication failed while downloading users")
                return []
            elif response.status_code == 403:
                print("✗ Permission denied: Check your API permissions")
                logger.error("Permission denied while downloading users")
                return []
            else:
                print(f"✗ Failed to download users: HTTP {response.status_code} - {response.text}")
                logger.error(f"Failed to download users: HTTP {response.status_code}")
                return []
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Network error downloading users: {e}")
            logger.error(f"Network error downloading users: {e}", exc_info=True)
            return []
        except json.JSONDecodeError as e:
            print(f"✗ Error parsing JSON response: {e}")
            logger.error(f"Error parsing JSON response: {e}")
            return []
    
    print(f"✓ Total users downloaded: {len(all_users)}")
    logger.info(f"Total users downloaded: {len(all_users)}")
    return all_users

def create_email_lookup(users: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Create a lookup dictionary mapping email addresses to user data."""
    lookup = {}
    
    for user in users:
        email = user.get('email', '').strip().lower()
        if email:
            lookup[email] = user
    
    return lookup

def create_username_lookup(users: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Create a lookup dictionary mapping usernames to user data."""
    lookup = {}
    
    for user in users:
        username = user.get('username', '').strip().lower()
        if username:
            lookup[username] = user
    
    return lookup

def read_csv_university_ids(file_path: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Read University IDs from CSV file and create email -> university ID and username -> university ID mappings.
    
    Returns:
        Tuple of (email_to_university_id, username_to_university_id) dictionaries
    """
    email_to_university_id = {}
    username_to_university_id = {}
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                email = row.get('Email', '').strip().lower()
                username = row.get('SUNet ID (mult. Acct. highlighted)', '').strip().lower()
                university_id = row.get('Card Number', '').strip()
                
                if email and university_id:
                    email_to_university_id[email] = university_id
                if username and university_id:
                    username_to_university_id[username] = university_id
        
        print(f"Found {len(email_to_university_id)} email-to-university-ID mappings in {file_path}")
        print(f"Found {len(username_to_university_id)} username-to-university-ID mappings in {file_path}")
        return email_to_university_id, username_to_university_id
    except FileNotFoundError:
        print(f"Error: File {file_path} not found")
        return {}, {}
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return {}, {}

def update_user_notes(user_id: int, university_id: str, email: str, current_notes: str, logger: logging.Logger) -> bool:
    """Update a user's notes field via PATCH request.
    
    Adds ID:{university_id} to the notes field.
    """
    update_url = f"{NEMO_USERS_API_URL}{user_id}/"
    
    # Prepare the notes update
    # If notes already exist, append the ID; otherwise set it
    if current_notes and current_notes.strip():
        # Check if ID already exists in notes
        if f"ID:{university_id}" in current_notes:
            logger.info(f"SKIP: User {user_id} ({email}) already has ID:{university_id} in notes")
            return True
        # Append to existing notes
        new_notes = f"{current_notes}\nID:{university_id}"
    else:
        new_notes = f"ID:{university_id}"
    
    # Use PATCH to update only the notes field
    payload = {'notes': new_notes}
    
    try:
        response = requests.patch(update_url, json=payload, headers=API_HEADERS)
        
        if response.status_code == 200:
            logger.info(f"SUCCESS: Updated user {user_id} ({email}) with ID:{university_id}")
            return True
        elif response.status_code == 400:
            error_msg = response.text
            logger.error(f"FAILED: Bad request for user {user_id} ({email}) - {error_msg}")
            logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
            return False
        elif response.status_code == 401:
            logger.error(f"FAILED: Authentication failed for user {user_id} ({email})")
            return False
        elif response.status_code == 403:
            logger.error(f"FAILED: Permission denied for user {user_id} ({email})")
            return False
        elif response.status_code == 404:
            logger.error(f"FAILED: User {user_id} ({email}) not found")
            return False
        else:
            error_msg = response.text
            logger.error(f"FAILED: HTTP {response.status_code} for user {user_id} ({email}) - {error_msg}")
            return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"FAILED: Network error for user {user_id} ({email}) - {str(e)}")
        return False
    except Exception as e:
        logger.error(f"FAILED: Unexpected error for user {user_id} ({email}) - {str(e)}", exc_info=True)
        return False

def setup_logging() -> Tuple[logging.Logger, str]:
    """Set up logging to file with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"add_user_id_log_{timestamp}.log"
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
    """Main function to add University IDs to user notes."""
    logger, log_path = setup_logging()
    
    print("=" * 60)
    print("NEMO Add University ID to User Notes Script")
    print("=" * 60)
    print(f"Log file: {log_path}")
    print()
    
    # Test API connection
    if not test_api_connection(logger):
        print("Exiting due to API connection failure.")
        exit(1)
    
    print()
    
    # Download users
    users = download_all_users(logger)
    
    if not users:
        print("No users found. Cannot proceed. Exiting.")
        logger.warning("No users found. Cannot proceed.")
        exit(1)
    
    print()
    print("Creating email lookup table...")
    email_lookup = create_email_lookup(users)
    print(f"✓ Created email lookup table with {len(email_lookup)} users")
    logger.info(f"Created email lookup table with {len(email_lookup)} users")
    
    print()
    print("Creating username lookup table...")
    username_lookup = create_username_lookup(users)
    print(f"✓ Created username lookup table with {len(username_lookup)} users")
    logger.info(f"Created username lookup table with {len(username_lookup)} users")
    
    print()
    
    # Read CSV file
    csv_file_path = "SNSF-Data/Copy-external-users.csv"
    email_to_university_id, username_to_university_id = read_csv_university_ids(csv_file_path)
    
    if not email_to_university_id and not username_to_university_id:
        print("No university IDs found in CSV. Exiting.")
        logger.warning("No university IDs found in CSV")
        exit(1)
    
    print()
    # Create email -> username mapping from CSV for fallback
    email_to_username_map = {}
    with open(csv_file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = row.get('Email', '').strip().lower()
            username = row.get('SUNet ID (mult. Acct. highlighted)', '').strip().lower()
            if email and username:
                email_to_username_map[email] = username
    
    total_mappings = len(email_to_university_id)
    print(f"Processing {total_mappings} mappings (email first, username fallback if needed)...")
    print()
    
    # Track results
    successful_updates = []
    failed_updates = []
    not_found_users = []
    processed_users = set()  # Track users we've already processed to avoid duplicates
    
    # Process email mappings with username fallback
    for i, (email, university_id) in enumerate(email_to_university_id.items(), 1):
        print(f"[{i}/{total_mappings}] Processing: {email} -> ID:{university_id}")
        
        # Look up user by email first
        user = email_lookup.get(email)
        matched_by = 'email'
        
        # If not found by email, try username fallback
        if not user:
            username = email_to_username_map.get(email)
            if username:
                print(f"  ⚠ User with email '{email}' not found, trying username '{username}'...")
                user = username_lookup.get(username)
                matched_by = 'username_fallback'
            else:
                print(f"  ⚠ User with email '{email}' not found and no username available")
                logger.warning(f"User with email '{email}' not found in NEMO")
                not_found_users.append(email)
                failed_updates.append({
                    'email': email,
                    'username': '',
                    'university_id': university_id,
                    'reason': 'User not found (email and no username)'
                })
                continue
        
        if not user:
            username = email_to_username_map.get(email, '')
            print(f"  ⚠ User not found (email: {email}, username: {username if username else 'N/A'})")
            logger.warning(f"User not found: email={email}, username={username if username else 'N/A'}")
            not_found_users.append(f"{email} (username: {username if username else 'N/A'})")
            failed_updates.append({
                'email': email,
                'username': username,
                'university_id': university_id,
                'reason': 'User not found'
            })
            continue
        
        user_id = user.get('id')
        if user_id in processed_users:
            print(f"  ⚠ User ID {user_id} already processed, skipping...")
            continue
        
        processed_users.add(user_id)
        current_notes = user.get('notes', '')
        user_email = user.get('email', email)
        user_username = user.get('username', email_to_username_map.get(email, ''))
        
        # Check if ID already exists in notes before making API call
        if current_notes and f"ID:{university_id}" in current_notes:
            print(f"  → Found user ID: {user_id} (matched by {matched_by})")
            print(f"  ⚠ User already has ID:{university_id} in notes, skipping...")
            logger.info(f"SKIP: User {user_id} ({user_email}) already has ID:{university_id} in notes")
            successful_updates.append({
                'email': user_email,
                'username': user_username,
                'user_id': user_id,
                'university_id': university_id,
                'matched_by': matched_by,
                'status': 'already_exists'
            })
            continue
        
        print(f"  → Found user ID: {user_id} (matched by {matched_by})")
        
        success = update_user_notes(user_id, university_id, user_email, current_notes, logger)
        
        if success:
            successful_updates.append({
                'email': user_email,
                'username': user_username,
                'user_id': user_id,
                'university_id': university_id,
                'matched_by': matched_by
            })
            print(f"  ✓ Updated successfully")
        else:
            failed_updates.append({
                'email': user_email,
                'username': user_username,
                'user_id': user_id,
                'university_id': university_id,
                'reason': 'API update failed',
                'matched_by': matched_by
            })
            print(f"  ✗ Update failed")
        
        # Small delay to avoid overwhelming the API
        time.sleep(0.5)
    
    # Summary
    print()
    print("=" * 60)
    print("Update Summary")
    print("=" * 60)
    print(f"Total mappings: {len(email_to_university_id)}")
    
    # Count different types of successful updates
    new_updates = [u for u in successful_updates if u.get('status') != 'already_exists']
    already_existing = [u for u in successful_updates if u.get('status') == 'already_exists']
    
    print(f"New updates: {len(new_updates)}")
    print(f"Already had ID (skipped): {len(already_existing)}")
    print(f"Total successful: {len(successful_updates)}")
    print(f"Failed updates: {len(failed_updates)}")
    print(f"Users not found: {len(not_found_users)}")
    print()
    
    if not_found_users:
        print("Users not found in NEMO:")
        for email in not_found_users[:10]:  # Show first 10
            print(f"  - {email}")
        if len(not_found_users) > 10:
            print(f"  ... and {len(not_found_users) - 10} more")
        print()
        logger.warning(f"Users not found: {not_found_users}")
    
    # Save successful updates to JSON file
    if successful_updates:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"updated_user_ids_{timestamp}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(successful_updates, f, indent=2)
        print(f"✓ Successfully updated users saved to: {output_file}")
        logger.info(f"Successfully updated users saved to: {output_file}")
    
    # Log failed updates
    if failed_updates:
        logger.warning(f"Failed to update {len(failed_updates)} users")
        for failed in failed_updates:
            logger.warning(f"Failed: {failed['email']} - {failed.get('reason', 'Unknown error')}")
    
    print()
    print(f"Detailed log saved to: {log_path}")
    print("=" * 60)

if __name__ == "__main__":
    main()

