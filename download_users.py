#!/usr/bin/env python3
"""
Script to download all users from NEMO API and save them locally.
This is needed before creating users to check for duplicates by username or email.
"""

import requests
import json
import os
import csv
from dotenv import load_dotenv
from typing import List, Dict, Any, Set

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

# API headers with authentication
API_HEADERS = {
    'Authorization': f'Token {NEMO_TOKEN}',
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

def test_api_connection():
    """Test the API connection and authentication."""
    try:
        response = requests.get(NEMO_USERS_API_URL, headers=API_HEADERS)
        if response.status_code == 200:
            print("✓ API connection successful")
            return True
        elif response.status_code == 401:
            print("✗ Authentication failed: Check your NEMO_TOKEN")
            return False
        elif response.status_code == 403:
            print("✗ Permission denied: Check your API permissions")
            return False
        else:
            print(f"✗ API connection failed: HTTP {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error connecting to API: {e}")
        return False

def download_users() -> List[Dict[str, Any]]:
    """Download all users from the NEMO API."""
    print("Downloading users from NEMO API...")
    
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
                return []
            elif response.status_code == 403:
                print("✗ Permission denied: Check your API permissions")
                return []
            else:
                print(f"✗ Failed to download users: HTTP {response.status_code} - {response.text}")
                return []
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Network error downloading users: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"✗ Error parsing JSON response: {e}")
            return []
    
    print(f"✓ Successfully downloaded {len(all_users)} users")
    return all_users

def save_users_to_file(users: List[Dict[str, Any]], filename: str = "nemo_users.json"):
    """Save users to a local JSON file."""
    try:
        with open(filename, 'w') as f:
            json.dump(users, f, indent=2)
        print(f"✓ Successfully saved {len(users)} users to {filename}")
    except Exception as e:
        print(f"✗ Error saving users to file: {e}")

def save_users_to_csv(users: List[Dict[str, Any]], filename: str = "nemo_users.csv"):
    """Save users to a CSV file, sorted by ID in ascending order."""
    if not users:
        print("No users to save to CSV")
        return
    
    try:
        # Sort users by ID in ascending order
        sorted_users = sorted(users, key=lambda x: x.get('id', 0))
        
        # Get all unique keys from all users to create comprehensive headers
        all_keys = set()
        for user in sorted_users:
            all_keys.update(user.keys())
        
        # Define column order (ID first, then username, email, then others alphabetically)
        priority_keys = ['id', 'username', 'email', 'first_name', 'last_name', 'is_active']
        fieldnames = [k for k in priority_keys if k in all_keys]
        fieldnames.extend(sorted([k for k in all_keys if k not in priority_keys]))
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for user in sorted_users:
                # Convert None values to empty strings for CSV
                row = {k: ('' if v is None else v) for k, v in user.items()}
                writer.writerow(row)
        
        print(f"✓ Successfully saved {len(sorted_users)} users to {filename} (sorted by ID)")
    except Exception as e:
        print(f"✗ Error saving users to CSV: {e}")

def create_username_lookup(users: List[Dict[str, Any]]) -> Dict[str, int]:
    """Create a lookup dictionary mapping usernames to user IDs."""
    lookup = {}
    for user in users:
        if 'username' in user and user['username'] and 'id' in user:
            username = str(user['username']).strip().lower()
            if username and username.lower() != 'none' and username.lower() != 'null':
                lookup[username] = user['id']
    
    print(f"✓ Created username lookup for {len(lookup)} users")
    return lookup

def create_email_lookup(users: List[Dict[str, Any]]) -> Dict[str, int]:
    """Create a lookup dictionary mapping email addresses to user IDs."""
    lookup = {}
    for user in users:
        if 'email' in user and user['email'] and 'id' in user:
            email = str(user['email']).strip().lower()
            if email and email.lower() != 'none' and email.lower() != 'null' and '@' in email:
                lookup[email] = user['id']
    
    print(f"✓ Created email lookup for {len(lookup)} users")
    return lookup

def get_existing_usernames(users: List[Dict[str, Any]]) -> Set[str]:
    """Extract all existing usernames as a set (lowercase)."""
    usernames = set()
    for user in users:
        if 'username' in user and user['username']:
            username = str(user['username']).strip().lower()
            if username and username.lower() != 'none' and username.lower() != 'null':
                usernames.add(username)
    
    print(f"✓ Found {len(usernames)} unique usernames in existing users")
    return usernames

def get_existing_emails(users: List[Dict[str, Any]]) -> Set[str]:
    """Extract all existing email addresses as a set (lowercase)."""
    emails = set()
    for user in users:
        if 'email' in user and user['email']:
            email = str(user['email']).strip().lower()
            if email and email.lower() != 'none' and email.lower() != 'null' and '@' in email:
                emails.add(email)
    
    print(f"✓ Found {len(emails)} unique email addresses in existing users")
    return emails

def main():
    """Main function to download and save users."""
    print("Starting user download from NEMO API...")
    print(f"API Endpoint: {NEMO_USERS_API_URL}")
    print("-" * 60)
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Download users
    users = download_users()
    
    if not users:
        print("No users downloaded. Cannot proceed.")
        return
    
    # Save users to file
    save_users_to_file(users)
    
    # Save users to CSV (sorted by ID)
    save_users_to_csv(users)
    
    # Create and save username lookup
    username_lookup = create_username_lookup(users)
    
    # Save username lookup to a separate file for easy access
    with open("username_lookup.json", 'w') as f:
        json.dump(username_lookup, f, indent=2)
    print("✓ Saved username lookup to username_lookup.json")
    
    # Create and save email lookup
    email_lookup = create_email_lookup(users)
    
    # Save email lookup to a separate file
    with open("email_lookup.json", 'w') as f:
        json.dump(email_lookup, f, indent=2)
    print("✓ Saved email lookup to email_lookup.json")
    
    # Create and save existing usernames set
    existing_usernames = get_existing_usernames(users)
    
    # Save existing usernames as a list (JSON doesn't support sets)
    with open("existing_usernames.json", 'w') as f:
        json.dump(sorted(list(existing_usernames)), f, indent=2)
    print("✓ Saved existing usernames list to existing_usernames.json")
    
    # Create and save existing emails set
    existing_emails = get_existing_emails(users)
    
    # Save existing emails as a list (JSON doesn't support sets)
    with open("existing_emails.json", 'w') as f:
        json.dump(sorted(list(existing_emails)), f, indent=2)
    print("✓ Saved existing emails list to existing_emails.json")
    
    # Show sample of users
    print("\nSample users:")
    for i, user in enumerate(users[:5]):
        username = user.get('username', 'N/A')
        email = user.get('email', 'N/A')
        print(f"  {i+1}. ID: {user.get('id', 'N/A')}, Username: {username}, Email: {email}")
    
    if len(users) > 5:
        print(f"  ... and {len(users) - 5} more users")
    
    print(f"\n✓ User download complete! {len(users)} users saved locally.")
    print(f"✓ Found {len(existing_usernames)} unique usernames in existing users.")
    print(f"✓ Found {len(existing_emails)} unique email addresses in existing users.")
    print("You can now run create_internal_users.py or create_external_users.py to create new users (duplicates will be filtered out).")

if __name__ == "__main__":
    main()

