#!/usr/bin/env python3
"""
Script to add area access to users in NEMO by modifying the JSON file and uploading to API.
"""

import json
import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoint
NEMO_API_URL = "https://nemo-plan.stanford.edu/api/users/"

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

def add_area_to_users():
    """Add area ID 2 to users and upload to API."""
    try:
        # Read the existing JSON file
        with open('snf_user_download.json', 'r') as f:
            users = json.load(f)

        # Add area ID 2 to physical_access_levels for each user
        for user in users:
            if 'physical_access_levels' not in user:
                user['physical_access_levels'] = []
            if 2 not in user['physical_access_levels']:
                user['physical_access_levels'].append(2)

        # Upload each modified user to API
        successful = 0
        failed = 0

        for user in users:
            try:
                response = requests.put(f"{NEMO_API_URL}{user['id']}/", 
                                     json=user,
                                     headers=API_HEADERS)
                
                if response.status_code == 200:
                    print(f"✓ Successfully updated user {user['id']}")
                    successful += 1
                else:
                    print(f"✗ Failed to update user {user['id']}: {response.status_code} - {response.text}")
                    failed += 1
                    
            except requests.exceptions.RequestException as e:
                print(f"✗ Network error updating user {user['id']}: {e}")
                failed += 1

        # Save the modified JSON back to file
        with open('snf_user_download.json', 'w') as f:
            json.dump(users, f, indent=2)

        print("\nSummary:")
        print(f"Total users processed: {len(users)}")
        print(f"Successfully updated: {successful}")
        print(f"Failed to update: {failed}")

    except FileNotFoundError:
        print("Error: snf_user_download.json file not found")
        exit(1)
    except json.JSONDecodeError:
        print("Error: Invalid JSON in snf_user_download.json")
        exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        exit(1)

if __name__ == "__main__":
    add_area_to_users()
