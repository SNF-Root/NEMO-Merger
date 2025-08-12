#!/usr/bin/env python3
"""
Script to create users in NEMO from SNSF Excel files.
"""

import os
import json
import pandas as pd
import requests
from dotenv import load_dotenv
from typing import List, Dict, Any
from datetime import datetime

# Load environment variables
load_dotenv()

# NEMO API endpoint for users
NEMO_USERS_API_URL = "https://nemo-plan.stanford.edu/api/users/"

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

def read_qualified_users() -> List[Dict[str, Any]]:
    """Read qualified users from Excel files in SNSF-Data folder."""
    users = []
    data_dir = "SNSF-Data"
    
    try:
        for filename in os.listdir(data_dir):
            if filename.endswith(".xlsx") and "qualified users" in filename.lower():
                file_path = os.path.join(data_dir, filename)
                print(f"Reading {filename}...")
                
                df = pd.read_excel(file_path)
                
                # Verify required columns exist
                required_columns = ["member", "first name", "last name"]
                if not all(col in df.columns for col in required_columns):
                    print(f"Warning: Missing required columns in {filename}")
                    print(f"Available columns: {df.columns.tolist()}")
                    continue
                
                for _, row in df.iterrows():
                    # Skip rows with missing or invalid member
                    if pd.isna(row["member"]) or not isinstance(row["member"], str) or '@' not in str(row["member"]):
                        continue
                    
                    member = str(row["member"]).strip()
                    username = member.split('@')[0].lower()
                    
                    user = {
                        "username": username,
                        "first_name": row["first name"],
                        "last_name": row["last name"],
                        "email": member,
                        "is_active": True,
                        "is_staff": False,
                        "is_user_office": False,
                        "is_accounting_officer": False,
                        "is_service_personnel": False,
                        "is_technician": False,
                        "is_facility_manager": False,
                        "is_superuser": False,
                        "training_required": False,
                        "type": 1,  # Default user type
                        "date_joined": datetime.now().isoformat(),
                        "domain": "",
                        "notes": None,
                        "badge_number": None,
                        "access_expiration": None,
                        "onboarding_phases": [],
                        "safety_trainings": [],
                        "groups": [],
                        "user_permissions": [],
                        "qualifications": [],
                        "projects": [],
                        "managed_projects": [],
                        "gender_name": None,
                        "race_name": None,
                        "ethnicity_name": None,
                        "education_level_name": None
                    }
                    users.append(user)
                    
    except Exception as e:
        print(f"Error reading Excel files: {e}")
        return []
        
    return users

def create_users(users: List[Dict[str, Any]]) -> None:
    """Create users in NEMO via API."""
    if not test_api_connection():
        return
        
    print(f"\nCreating {len(users)} users in NEMO...")
    
    for user in users:
        try:
            response = requests.post(
                NEMO_USERS_API_URL,
                headers=API_HEADERS,
                json=user
            )
            
            if response.status_code == 200:
                print(f"✓ Created user: {user['username']}")
            else:
                print(f"✗ Failed to create user {user['username']}: {response.status_code}")
                print(f"Error: {response.text}")
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Network error creating user {user['username']}: {e}")

def main():
    """Main function to read and create users."""
    print("Starting user creation process...")
    users = read_qualified_users()
    
    if users:
        print(f"Found {len(users)} qualified users")
        create_users(users)
    else:
        print("No qualified users found in Excel files")

if __name__ == "__main__":
    main()
