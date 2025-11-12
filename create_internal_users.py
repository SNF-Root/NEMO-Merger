#!/usr/bin/env python3
"""
Script to create internal users in NEMO from SNSF Internal User Tracking Excel file.
"""

import os
import json
import pandas as pd
import requests
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional, Set
from datetime import datetime
import re

# Load environment variables
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

def load_pta_lookup(filename: str = "pta_lookup.json") -> Dict[str, int]:
    """Load PTA to project ID lookup from JSON file."""
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                lookup = json.load(f)
                print(f"✓ Loaded PTA lookup with {len(lookup)} entries")
                return lookup
        else:
            print(f"⚠ Warning: {filename} not found. Proceeding without PTA lookup.")
            return {}
    except Exception as e:
        print(f"⚠ Warning: Error loading PTA lookup: {e}")
        return {}

def load_existing_usernames(filename: str = "existing_usernames.json") -> Set[str]:
    """Load existing usernames from a JSON file."""
    try:
        with open(filename, 'r') as f:
            usernames_list = json.load(f)
        existing_usernames = set(username.lower() for username in usernames_list)
        print(f"✓ Loaded {len(existing_usernames)} existing usernames from {filename}")
        return existing_usernames
    except FileNotFoundError:
        print(f"⚠ Existing usernames file {filename} not found!")
        print("Please run download_users.py first to download users from NEMO.")
        return set()
    except Exception as e:
        print(f"✗ Error loading existing usernames: {e}")
        return set()

def download_existing_usernames() -> Set[str]:
    """Download all existing users from NEMO API and return a set of usernames."""
    try:
        print("Downloading existing users from NEMO API...")
        response = requests.get(NEMO_USERS_API_URL, headers=API_HEADERS)
        
        if response.status_code == 200:
            users = response.json()
            # Extract usernames from users
            existing_usernames = set()
            for user in users:
                if 'username' in user and user['username']:
                    username = str(user['username']).strip().lower()
                    if username and username.lower() != 'none' and username.lower() != 'null':
                        existing_usernames.add(username)
            
            print(f"✓ Successfully downloaded {len(users)} users")
            print(f"✓ Found {len(existing_usernames)} unique usernames in existing users")
            return existing_usernames
        else:
            print(f"✗ Failed to download users: HTTP {response.status_code} - {response.text}")
            return set()
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error downloading users: {e}")
        return set()
    except json.JSONDecodeError as e:
        print(f"✗ Error parsing JSON response: {e}")
        return set()
    except Exception as e:
        print(f"✗ Error processing users: {e}")
        return set()

def filter_existing_users(users: List[Dict[str, Any]], existing_usernames: Set[str]) -> List[Dict[str, Any]]:
    """Filter out users that already exist in NEMO based on username comparison."""
    new_users = []
    duplicate_users = []
    
    for user in users:
        username = user.get('username', '').strip().lower()
        
        if username in existing_usernames:
            duplicate_users.append(user)
        else:
            new_users.append(user)
    
    if duplicate_users:
        print(f"⚠ Filtered out {len(duplicate_users)} duplicate users (already exist in NEMO):")
        for dup in duplicate_users[:10]:  # Show first 10
            print(f"  - {dup.get('username', 'N/A')} ({dup.get('email', 'N/A')})")
        if len(duplicate_users) > 10:
            print(f"  ... and {len(duplicate_users) - 10} more duplicates")
    
    return new_users

def extract_email_from_university_id_email(value: str) -> Optional[str]:
    """Extract email address from 'University ID Email' column which may contain both ID and email."""
    if pd.isna(value):
        return None
    
    value_str = str(value).strip()
    # Look for email pattern
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    match = re.search(email_pattern, value_str)
    if match:
        return match.group(0)
    return None

def extract_ptas_from_other_ptas(value: str) -> List[str]:
    """Extract individual PTAs from 'Other PTAs' column which may contain concatenated PTAs.
    
    Handles multiple formats:
    - Newline-separated: "1168003-1-DJYBX\n1277206-10-UBILB"
    - Space-separated: "1168003-1-DJYBX 1277206-10-UBILB"
    - Comma-separated: "1168003-1-DJYBX,1277206-10-UBILB"
    - Concatenated: "1168003-1-DJYBX1277206-10-UBILB"
    """
    if pd.isna(value):
        return []
    
    value_str = str(value).strip()
    if not value_str or value_str.lower() == 'nan' or value_str.lower() == 'none':
        return []
    
    # PTA pattern: numbers-hyphen-numbers-hyphen-letters (e.g., "1262004-000-DDLOT")
    # This pattern will match PTAs regardless of how they're separated
    pta_pattern = r'\d+-\d+-[A-Z]+'
    ptas = re.findall(pta_pattern, value_str)
    
    # Clean up and deduplicate
    cleaned_ptas = []
    seen = set()
    for pta in ptas:
        pta_clean = pta.strip().upper()
        if pta_clean and pta_clean not in seen:
            cleaned_ptas.append(pta_clean)
            seen.add(pta_clean)
    
    return cleaned_ptas

def get_project_ids_from_ptas(ptas: List[str], pta_lookup: Dict[str, int]) -> List[int]:
    """Map PTAs to project IDs using the lookup table."""
    project_ids = []
    for pta in ptas:
        if pta in pta_lookup:
            project_id = pta_lookup[pta]
            if project_id not in project_ids:
                project_ids.append(project_id)
    return project_ids

def read_internal_users(pta_lookup: Dict[str, int]) -> List[Dict[str, Any]]:
    """Read internal users from Internal User Tracking and Emails.xlsx file."""
    users = []
    file_path = "SNSF-Data/Internal User Tracking and Emails.xlsx"
    
    try:
        if not os.path.exists(file_path):
            print(f"✗ Error: File not found: {file_path}")
            return []
        
        print(f"Reading {file_path}...")
        df = pd.read_excel(file_path)
        
        print(f"Found {len(df)} rows")
        print(f"Columns: {df.columns.tolist()}")
        
        # Map column names (handle variations in column names)
        # Based on image description, we need: Status, First, Last, University ID Email, SUNet ID, PTA, Other PTAs, Notes
        status_col = None
        first_col = None
        last_col = None
        email_col = None
        sunet_col = None
        pta_col = None
        other_ptas_col = None
        notes_col = None
        
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if 'status' in col_lower and not status_col:
                status_col = col
            elif ('first' in col_lower or col_lower.startswith('first')) and not first_col:
                first_col = col
            elif ('last' in col_lower or col_lower.startswith('last')) and not last_col:
                last_col = col
            elif 'university id email' in col_lower and not email_col:
                # Combined column with both ID and email
                email_col = col
            elif col_lower == 'email' and not email_col:
                # Standalone email column
                email_col = col
            elif 'sunet' in col_lower and not sunet_col:
                sunet_col = col
            elif (col_lower == 'pta' or col_lower.startswith('pta')) and 'other' not in col_lower and not pta_col:
                pta_col = col
            elif 'other pta' in col_lower and not other_ptas_col:
                other_ptas_col = col
            elif 'notes' in col_lower and not notes_col:
                notes_col = col
        
        # Verify required columns exist
        required_cols = [status_col, first_col, last_col, email_col]
        if not all(required_cols):
            print(f"✗ Error: Missing required columns")
            print(f"  Status: {status_col}")
            print(f"  First: {first_col}")
            print(f"  Last: {last_col}")
            print(f"  Email: {email_col}")
            return []
        
        print(f"Using columns:")
        print(f"  Status: {status_col}")
        print(f"  First: {first_col}")
        print(f"  Last: {last_col}")
        print(f"  Email: {email_col}")
        print(f"  SUNet ID: {sunet_col}")
        print(f"  PTA: {pta_col}")
        print(f"  Other PTAs: {other_ptas_col}")
        print(f"  Notes: {notes_col}")
        
        for idx, row in df.iterrows():
            # Skip rows with missing required data
            if pd.isna(row[first_col]) or pd.isna(row[last_col]):
                continue
            
            # Extract email
            email = None
            if email_col:
                email_value = row[email_col]
                # Check if this is a combined "University ID Email" column or standalone "Email" column
                if 'university id' in str(email_col).lower():
                    # Combined column - extract email using regex
                    email = extract_email_from_university_id_email(email_value)
                else:
                    # Standalone email column - use directly
                    if not pd.isna(email_value):
                        email = str(email_value).strip()
                        if not email or email.lower() in ['nan', 'none', '']:
                            email = None
            
            # If no email found, try to construct from SUNet ID
            if not email and sunet_col and not pd.isna(row[sunet_col]):
                sunet_id = str(row[sunet_col]).strip()
                if sunet_id:
                    email = f"{sunet_id}@stanford.edu"
            
            if not email or '@' not in email:
                continue
            
            username = email.split('@')[0].lower()
            
            # Check status - if "Inactivated", set is_active to False
            is_active = True
            if status_col and not pd.isna(row[status_col]):
                status = str(row[status_col]).strip()
                if status.lower() == "inactivated":
                    is_active = False
            
            # Extract PTAs
            ptas = []
            if pta_col and not pd.isna(row[pta_col]):
                pta = str(row[pta_col]).strip()
                if pta and pta.upper() not in ['NAN', 'NONE', '']:
                    ptas.append(pta.upper())
            
            if other_ptas_col and not pd.isna(row[other_ptas_col]):
                other_ptas = extract_ptas_from_other_ptas(row[other_ptas_col])
                if other_ptas:
                    print(f"  Found {len(other_ptas)} PTAs in 'Other PTAs' for {username}: {', '.join(other_ptas)}")
                ptas.extend(other_ptas)
            
            # Deduplicate PTAs
            ptas = list(dict.fromkeys(ptas))  # Preserves order while removing duplicates
            
            # Map PTAs to project IDs
            project_ids = get_project_ids_from_ptas(ptas, pta_lookup)
            
            # Log PTA mapping results
            if ptas:
                found_ptas = [pta for pta in ptas if pta in pta_lookup]
                missing_ptas = [pta for pta in ptas if pta not in pta_lookup]
                if found_ptas:
                    print(f"  User {username}: Mapped {len(found_ptas)} PTAs to {len(project_ids)} project(s)")
                if missing_ptas:
                    print(f"  ⚠ Warning: {len(missing_ptas)} PTA(s) not found in lookup: {', '.join(missing_ptas)}")
            
            # Extract notes from Notes column
            notes = None
            if notes_col and not pd.isna(row[notes_col]):
                notes_value = str(row[notes_col]).strip()
                if notes_value and notes_value.lower() != 'nan' and notes_value.lower() != 'none':
                    notes = notes_value
            
            user = {
                "username": username,
                "first_name": str(row[first_col]).strip(),
                "last_name": str(row[last_col]).strip(),
                "email": email,
                "is_active": is_active,
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
                "notes": notes,
                "badge_number": None,
                "access_expiration": None,
                "onboarding_phases": [],
                "safety_trainings": [],
                "groups": [],
                "user_permissions": [],
                "qualifications": [],
                "projects": project_ids,  # Add project IDs from PTA lookup
                "managed_projects": [],
                "gender_name": None,
                "race_name": None,
                "ethnicity_name": None,
                "education_level_name": None
            }
            users.append(user)
            
            if project_ids:
                print(f"  User {username}: Found {len(project_ids)} project(s) from {len(ptas)} PTA(s)")
                    
    except Exception as e:
        print(f"✗ Error reading Excel file: {e}")
        import traceback
        traceback.print_exc()
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
    """Main function to read and create internal users."""
    print("Starting internal user creation process...")
    print("-" * 60)
    
    # Load PTA lookup
    print("\nLoading PTA lookup...")
    pta_lookup = load_pta_lookup()
    
    # Read internal users from Excel file
    print("\nReading internal users from Excel file...")
    users = read_internal_users(pta_lookup)
    
    if not users:
        print("✗ No internal users found in Excel file")
        return
    
    active_count = sum(1 for u in users if u['is_active'])
    inactive_count = len(users) - active_count
    print(f"\n✓ Found {len(users)} internal users")
    print(f"  Active: {active_count}")
    print(f"  Inactive: {inactive_count}")
    
    # Load existing usernames from file (or download if file doesn't exist)
    print("\nLoading existing usernames...")
    existing_usernames = load_existing_usernames()
    
    if not existing_usernames:
        print("⚠ Warning: Could not load existing usernames from file. Attempting to download from API...")
        existing_usernames = download_existing_usernames()
        
        if not existing_usernames:
            print("⚠ Warning: Could not download existing users or no users found. Proceeding without duplicate check.")
    
    print("\nFiltering out duplicate users...")
    filtered_users = filter_existing_users(users, existing_usernames)
    
    if not filtered_users:
        print("No new users to create! All users already exist in NEMO.")
        return
    
    print(f"\n✓ {len(filtered_users)} new users to create (filtered out {len(users) - len(filtered_users)} duplicates)")
    
    # Create users
    create_users(filtered_users)

if __name__ == "__main__":
    main()

