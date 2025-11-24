#!/usr/bin/env python3
"""
Script to create users in NEMO from SNSF Excel files.
"""

import os
import json
import logging
import pandas as pd
import requests
from dotenv import load_dotenv
from typing import List, Dict, Any, Set
from datetime import datetime

# Load environment variables
load_dotenv()

# Set up logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = os.path.join(LOG_DIR, f"external_users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# NEMO API endpoint for users
NEMO_USERS_API_URL = "https://nemo.stanford.edu/api/users/"

# Get NEMO token from environment
NEMO_TOKEN = os.getenv('NEMO_TOKEN')
if not NEMO_TOKEN:
    logger.error("NEMO_TOKEN not found in environment variables or .env file")
    logger.error("Please create a .env file with: NEMO_TOKEN=your_token_here")
    logger.error("Or set the environment variable: export NEMO_TOKEN=your_token_here")
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

def load_pta_lookup(filename: str = "pta_lookup.json") -> Dict[str, int]:
    """Load PTA to project ID lookup from JSON file."""
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                lookup = json.load(f)
                logger.info(f"✓ Loaded PTA lookup with {len(lookup)} entries")
                return lookup
        else:
            logger.warning(f"⚠ Warning: {filename} not found. Proceeding without PTA lookup.")
            return {}
    except Exception as e:
        logger.warning(f"⚠ Warning: Error loading PTA lookup: {e}")
        return {}

def get_project_ids_from_ptas(ptas: List[str], pta_lookup: Dict[str, int]) -> List[int]:
    """Map PTAs to project IDs using the lookup table."""
    project_ids = []
    for pta in ptas:
        pta_upper = pta.strip().upper()
        if pta_upper in pta_lookup:
            project_id = pta_lookup[pta_upper]
            if project_id not in project_ids:
                project_ids.append(project_id)
    return project_ids

def load_existing_usernames(filename: str = "existing_usernames.json") -> Set[str]:
    """Load existing usernames from a JSON file."""
    try:
        with open(filename, 'r') as f:
            usernames_list = json.load(f)
        existing_usernames = set(username.lower() for username in usernames_list)
        logger.info(f"✓ Loaded {len(existing_usernames)} existing usernames from {filename}")
        return existing_usernames
    except FileNotFoundError:
        logger.warning(f"⚠ Existing usernames file {filename} not found!")
        logger.info("Please run download_users.py first to download users from NEMO.")
        return set()
    except Exception as e:
        logger.error(f"✗ Error loading existing usernames: {e}")
        return set()

def download_existing_usernames() -> Set[str]:
    """Download all existing users from NEMO API and return a set of usernames."""
    try:
        logger.info("Downloading existing users from NEMO API...")
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
            
            logger.info(f"✓ Successfully downloaded {len(users)} users")
            logger.info(f"✓ Found {len(existing_usernames)} unique usernames in existing users")
            return existing_usernames
        else:
            logger.error(f"✗ Failed to download users: HTTP {response.status_code} - {response.text}")
            return set()
            
    except requests.exceptions.RequestException as e:
        logger.error(f"✗ Network error downloading users: {e}")
        return set()
    except json.JSONDecodeError as e:
        logger.error(f"✗ Error parsing JSON response: {e}")
        return set()
    except Exception as e:
        logger.error(f"✗ Error processing users: {e}")
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
        logger.warning(f"⚠ Filtered out {len(duplicate_users)} duplicate users (already exist in NEMO):")
        for dup in duplicate_users[:10]:  # Show first 10
            logger.warning(f"  - {dup.get('username', 'N/A')} ({dup.get('email', 'N/A')})")
        if len(duplicate_users) > 10:
            logger.warning(f"  ... and {len(duplicate_users) - 10} more duplicates")
    
    return new_users

def map_account_type_to_user_type(account_type: str) -> int:
    """
    Map Excel account type to NEMO user type ID.
    
    Mapping rules:
    - 'Industry' or 'Industrial' -> Industry user type
    - 'Academic' -> Other Academic user type (as specified)
    
    Returns user type ID (defaults to 1 if unknown).
    
    Note: User type IDs may need to be verified against actual NEMO API.
    Consider downloading user types from NEMO API to get correct IDs.
    """
    if pd.isna(account_type):
        logger.debug("Account type is NaN, defaulting to user type 1")
        return 1
    
    account_type_str = str(account_type).strip()
    account_type_lower = account_type_str.lower()
    
    # Map account types to user type IDs
    # Excel "Academic" maps to NEMO "Other Academic" user type (Type 1)
    # Excel "Industry" maps to NEMO "Industry" user type (Type 2)
    account_type_mapping = {
        "industry": 2,      # Industry user type
        "industrial": 2,    # Industry user type (alternative spelling)
        "academic": 1,      # Other Academic user type (Type 1)
    }
    
    if account_type_lower in account_type_mapping:
        mapped_type = account_type_mapping[account_type_lower]
        logger.debug(f"Mapped account type '{account_type_str}' -> user type {mapped_type}")
        return mapped_type
    
    # Default to type 1 if unknown
    logger.warning(f"Unknown account type '{account_type_str}', defaulting to user type 1")
    return 1

def find_column(df: pd.DataFrame, possible_names: List[str]) -> str:
    """Find a column by trying multiple possible names (case-insensitive)."""
    for possible_name in possible_names:
        for col in df.columns:
            if str(col).strip().lower() == possible_name.lower():
                return col
    return None

def read_qualified_users(pta_lookup: Dict[str, int]) -> List[Dict[str, Any]]:
    """Read external users from specified Excel file."""
    users = []
    file_path = "/Users/adenton/Desktop/NEMO-Merger/SNSF-Data/Copy-external-users.xlsx"
    
    try:
        if not os.path.exists(file_path):
            logger.error(f"✗ Error: File not found: {file_path}")
            return []
        
        logger.info(f"Reading {file_path}...")
        df = pd.read_excel(file_path)
        logger.info(f"Found {len(df)} rows with columns: {df.columns.tolist()}")
        
        # Map column names (handle variations)
        first_col = find_column(df, ["first", "first name"])
        last_col = find_column(df, ["last", "last name"])
        email_col = find_column(df, ["email", "member", "account owner e-mail"])
        account_type_col = find_column(df, ["account typ", "account type", "type"])
        company_col = find_column(df, ["company"])
        sunet_col = find_column(df, ["sunet id", "sunet", "sunet id (mult. acct. highlighted)"])
        pta_col = find_column(df, ["pta"])
        activation_date_col = find_column(df, ["user activation date", "activation date", "date"])
        card_number_col = find_column(df, ["card number", "card", "badge"])
        notes_col = find_column(df, ["notes"])
        tracking_sheet_col = find_column(df, ["tracking sheet link", "tracking sheet", "tracking link"])
        status_col = find_column(df, ["status (tra)", "status", "status (ba)"])
                
                # Verify required columns exist
        if not first_col or not last_col or not email_col:
            logger.error(f"✗ Error: Missing required columns")
            logger.error(f"  First: {first_col}, Last: {last_col}, Email: {email_col}")
            logger.error(f"Available columns: {df.columns.tolist()}")
            return []
        
        logger.info(f"Using columns:")
        logger.info(f"  First: {first_col}, Last: {last_col}, Email: {email_col}")
        logger.info(f"  Account Type: {account_type_col}, Company: {company_col}")
        logger.info(f"  SUNet ID: {sunet_col}, PTA: {pta_col}")
        logger.info(f"  Activation Date: {activation_date_col}, Card Number: {card_number_col}")
        logger.info(f"  Tracking Sheet Link: {tracking_sheet_col}")
        
        for idx, row in df.iterrows():
            # Skip rows with missing required data
            if pd.isna(row[first_col]) or pd.isna(row[last_col]):
                    continue
                
            # Get email
            email = None
            if email_col and not pd.isna(row[email_col]):
                email = str(row[email_col]).strip()
                if not email or '@' not in email or email.lower() in ['nan', 'none', '']:
                    email = None
            
            # If no email, try to construct from SUNet ID
            if not email and sunet_col and not pd.isna(row[sunet_col]):
                sunet_id = str(row[sunet_col]).strip()
                if sunet_id and sunet_id.lower() not in ['nan', 'none', '']:
                    email = f"{sunet_id}@stanford.edu"
            
            if not email or '@' not in email:
                logger.debug(f"Skipping row {idx+1}: No valid email found")
                continue
            
            # Determine username (prefer SUNet ID, otherwise use email prefix)
            username = None
            if sunet_col and not pd.isna(row[sunet_col]):
                sunet_id = str(row[sunet_col]).strip()
                if sunet_id and sunet_id.lower() not in ['nan', 'none', '']:
                    username = sunet_id.lower()
            
            if not username:
                username = email.split('@')[0].lower()
            
            # Get account type and map to user type
            user_type = 1  # Default
            account_type_value = None
            if account_type_col and not pd.isna(row[account_type_col]):
                account_type_value = str(row[account_type_col]).strip()
                user_type = map_account_type_to_user_type(account_type_value)
                logger.debug(f"User {username}: Account type '{account_type_value}' mapped to user type {user_type}")
            
            # Get activation date
            date_joined = datetime.now().isoformat()
            if activation_date_col and not pd.isna(row[activation_date_col]):
                try:
                    activation_date = pd.to_datetime(row[activation_date_col])
                    date_joined = activation_date.isoformat()
                except Exception as e:
                    logger.debug(f"Could not parse activation date for {username}: {e}")
            
            # Get card number (badge number)
            badge_number = None
            if card_number_col and not pd.isna(row[card_number_col]):
                card_value = str(row[card_number_col]).strip()
                if card_value and card_value.lower() not in ['nan', 'none', '']:
                    badge_number = card_value
            
            # Build notes from tracking sheet and other info (excluding company and account type)
            notes_parts = []
            
            if tracking_sheet_col and not pd.isna(row[tracking_sheet_col]):
                tracking_link = str(row[tracking_sheet_col]).strip()
                if tracking_link and tracking_link.lower() not in ['nan', 'none', '']:
                    notes_parts.append(f"Tracking Sheet: {tracking_link}")
            
            if notes_col and not pd.isna(row[notes_col]):
                existing_notes = str(row[notes_col]).strip()
                if existing_notes and existing_notes.lower() not in ['nan', 'none', '']:
                    notes_parts.append(existing_notes)
            
            notes = "; ".join(notes_parts) if notes_parts else None
            
            # Determine if active based on status
            is_active = True
            if status_col and not pd.isna(row[status_col]):
                status = str(row[status_col]).strip().lower()
                if status in ['inactive', 'inactivated', 'disabled']:
                    is_active = False
            
            # Extract PTA and map to project IDs
            ptas = []
            project_ids = []
            if pta_col and not pd.isna(row[pta_col]):
                pta = str(row[pta_col]).strip()
                if pta and pta.upper() not in ['NAN', 'NONE', '']:
                    ptas.append(pta.upper())
            
            # Map PTAs to project IDs
            if ptas and pta_lookup:
                project_ids = get_project_ids_from_ptas(ptas, pta_lookup)
                
                # Log PTA mapping results
                found_ptas = [pta for pta in ptas if pta in pta_lookup]
                missing_ptas = [pta for pta in ptas if pta not in pta_lookup]
                if found_ptas:
                    logger.debug(f"User {username}: Mapped {len(found_ptas)} PTA(s) to {len(project_ids)} project(s)")
                if missing_ptas:
                    logger.warning(f"User {username}: {len(missing_ptas)} PTA(s) not found in lookup: {', '.join(missing_ptas)}")
            
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
                "type": user_type,  # Mapped from account type
                "date_joined": date_joined,
                "domain": "",
                "notes": notes,
                "badge_number": badge_number,
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
            
            # Log parsed user details
            logger.info(f"Parsed user from row {idx+1}: {username} ({email})")
            logger.debug(f"  First: {user['first_name']}, Last: {user['last_name']}")
            logger.debug(f"  Active: {is_active}, User Type: {user_type}")
            if badge_number:
                logger.debug(f"  Badge: {badge_number}")
            if project_ids:
                logger.debug(f"  Projects: {project_ids} (from PTAs: {ptas})")
            if notes:
                logger.debug(f"  Notes: {notes}")
                    
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        import traceback
        traceback.print_exc()
        return []
        
    return users

def log_user_details(user: Dict[str, Any], prefix: str = "") -> None:
    """Log detailed user information in a readable format."""
    logger.info(f"{prefix}Username: {user.get('username', 'N/A')}")
    logger.info(f"{prefix}Email: {user.get('email', 'N/A')}")
    logger.info(f"{prefix}Name: {user.get('first_name', 'N/A')} {user.get('last_name', 'N/A')}")
    logger.info(f"{prefix}Active: {user.get('is_active', False)}")
    logger.info(f"{prefix}User Type ID: {user.get('type', 'N/A')}")
    logger.info(f"{prefix}Date Joined: {user.get('date_joined', 'N/A')}")
    if user.get('badge_number'):
        logger.info(f"{prefix}Badge Number: {user.get('badge_number')}")
    if user.get('projects'):
        logger.info(f"{prefix}Projects: {user.get('projects')}")
    if user.get('notes'):
        logger.info(f"{prefix}Notes: {user.get('notes')}")
    logger.info(f"{prefix}Full JSON: {json.dumps(user, indent=2, default=str)}")

def create_users(users: List[Dict[str, Any]]) -> None:
    """Create users in NEMO via API."""
    if not test_api_connection():
        return
    
    # Log summary of all users that will be created
    logger.info(f"\n{'=' * 60}")
    logger.info(f"PREVIEW: {len(users)} users to be created:")
    logger.info(f"{'=' * 60}")
    for idx, user in enumerate(users, 1):
        logger.info(f"\n[{idx}/{len(users)}] User to be created:")
        log_user_details(user, prefix="  ")
    
    logger.info(f"\n{'=' * 60}")
    logger.info(f"Starting creation of {len(users)} users in NEMO...")
    logger.info(f"{'=' * 60}\n")
        
    created_count = 0
    failed_count = 0
    
    for idx, user in enumerate(users, 1):
        logger.info(f"\n[{idx}/{len(users)}] Creating user: {user['username']} ({user['email']})")
        try:
            response = requests.post(
                NEMO_USERS_API_URL,
                headers=API_HEADERS,
                json=user
            )
            
            if response.status_code == 200:
                created_count += 1
                logger.info(f"✓ SUCCESS: Created user {user['username']}")
                log_user_details(user, prefix="  ")
                
                # Also log the API response if available
                try:
                    response_data = response.json()
                    logger.info(f"  API Response: {json.dumps(response_data, indent=2, default=str)}")
                except:
                    logger.info(f"  API Response: {response.text}")
            else:
                failed_count += 1
                logger.error(f"✗ FAILED: Could not create user {user['username']} ({user['email']})")
                logger.error(f"  HTTP Status: {response.status_code}")
                logger.error(f"  Error response: {response.text}")
                log_user_details(user, prefix="  ")
                
        except requests.exceptions.RequestException as e:
            failed_count += 1
            logger.error(f"✗ NETWORK ERROR: Could not create user {user['username']} ({user['email']})")
            logger.error(f"  Error: {e}")
            log_user_details(user, prefix="  ")
    
    logger.info(f"\n{'=' * 60}")
    logger.info(f"CREATION SUMMARY:")
    logger.info(f"  Total users: {len(users)}")
    logger.info(f"  Successfully created: {created_count}")
    logger.info(f"  Failed: {failed_count}")
    logger.info(f"{'=' * 60}")

def main():
    """Main function to read and create users."""
    logger.info("=" * 60)
    logger.info("Starting external user creation process...")
    logger.info(f"Log file: {log_filename}")
    logger.info("=" * 60)
    
    # Load PTA lookup table
    logger.info("\nLoading PTA lookup...")
    pta_lookup = load_pta_lookup()
    
    # Read external users from Excel file
    logger.info("\nReading external users from Excel file...")
    users = read_qualified_users(pta_lookup)
    
    if not users:
        logger.error("✗ No external users found in Excel file")
        return
    
    active_count = sum(1 for u in users if u['is_active'])
    inactive_count = len(users) - active_count
    logger.info(f"\n✓ Found {len(users)} external users")
    logger.info(f"  Active: {active_count}")
    logger.info(f"  Inactive: {inactive_count}")
    
    # Load existing usernames from file (or download if file doesn't exist)
    logger.info("\nLoading existing usernames...")
    existing_usernames = load_existing_usernames()
    
    if not existing_usernames:
        logger.warning("⚠ Warning: Could not load existing usernames from file. Attempting to download from API...")
        existing_usernames = download_existing_usernames()
        
        if not existing_usernames:
            logger.warning("⚠ Warning: Could not download existing users or no users found. Proceeding without duplicate check.")
    
    logger.info("\nFiltering out duplicate users...")
    filtered_users = filter_existing_users(users, existing_usernames)
    
    if not filtered_users:
        logger.warning("No new users to create! All users already exist in NEMO.")
        return
    
    logger.info(f"\n✓ {len(filtered_users)} new users to create (filtered out {len(users) - len(filtered_users)} duplicates)")
    
    # Create users
    create_users(filtered_users)
    
    logger.info("=" * 60)
    logger.info("External user creation process completed")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()