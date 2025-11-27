#!/usr/bin/env python3
"""
Script to assign projects to existing external users based on PTA information.
Reads from Copy-external-users.xlsx and updates existing users with project IDs.
"""

import os
import json
import pandas as pd
import requests
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional, Set, Tuple
from datetime import datetime
import re
import logging
import time

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

def setup_logging() -> logging.Logger:
    """Set up logging to file with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"logs/assign_projects_to_external_users_{timestamp}.log"
    
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Create logger
    logger = logging.getLogger('assign_projects')
    logger.setLevel(logging.DEBUG)
    
    # Remove existing handlers
    logger.handlers = []
    
    # File handler for detailed logging
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    
    # Console handler for important messages
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info("=" * 60)
    logger.info("ASSIGN PROJECTS TO EXTERNAL USERS SESSION STARTED")
    logger.info("=" * 60)
    logger.info(f"Log file: {log_filename}")
    
    return logger

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

def load_project_name_lookup(filename: str = "project_name_lookup.json") -> Dict[str, int]:
    """Load project name to project ID lookup from JSON file."""
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                lookup = json.load(f)
                print(f"✓ Loaded project name lookup with {len(lookup)} entries")
                return lookup
        else:
            print(f"✗ Error: {filename} not found!")
            print("Please ensure project_name_lookup.json exists in the current directory.")
            return {}
    except Exception as e:
        print(f"✗ Error loading project name lookup: {e}")
        return {}

def load_excel_pta_to_name_mapping(excel_path: str) -> Dict[str, str]:
    """Load Excel file and create a mapping from PTA to PTA Name."""
    try:
        print(f"Reading Excel file: {excel_path}...")
        df = pd.read_excel(excel_path)
        
        print(f"Found {len(df)} rows")
        print(f"Columns: {df.columns.tolist()}")
        
        # Find PTA and PTA Name columns
        pta_col = None
        pta_name_col = None
        
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if (col_lower == 'pta' or col_lower.startswith('pta')) and 'name' not in col_lower and not pta_col:
                pta_col = col
            elif 'pta name' in col_lower or (col_lower == 'name' and pta_col):
                pta_name_col = col
        
        if not pta_col:
            print("✗ Error: Could not find PTA column in Excel file")
            return {}
        
        if not pta_name_col:
            print("✗ Error: Could not find PTA Name column in Excel file")
            return {}
        
        print(f"Using columns:")
        print(f"  PTA: {pta_col}")
        print(f"  PTA Name: {pta_name_col}")
        
        # Create mapping
        pta_to_name = {}
        for idx, row in df.iterrows():
            if pd.notna(row[pta_col]):
                pta = str(row[pta_col]).strip().upper()
                if pta and pta not in ['NAN', 'NONE', '']:
                    pta_name = str(row[pta_name_col]).strip() if pd.notna(row[pta_name_col]) else pta
                    if pta_name and pta_name.lower() not in ['nan', 'none', '']:
                        # If PTA already exists, keep the first one (or log warning)
                        if pta in pta_to_name and pta_to_name[pta] != pta_name:
                            print(f"⚠ Warning: PTA {pta} has multiple names: '{pta_to_name[pta]}' and '{pta_name}'")
                        else:
                            pta_to_name[pta] = pta_name
        
        print(f"✓ Created PTA to PTA Name mapping with {len(pta_to_name)} entries")
        return pta_to_name
        
    except Exception as e:
        print(f"✗ Error reading Excel file: {e}")
        import traceback
        traceback.print_exc()
        return {}

def load_existing_users() -> Dict[str, Dict[str, Any]]:
    """Load existing users from nemo_users.json and create lookup dictionaries."""
    try:
        filename = "nemo_users.json"
        if not os.path.exists(filename):
            print(f"⚠ Warning: {filename} not found. Attempting to download users...")
            return download_and_load_users()
        
        with open(filename, 'r') as f:
            users = json.load(f)
        
        # Create lookup dictionaries: email -> user, username -> user, name -> user
        email_lookup = {}
        username_lookup = {}
        name_lookup = {}  # "first_name|last_name" -> user (or list if duplicates)
        
        for user in users:
            email = user.get('email', '').strip().lower() if user.get('email') else None
            username = user.get('username', '').strip().lower() if user.get('username') else None
            first_name = user.get('first_name', '').strip().lower() if user.get('first_name') else None
            last_name = user.get('last_name', '').strip().lower() if user.get('last_name') else None
            
            if email and '@' in email:
                email_lookup[email] = user
            if username:
                username_lookup[username] = user
            if first_name and last_name:
                name_key = f"{first_name}|{last_name}"
                if name_key in name_lookup:
                    # Handle duplicates - convert to list if not already
                    if not isinstance(name_lookup[name_key], list):
                        name_lookup[name_key] = [name_lookup[name_key]]
                    name_lookup[name_key].append(user)
                else:
                    name_lookup[name_key] = user
        
        print(f"✓ Loaded {len(users)} users from {filename}")
        print(f"✓ Created email lookup with {len(email_lookup)} entries")
        print(f"✓ Created username lookup with {len(username_lookup)} entries")
        print(f"✓ Created name lookup with {len(name_lookup)} entries")
        
        return {
            'users': users,
            'email_lookup': email_lookup,
            'username_lookup': username_lookup,
            'name_lookup': name_lookup
        }
    except Exception as e:
        print(f"✗ Error loading users: {e}")
        return {'users': [], 'email_lookup': {}, 'username_lookup': {}}

def download_and_load_users() -> Dict[str, Dict[str, Any]]:
    """Download users from API and create lookup dictionaries."""
    try:
        print("Downloading users from NEMO API...")
        all_users = []
        page = 1
        
        while True:
            params = {'page': page}
            response = requests.get(NEMO_USERS_API_URL, headers=API_HEADERS, params=params)
            
            if response.status_code == 200:
                response_data = response.json()
                
                if 'results' in response_data:
                    users = response_data['results']
                    print(f"  Page {page}: Retrieved {len(users)} users")
                else:
                    users = response_data
                    print(f"  Retrieved {len(users)} users (no pagination)")
                
                if not users:
                    break
                
                all_users.extend(users)
                
                if 'next' in response_data and response_data['next']:
                    page += 1
                else:
                    break
            else:
                print(f"✗ Failed to download users: HTTP {response.status_code}")
                return {'users': [], 'email_lookup': {}, 'username_lookup': {}}
        
        # Create lookup dictionaries
        email_lookup = {}
        username_lookup = {}
        name_lookup = {}  # "first_name|last_name" -> user (or list if duplicates)
        
        for user in all_users:
            email = user.get('email', '').strip().lower() if user.get('email') else None
            username = user.get('username', '').strip().lower() if user.get('username') else None
            first_name = user.get('first_name', '').strip().lower() if user.get('first_name') else None
            last_name = user.get('last_name', '').strip().lower() if user.get('last_name') else None
            
            if email and '@' in email:
                email_lookup[email] = user
            if username:
                username_lookup[username] = user
            if first_name and last_name:
                name_key = f"{first_name}|{last_name}"
                if name_key in name_lookup:
                    # Handle duplicates - convert to list if not already
                    if not isinstance(name_lookup[name_key], list):
                        name_lookup[name_key] = [name_lookup[name_key]]
                    name_lookup[name_key].append(user)
                else:
                    name_lookup[name_key] = user
        
        print(f"✓ Downloaded {len(all_users)} users")
        print(f"✓ Created email lookup with {len(email_lookup)} entries")
        print(f"✓ Created username lookup with {len(username_lookup)} entries")
        print(f"✓ Created name lookup with {len(name_lookup)} entries")
        
        return {
            'users': all_users,
            'email_lookup': email_lookup,
            'username_lookup': username_lookup,
            'name_lookup': name_lookup
        }
    except Exception as e:
        print(f"✗ Error downloading users: {e}")
        return {'users': [], 'email_lookup': {}, 'username_lookup': {}}

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

def get_project_ids_from_ptas(
    ptas: List[str],
    excel_pta_to_name: Dict[str, str],
    project_name_lookup: Dict[str, int]
) -> Tuple[List[int], List[str]]:
    """Map PTAs to project IDs via Excel PTA Name lookup.
    
    Flow: PTA -> Excel PTA Name -> project_name_lookup -> Project ID
    """
    project_ids = []
    missing_mappings = []
    
    for pta in ptas:
        # Step 1: Find PTA Name in Excel
        if pta not in excel_pta_to_name:
            missing_mappings.append(f"{pta} (not in Excel)")
            continue
        
        pta_name = excel_pta_to_name[pta]
        
        # Step 2: Find Project ID using PTA Name
        if pta_name not in project_name_lookup:
            missing_mappings.append(f"{pta} -> '{pta_name}' (not in project_name_lookup)")
            continue
        
        project_id = project_name_lookup[pta_name]
        if project_id not in project_ids:
            project_ids.append(project_id)
    
    return project_ids, missing_mappings

def read_excel_and_extract_project_assignments(
    excel_path: str,
    excel_pta_to_name: Dict[str, str],
    project_name_lookup: Dict[str, int],
    user_lookups: Dict[str, Dict[str, Any]],
    logger: logging.Logger
) -> List[Dict[str, Any]]:
    """Read Excel file and extract project assignments for existing external users."""
    assignments = []
    
    try:
        print(f"Reading {excel_path}...")
        # Read Excel file
        df = pd.read_excel(excel_path, dtype=str, keep_default_na=False)
        
        print(f"Found {len(df)} rows")
        print(f"Columns: {df.columns.tolist()}")
        
        # Map column names
        email_col = None
        sunet_col = None
        pta_col = None
        first_col = None
        last_col = None
        
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if col_lower == 'email' and not email_col:
                email_col = col
            elif 'sunet' in col_lower and not sunet_col:
                sunet_col = col
            elif (col_lower == 'pta' or col_lower.startswith('pta')) and 'other' not in col_lower and not pta_col:
                pta_col = col
            elif (col_lower == 'first' or col_lower == 'first name') and not first_col:
                first_col = col
            elif (col_lower == 'last' or col_lower == 'last name') and not last_col:
                last_col = col
        
        if not email_col and not sunet_col and (not first_col or not last_col):
            print("✗ Error: Could not find Email, SUNet ID, or First/Last name columns")
            return []
        
        if not pta_col:
            print("✗ Error: Could not find PTA column")
            return []
        
        print(f"Using columns:")
        print(f"  Email: {email_col}")
        print(f"  SUNet ID: {sunet_col}")
        print(f"  First Name: {first_col}")
        print(f"  Last Name: {last_col}")
        print(f"  PTA: {pta_col}")
        
        email_lookup = user_lookups.get('email_lookup', {})
        username_lookup = user_lookups.get('username_lookup', {})
        name_lookup = user_lookups.get('name_lookup', {})
        
        # First pass: Collect all PTAs for each user across all rows
        user_ptas: Dict[str, List[str]] = {}  # email -> list of PTAs
        user_info: Dict[str, Dict[str, Any]] = {}  # email -> user info
        
        for idx, row in df.iterrows():
            # Extract email
            email = None
            if email_col and row[email_col]:
                email_value = str(row[email_col]).strip()
                if email_value and email_value.lower() not in ['nan', 'none', '']:
                    email = email_value.lower()
            
            # If no email, try to construct from SUNet ID
            if not email and sunet_col and row[sunet_col]:
                sunet_id = str(row[sunet_col]).strip()
                if sunet_id and sunet_id.lower() not in ['nan', 'none', '']:
                    email = f"{sunet_id.lower()}@stanford.edu"
            
            # Find user by email/username first, then fall back to name
            user = None
            user_identifier = None
            
            # Try email first
            if email and '@' in email:
                if email in email_lookup:
                    user = email_lookup[email]
                    user_identifier = email
                else:
                    # Try username
                    username = email.split('@')[0]
                    if username in username_lookup:
                        user = username_lookup[username]
                        user_identifier = f"{email} (matched by username)"
            
            # Fall back to name matching if email/username didn't work
            if not user and first_col and last_col:
                first_name = None
                last_name = None
                if row[first_col]:
                    first_name = str(row[first_col]).strip().lower()
                if row[last_col]:
                    last_name = str(row[last_col]).strip().lower()
                
                if first_name and last_name and first_name not in ['nan', 'none', ''] and last_name not in ['nan', 'none', '']:
                    name_key = f"{first_name}|{last_name}"
                    if name_key in name_lookup:
                        name_match = name_lookup[name_key]
                        if isinstance(name_match, list):
                            # Multiple users with same name - warn and use first one
                            logger.warning(f"Multiple users found with name '{first_name} {last_name}' (row {idx + 2}) - using first match")
                            user = name_match[0]
                            user_identifier = f"{first_name} {last_name} (name match, {len(name_match)} duplicates)"
                        else:
                            user = name_match
                            user_identifier = f"{first_name} {last_name} (name match)"
            
            if not user:
                identifier = email if email and '@' in email else (f"{first_name} {last_name}" if first_col and last_col and row.get(first_col) and row.get(last_col) else "unknown")
                logger.warning(f"User not found in NEMO: {identifier} (row {idx + 2}) - skipping")
                continue
            
            user_id = user.get('id')
            if not user_id:
                logger.warning(f"User found but no ID: {email}")
                continue
            
            # Use email as key if available, otherwise use user_id
            user_key = email if email and '@' in email else f"user_{user_id}"
            
            # Store user info for later use
            if user_key not in user_info:
                user_info[user_key] = {
                    'user_id': user_id,
                    'user': user,
                    'email': user.get('email', '') if user.get('email') else user_identifier,
                    'identifier': user_identifier
                }
            
            # Extract PTA from this row (only PTA column, no Other PTAs)
            if pta_col and row[pta_col]:
                pta = str(row[pta_col]).strip().upper()
                if pta and pta not in ['NAN', 'NONE', '']:
                    # Collect PTAs for this user (across all rows)
                    if user_key not in user_ptas:
                        user_ptas[user_key] = []
                    user_ptas[user_key].append(pta)
        
        # Second pass: Process each user with all their collected PTAs
        for user_key, ptas_list in user_ptas.items():
            if not ptas_list:
                continue
            
            # Deduplicate PTAs for this user
            ptas = list(dict.fromkeys(ptas_list))
            
            # Get user info
            info = user_info[user_key]
            user = info['user']
            user_id = info['user_id']
            user_email = info.get('email', user_key)
            user_identifier = info.get('identifier', user_email)
            
            # Map PTAs to project IDs via Excel PTA Name lookup
            project_ids, missing_mappings = get_project_ids_from_ptas(
                ptas,
                excel_pta_to_name,
                project_name_lookup
            )
            
            if not project_ids:
                logger.warning(f"User {user_identifier}: No projects found for PTAs: {', '.join(missing_mappings)}")
                continue
            
            if missing_mappings:
                logger.warning(f"User {user_identifier}: Some PTAs could not be mapped: {', '.join(missing_mappings)}")
            
            # Get existing projects from user
            existing_projects = user.get('projects', [])
            if not isinstance(existing_projects, list):
                existing_projects = []
            
            # Merge with new projects (avoid duplicates)
            # We update ALL projects, not just new ones - this ensures consistency
            all_project_ids = list(set(existing_projects + project_ids))
            
            # Calculate new projects for logging purposes
            new_project_ids = [pid for pid in project_ids if pid not in existing_projects]
            
            # Always add assignment - we'll update the user regardless of existing projects
            assignments.append({
                'user_id': user_id,
                'email': user_email,
                'identifier': user_identifier,
                'username': user.get('username', 'N/A'),
                'existing_projects': existing_projects,
                'new_projects': new_project_ids,
                'all_projects': all_project_ids,
                'ptas': ptas,
                'found_ptas': [pta for pta in ptas if pta in excel_pta_to_name and excel_pta_to_name[pta] in project_name_lookup],
                'missing_ptas': [pta for pta in ptas if pta not in excel_pta_to_name or excel_pta_to_name.get(pta) not in project_name_lookup]
            })
            
            if new_project_ids:
                logger.info(f"User {user_identifier}: Adding {len(new_project_ids)} new project(s) from {len(ptas)} PTA(s)")
            else:
                logger.info(f"User {user_identifier}: Updating with {len(project_ids)} project(s) from {len(ptas)} PTA(s) (all already assigned)")
            
            if assignments[-1]['missing_ptas']:
                logger.warning(f"  Missing PTAs: {', '.join(assignments[-1]['missing_ptas'])}")
    
    except Exception as e:
        print(f"✗ Error reading Excel file: {e}")
        logger.error(f"Error reading Excel: {e}", exc_info=True)
        import traceback
        traceback.print_exc()
        return []
    
    return assignments

def update_user_projects(user_id: int, project_ids: List[int], email: str, logger: logging.Logger) -> bool:
    """Update a user's projects via the NEMO API.
    
    Returns True if successful, False otherwise.
    """
    update_url = f"{NEMO_USERS_API_URL}{user_id}/"
    
    # First, get the current user data
    try:
        response = requests.get(update_url, headers=API_HEADERS)
        if response.status_code != 200:
            logger.error(f"Failed to fetch user {user_id}: HTTP {response.status_code}")
            return False
        
        user_data = response.json()
        
        # Update projects list
        user_data['projects'] = project_ids
        
        # Use PUT to update the user
        response = requests.put(update_url, json=user_data, headers=API_HEADERS)
        
        if response.status_code == 200:
            logger.info(f"SUCCESS: Updated user {user_id} ({email}) with {len(project_ids)} project(s)")
            return True
        elif response.status_code == 400:
            error_msg = response.text
            logger.error(f"FAILED: Bad request for user {user_id} ({email}) - {error_msg}")
            logger.debug(f"Payload projects: {project_ids}")
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

def main():
    """Main function to assign projects to existing external users."""
    logger = setup_logging()
    
    print("Starting project assignment to existing external users...")
    print(f"API Endpoint: {NEMO_USERS_API_URL}")
    print("-" * 60)
    logger.info(f"API Endpoint: {NEMO_USERS_API_URL}")
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Load Excel PTA to PTA Name mapping
    pta_mapping_path = "/Users/adenton/Desktop/NEMO-Merger/SNSF-Data/Copy of SNSF PTAs for Alex Denton.xlsx"
    print(f"\nLoading Excel PTA to PTA Name mapping from {pta_mapping_path}...")
    excel_pta_to_name = load_excel_pta_to_name_mapping(pta_mapping_path)
    if not excel_pta_to_name:
        print("Cannot proceed without Excel PTA mapping.")
        return
    
    # Load project name lookup
    print("\nLoading project name lookup...")
    project_name_lookup = load_project_name_lookup()
    if not project_name_lookup:
        print("Cannot proceed without project name lookup.")
        return
    
    # Load existing users
    print("\nLoading existing users...")
    user_lookups = load_existing_users()
    if not user_lookups.get('email_lookup') and not user_lookups.get('username_lookup'):
        print("Cannot proceed without user lookup.")
        return
    
    # Read Excel and extract project assignments
    external_users_path = "/Users/adenton/Desktop/NEMO-Merger/SNSF-Data/Copy-external-users.xlsx"
    print(f"\nReading Excel file: {external_users_path}")
    assignments = read_excel_and_extract_project_assignments(
        external_users_path,
        excel_pta_to_name,
        project_name_lookup,
        user_lookups,
        logger
    )
    
    if not assignments:
        print("No project assignments found to process.")
        return
    
    print(f"\nFound {len(assignments)} users to update with projects")
    
    # Confirm before proceeding
    print("\n" + "=" * 60)
    print("SUMMARY OF ASSIGNMENTS")
    print("=" * 60)
    for i, assignment in enumerate(assignments[:10], 1):
        identifier = assignment.get('identifier', assignment['email'])
        if assignment['new_projects']:
            print(f"{i}. {identifier}: Adding {len(assignment['new_projects'])} new project(s) (total: {len(assignment['all_projects'])})")
        else:
            print(f"{i}. {identifier}: Updating with {len(assignment['all_projects'])} project(s) (all already assigned)")
        print(f"   PTAs: {', '.join(assignment['found_ptas'])}")
        if assignment['missing_ptas']:
            print(f"   Missing PTAs: {', '.join(assignment['missing_ptas'])}")
    if len(assignments) > 10:
        print(f"\n... and {len(assignments) - 10} more users")
    
    response = input("\nProceed with updating users? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("Aborted by user.")
        return
    
    # Update users
    print("\nUpdating users...")
    successful = 0
    failed = 0
    
    for i, assignment in enumerate(assignments, 1):
        identifier = assignment.get('identifier', assignment['email'])
        print(f"\n[{i}/{len(assignments)}] Updating {identifier}...")
        if assignment['new_projects']:
            print(f"  Adding {len(assignment['new_projects'])} new project(s) (total: {len(assignment['all_projects'])})")
        else:
            print(f"  Updating with {len(assignment['all_projects'])} project(s) (all already assigned)")
        
        success = update_user_projects(
            assignment['user_id'],
            assignment['all_projects'],
            identifier,
            logger
        )
        
        if success:
            successful += 1
        else:
            failed += 1
        
        # Add a small delay to avoid overwhelming the API
        time.sleep(0.5)
    
    # Summary
    print("\n" + "=" * 60)
    print("PROJECT ASSIGNMENT SUMMARY")
    print("=" * 60)
    print(f"Total users to update: {len(assignments)}")
    print(f"Successfully updated: {successful}")
    print(f"Failed to update: {failed}")
    if len(assignments) > 0:
        print(f"Success rate: {(successful/len(assignments)*100):.1f}%")
    
    logger.info("=" * 60)
    logger.info("PROJECT ASSIGNMENT SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total users to update: {len(assignments)}")
    logger.info(f"Successfully updated: {successful}")
    logger.info(f"Failed to update: {failed}")
    logger.info("=" * 60)
    logger.info("ASSIGN PROJECTS TO EXTERNAL USERS SESSION ENDED")
    logger.info("=" * 60)
    
    print(f"\n✓ Detailed log saved to logs/assign_projects_to_external_users_*.log")

if __name__ == "__main__":
    main()

