#!/usr/bin/env python3
"""
Script to create NEMO projects from SNSF PTA Excel file.
Maps 'PTA' to 'application_identifier', 'PTA Name' to 'name', 'Account' to account lookup, and 'project_type' to project_types.
"""

import pandas as pd
import requests
import json
import os
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional, Tuple
import time
import logging
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoint for projects
NEMO_PROJECTS_API_URL = "https://nemo.stanford.edu/api/projects/"

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

# Template for project data
PROJECT_TEMPLATE = {
    "id": None,  # Will be assigned by API
    "principal_investigators": [],
    "users": [],
    "name": "",  # Will be filled from 'PTA Name' column
    "application_identifier": "",  # Will be filled from 'PTA' column
    "start_date": None,
    "active": True,
    "allow_consumable_withdrawals": True,
    "allow_staff_charges": True,
    "account": None,  # Will be filled manually
    "discipline": None,  # Will be filled manually
    "project_types": [],
    "only_allow_tools": [],
    "project_name": None,
    "contact_name": "",  # Will be filled manually
    "contact_phone": None,
    "contact_email": "",
    "expires_on": None,
    "addressee": "",
    "comments": "",
    "no_charge": False,
    "no_tax": False,
    "category": None,  # Will be filled manually
    "institution": None,
    "department": None,  # Will be filled manually
    "staff_host": None
}

def read_user_information_excel(file_path: str) -> pd.DataFrame:
    """Read the User Information Excel file and return a DataFrame."""
    try:
        df = pd.read_excel(file_path)
        print(f"Successfully read {file_path}")
        print(f"Shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        exit(1)

def extract_unique_projects(df: pd.DataFrame) -> List[Dict[str, str]]:
    """Extract unique projects from the DataFrame."""
    # Filter out rows where PTA is NaN (required field)
    df_filtered = df.dropna(subset=['PTA'])
    
    # Create unique projects based on PTA (application_identifier)
    unique_projects = []
    seen_ptas = set()
    
    for _, row in df_filtered.iterrows():
        pta = str(row['PTA']).strip() if pd.notna(row['PTA']) else ""
        pta_name = str(row['PTA Name']).strip() if pd.notna(row['PTA Name']) else pta
        account_name = str(row['Account']).strip() if pd.notna(row['Account']) else ""
        project_type = str(row['project_type']).strip() if pd.notna(row['project_type']) else ""
        
        # Skip if PTA is empty or already processed
        if not pta or pta in seen_ptas:
            continue
            
        seen_ptas.add(pta)
        unique_projects.append({
            'name': pta_name,
            'application_identifier': pta,
            'account_name': account_name,
            'project_type': project_type
        })
    
    print(f"Found {len(unique_projects)} unique projects")
    return unique_projects

def load_account_lookup(filename: str = "account_lookup.json") -> Dict[str, int]:
    """Load the account lookup from the downloaded accounts."""
    try:
        with open(filename, 'r') as f:
            lookup = json.load(f)
        print(f"✓ Loaded account lookup with {len(lookup)} accounts")
        return lookup
    except FileNotFoundError:
        print(f"✗ Account lookup file {filename} not found!")
        print("Please run download_accounts.py first to download accounts from NEMO.")
        return {}
    except Exception as e:
        print(f"✗ Error loading account lookup: {e}")
        return {}

def load_rate_categories(filename: str = "nemo_rate_categories.json") -> Dict[str, int]:
    """Load rate categories from nemo_rate_categories.json and create a case-insensitive mapping.
    
    Maps Excel project_type values (lowercase) to NEMO rate category IDs.
    Handles case-insensitive matching between Excel values and NEMO category names.
    """
    try:
        with open(filename, 'r') as f:
            rate_categories = json.load(f)
        
        # Create a case-insensitive mapping from Excel project_type to category ID
        # Excel has lowercase values like "industrial", "other academic", etc.
        # NEMO has mixed case like "Industry", "Other Academic", etc.
        mapping = {}
        
        # Common mappings from Excel project_type to NEMO category names
        excel_to_nemo_name = {
            "industrial": "Industry",
            "industrial-sbir": "Industry-SBIR",
            "local": "Local",
            "no charge": "No Charge",
            "other academic": "Other Academic",
            "academic": "Other Academic",  # Default academic to Other Academic
            "foreign": "Other Academic"  # Default foreign to Other Academic
        }
        
        # Build mapping: Excel project_type (lowercase) -> category ID
        for excel_type, nemo_name in excel_to_nemo_name.items():
            # Find matching category (case-insensitive)
            found = False
            for category in rate_categories:
                if category.get('name', '').lower() == nemo_name.lower():
                    mapping[excel_type.lower()] = category['id']
                    found = True
                    break
            
            if not found:
                print(f"⚠ Warning: Could not find rate category '{nemo_name}' for Excel type '{excel_type}'")
        
        # Also create a direct case-insensitive lookup for any project_type value
        # This allows matching even if not in the predefined mapping
        for category in rate_categories:
            category_name = category.get('name', '').lower()
            if category_name and category_name not in mapping:
                # Add direct lowercase mapping (if not already mapped)
                mapping[category_name] = category['id']
        
        print(f"✓ Loaded rate categories with {len(rate_categories)} categories")
        print(f"✓ Created case-insensitive mapping with {len(mapping)} entries")
        return mapping
        
    except FileNotFoundError:
        print(f"✗ Rate categories file {filename} not found!")
        print("Please run download_rate_categories.py first to download rate categories from NEMO.")
        return {}
    except Exception as e:
        print(f"✗ Error loading rate categories: {e}")
        return {}

def load_existing_ptas(filename: str = "existing_ptas.json") -> set:
    """Load existing PTAs from a JSON file."""
    try:
        with open(filename, 'r') as f:
            ptas_list = json.load(f)
        existing_ptas = set(ptas_list)
        print(f"✓ Loaded {len(existing_ptas)} existing PTAs from {filename}")
        return existing_ptas
    except FileNotFoundError:
        print(f"⚠ Existing PTAs file {filename} not found!")
        print("Please run download_projects.py first to download projects from NEMO.")
        return set()
    except Exception as e:
        print(f"✗ Error loading existing PTAs: {e}")
        return set()

def download_existing_projects() -> set:
    """Download all existing projects from NEMO API and return a set of PTAs."""
    try:
        print("Downloading existing projects from NEMO API...")
        response = requests.get(NEMO_PROJECTS_API_URL, headers=API_HEADERS)
        
        if response.status_code == 200:
            projects = response.json()
            # Extract PTAs from projects
            existing_ptas = set()
            for project in projects:
                # Check for PTA field (could be 'PTA', 'pta', 'application_identifier', etc.)
                pta = None
                if 'application_identifier' in project and project['application_identifier']:
                    pta = str(project['application_identifier']).strip()
                elif 'PTA' in project and project['PTA']:
                    pta = str(project['PTA']).strip()
                elif 'pta' in project and project['pta']:
                    pta = str(project['pta']).strip()
                
                if pta and pta.lower() != 'none' and pta.lower() != 'null':
                    existing_ptas.add(pta)
            
            print(f"✓ Successfully downloaded {len(projects)} projects")
            print(f"✓ Found {len(existing_ptas)} unique PTAs in existing projects")
            return existing_ptas
        else:
            print(f"✗ Failed to download projects: HTTP {response.status_code} - {response.text}")
            return set()
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error downloading projects: {e}")
        return set()
    except json.JSONDecodeError as e:
        print(f"✗ Error parsing JSON response: {e}")
        return set()
    except Exception as e:
        print(f"✗ Error processing projects: {e}")
        return set()

def filter_existing_projects(projects: List[Dict[str, str]], existing_ptas: set) -> List[Dict[str, str]]:
    """Filter out projects that already exist in NEMO based on PTA (application_identifier) comparison."""
    new_projects = []
    duplicate_projects = []
    
    for project in projects:
        # Compare the PTA (application_identifier) from Excel to the PTAs from API
        pta = project['application_identifier'].strip()
        
        if pta in existing_ptas:
            duplicate_projects.append(project)
        else:
            new_projects.append(project)
    
    if duplicate_projects:
        print(f"⚠ Filtered out {len(duplicate_projects)} duplicate projects (already exist in NEMO):")
        for dup in duplicate_projects[:10]:  # Show first 10
            print(f"  - {dup['application_identifier']} ({dup['name']})")
        if len(duplicate_projects) > 10:
            print(f"  ... and {len(duplicate_projects) - 10} more duplicates")
    
    return new_projects

def match_projects_to_accounts(projects: List[Dict[str, str]], account_lookup: Dict[str, int]) -> List[Dict[str, Any]]:
    """Match projects to accounts based on Account name from the Excel file.
    
    Maps the Account name from Excel to the account ID from account_lookup.json
    (which is created from nemo_accounts.json). The account_id will be used
    to set the "account" field in the project payload (must be an integer ID).
    """
    matched_projects = []
    unmatched_projects = []
    
    for project in projects:
        account_name = project.get('account_name', '').strip()
        
        if account_name and account_name in account_lookup:
            # Found matching account - map Account name to account ID
            project_with_account = project.copy()
            project_with_account['account_id'] = account_lookup[account_name]  # Integer ID from account_lookup.json
            matched_projects.append(project_with_account)
        else:
            # No matching account found
            project_with_account = project.copy()
            project_with_account['account_id'] = None
            unmatched_projects.append(project_with_account)
    
    print(f"✓ Matched {len(matched_projects)} projects to accounts")
    if unmatched_projects:
        print(f"⚠ {len(unmatched_projects)} projects could not be matched to accounts")
        print("These will need manual account assignment.")
        for unmatched in unmatched_projects[:5]:  # Show first 5 unmatched
            print(f"  - {unmatched['application_identifier']}: Account '{unmatched.get('account_name', 'N/A')}' not found")
    
    return matched_projects + unmatched_projects

def create_project_payload(project_data: Dict[str, Any], rate_mapping: Dict[str, int]) -> Dict[str, Any]:
    """Create a project payload with the given data."""
    payload = PROJECT_TEMPLATE.copy()
    payload["name"] = project_data['name']
    payload["application_identifier"] = project_data['application_identifier']
    
    # Set the account ID if we have one
    # The "account" field must be an integer ID from nemo_accounts.json (loaded via account_lookup.json)
    # We map the Account name from Excel to the account ID using account_lookup
    if project_data.get('account_id'):
        payload["account"] = project_data['account_id']  # This is the integer ID from account_lookup.json
        account_name = project_data.get('account_name', 'Unknown')
        print(f"  → Associated with account ID: {project_data['account_id']} (Account: {account_name})")
    else:
        account_name = project_data.get('account_name', 'Unknown')
        print(f"  ⚠ No account found for: {account_name}")
    
    # Set the rate category based on project_type from Excel (case-insensitive matching)
    project_type = project_data.get('project_type', '').strip().lower()
    if project_type:
        # Try direct lookup first
        if project_type in rate_mapping:
            payload["category"] = rate_mapping[project_type]
            print(f"  → Set rate category ID: {rate_mapping[project_type]} for type '{project_data.get('project_type', '')}'")
        else:
            # Try case-insensitive lookup in rate_mapping keys
            found = False
            for key, category_id in rate_mapping.items():
                if key.lower() == project_type:
                    payload["category"] = category_id
                    print(f"  → Set rate category ID: {category_id} for type '{project_data.get('project_type', '')}' (matched '{key}')")
                    found = True
                    break
            
            if not found:
                # Default to "Academic" (most common) if available, otherwise use first available
                if "academic" in rate_mapping:
                    payload["category"] = rate_mapping["academic"]
                    print(f"  ⚠ Defaulted to 'Academic' rate category ID: {rate_mapping['academic']} for type '{project_data.get('project_type', '')}'")
                elif "other academic" in rate_mapping:
                    payload["category"] = rate_mapping["other academic"]
                    print(f"  ⚠ Defaulted to 'Other Academic' rate category ID: {rate_mapping['other academic']} for type '{project_data.get('project_type', '')}'")
                elif "local" in rate_mapping:
                    payload["category"] = rate_mapping["local"]
                    print(f"  ⚠ Defaulted to 'Local' rate category ID: {rate_mapping['local']} for type '{project_data.get('project_type', '')}'")
                elif rate_mapping:
                    # Use first available category as last resort
                    first_id = list(rate_mapping.values())[0]
                    payload["category"] = first_id
                    print(f"  ⚠ Defaulted to first available rate category ID: {first_id} for type '{project_data.get('project_type', '')}'")
                else:
                    print(f"  ✗ ERROR: No rate categories available and project_type '{project_data.get('project_type', '')}' not found in mapping!")
    else:
        # No project_type provided - default to "Academic" (most common) if available
        if "academic" in rate_mapping:
            payload["category"] = rate_mapping["academic"]
            print(f"  ⚠ No project_type provided, defaulted to 'Academic' rate category ID: {rate_mapping['academic']}")
        elif "other academic" in rate_mapping:
            payload["category"] = rate_mapping["other academic"]
            print(f"  ⚠ No project_type provided, defaulted to 'Other Academic' rate category ID: {rate_mapping['other academic']}")
        elif "local" in rate_mapping:
            payload["category"] = rate_mapping["local"]
            print(f"  ⚠ No project_type provided, defaulted to 'Local' rate category ID: {rate_mapping['local']}")
        elif rate_mapping:
            first_id = list(rate_mapping.values())[0]
            payload["category"] = first_id
            print(f"  ⚠ No project_type provided, defaulted to first available rate category ID: {first_id}")
        else:
            print(f"  ✗ ERROR: No project_type provided and no rate categories available!")
    
    # Note: project_types field remains empty as it is not used in our lab management software
    
    return payload

def test_api_connection():
    """Test the API connection and authentication."""
    try:
        response = requests.get(NEMO_PROJECTS_API_URL, headers=API_HEADERS)
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

def push_project_to_api(project_data: Dict[str, str], api_url: str, rate_mapping: Dict[str, int], logger: logging.Logger) -> Optional[Dict[str, Any]]:
    """Push a single project to the NEMO API.
    
    Returns:
        Dict with project details if successful, None if failed
    """
    payload = create_project_payload(project_data, rate_mapping)
    
    try:
        response = requests.post(api_url, json=payload, headers=API_HEADERS)
        
        if response.status_code == 200:  # Created
            created_project = response.json() if response.text else {}
            project_id = created_project.get('id', 'Unknown')
            print(f"✓ Successfully created project: {project_data['name']} (ID: {project_id})")
            
            # Log successful creation with full details
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'status': 'SUCCESS',
                'project_id': project_id,
                'name': project_data['name'],
                'application_identifier': project_data['application_identifier'],
                'account_id': project_data.get('account_id'),
                'account_name': project_data.get('account_name'),
                'project_type': project_data.get('project_type'),
                'category_id': payload.get('category'),
                'payload_sent': payload,
                'response': created_project
            }
            logger.info(f"SUCCESS: Created project ID {project_id} - {project_data['name']} (PTA: {project_data['application_identifier']})")
            logger.debug(f"Full details: {json.dumps(log_entry, indent=2)}")
            
            return log_entry
        elif response.status_code == 400:
            error_msg = response.text
            print(f"✗ Bad request for project '{project_data['name']}': {error_msg}")
            logger.error(f"FAILED: Bad request for {project_data['name']} (PTA: {project_data['application_identifier']}) - {error_msg}")
            logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
            return None
        elif response.status_code == 401:
            print(f"✗ Authentication failed for project '{project_data['name']}': Check your NEMO_TOKEN")
            logger.error(f"FAILED: Authentication failed for {project_data['name']} (PTA: {project_data['application_identifier']})")
            return None
        elif response.status_code == 403:
            print(f"✗ Permission denied for project '{project_data['name']}': Check your API permissions")
            logger.error(f"FAILED: Permission denied for {project_data['name']} (PTA: {project_data['application_identifier']})")
            return None
        elif response.status_code == 409:
            print(f"⚠ Project '{project_data['name']}' already exists (conflict)")
            logger.warning(f"CONFLICT: Project {project_data['name']} (PTA: {project_data['application_identifier']}) already exists")
            return None
        else:
            error_msg = response.text
            print(f"✗ Failed to create project '{project_data['name']}': HTTP {response.status_code} - {error_msg}")
            logger.error(f"FAILED: HTTP {response.status_code} for {project_data['name']} (PTA: {project_data['application_identifier']}) - {error_msg}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error creating project '{project_data['name']}': {e}")
        logger.error(f"FAILED: Network error for {project_data['name']} (PTA: {project_data['application_identifier']}) - {str(e)}")
        return None
    except Exception as e:
        print(f"✗ Unexpected error creating project '{project_data['name']}': {e}")
        logger.error(f"FAILED: Unexpected error for {project_data['name']} (PTA: {project_data['application_identifier']}) - {str(e)}")
        return None

def setup_logging() -> Tuple[logging.Logger, str]:
    """Set up logging to file with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"project_creation_log_{timestamp}.log"
    json_log_filename = f"created_projects_{timestamp}.json"
    
    # Create logger
    logger = logging.getLogger('project_creation')
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
    logger.info("PROJECT CREATION SESSION STARTED")
    logger.info("=" * 60)
    logger.info(f"Log file: {log_filename}")
    logger.info(f"JSON log file: {json_log_filename}")
    
    return logger, json_log_filename

def main():
    """Main function to read PTA information and create projects."""
    # Set up logging
    logger, json_log_filename = setup_logging()
    
    print("Starting project creation from SNSF PTA Excel file...")
    print(f"API Endpoint: {NEMO_PROJECTS_API_URL}")
    print("-" * 60)
    logger.info(f"API Endpoint: {NEMO_PROJECTS_API_URL}")
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Read the Excel file
    excel_file = "SNSF-Data/Copy of SNSF PTAs for Alex Denton.xlsx"
    df = read_user_information_excel(excel_file)
    
    # Extract unique projects
    unique_projects = extract_unique_projects(df)
    
    if not unique_projects:
        print("No projects found to create!")
        return
    
    # Load existing PTAs from file (or download if file doesn't exist)
    print("\nLoading existing PTAs...")
    existing_ptas = load_existing_ptas()
    
    if not existing_ptas:
        print("⚠ Warning: Could not load existing PTAs from file. Attempting to download from API...")
        existing_ptas = download_existing_projects()
        
        if not existing_ptas:
            print("⚠ Warning: Could not download existing projects or no projects found. Proceeding without duplicate check.")
    
    print("\nFiltering out duplicate projects...")
    filtered_projects = filter_existing_projects(unique_projects, existing_ptas)
    
    if not filtered_projects:
        print("No new projects to create! All projects already exist in NEMO.")
        return
    
    print(f"\n✓ {len(filtered_projects)} new projects to create (filtered out {len(unique_projects) - len(filtered_projects)} duplicates)")
    
    # Load account lookup and rate categories
    print("\nLoading account lookup...")
    account_lookup = load_account_lookup()
    
    if not account_lookup:
        print("Cannot proceed without account lookup. Please run download_accounts.py first.")
        return
    
    print("Loading rate categories...")
    rate_mapping = load_rate_categories()
    
    if not rate_mapping:
        print("Cannot proceed without rate categories. Please run download_rate_categories.py first.")
        return
    
    print("Matching projects to accounts...")
    projects_with_accounts = match_projects_to_accounts(filtered_projects, account_lookup)
    
    print(f"\nReady to create {len(projects_with_accounts)} projects...")
    
    # Create projects via API
    successful_creations = 0
    failed_creations = 0
    created_projects = []  # Store all successfully created projects for JSON log
    
    logger.info(f"Starting to create {len(projects_with_accounts)} projects...")
    
    for i, project_data in enumerate(projects_with_accounts, 1):
        print(f"\n[{i}/{len(projects_with_accounts)}] Creating project: {project_data['name']}")
        print(f"  Application Identifier: {project_data['application_identifier']}")
        
        result = push_project_to_api(project_data, NEMO_PROJECTS_API_URL, rate_mapping, logger)
        if result:
            successful_creations += 1
            created_projects.append(result)
        else:
            failed_creations += 1
        
        # Add a small delay to avoid overwhelming the API
        time.sleep(0.5)
    
    # Save JSON log of all created projects for easy rollback
    if created_projects:
        try:
            with open(json_log_filename, 'w', encoding='utf-8') as f:
                json.dump({
                    'session_timestamp': datetime.now().isoformat(),
                    'total_created': len(created_projects),
                    'created_projects': created_projects
                }, f, indent=2, ensure_ascii=False)
            print(f"\n✓ Saved JSON log of created projects to: {json_log_filename}")
            logger.info(f"Saved JSON log to: {json_log_filename}")
        except Exception as e:
            print(f"⚠ Warning: Could not save JSON log: {e}")
            logger.error(f"Failed to save JSON log: {e}")
    
    # Summary
    print("\n" + "=" * 60)
    print("PROJECT CREATION SUMMARY")
    print("=" * 60)
    print(f"Total projects in Excel: {len(unique_projects)}")
    print(f"Duplicate projects (already exist): {len(unique_projects) - len(filtered_projects)}")
    print(f"New projects to create: {len(filtered_projects)}")
    print(f"Successfully created: {successful_creations}")
    print(f"Failed to create: {failed_creations}")
    if len(filtered_projects) > 0:
        print(f"Success rate: {(successful_creations/len(filtered_projects)*100):.1f}%")
    
    if failed_creations > 0:
        print(f"\nNote: {failed_creations} projects failed to create.")
        print("These may need to be created manually or have their data corrected.")
    
    # Log summary
    logger.info("=" * 60)
    logger.info("PROJECT CREATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total projects in Excel: {len(unique_projects)}")
    logger.info(f"Duplicate projects (already exist): {len(unique_projects) - len(filtered_projects)}")
    logger.info(f"New projects to create: {len(filtered_projects)}")
    logger.info(f"Successfully created: {successful_creations}")
    logger.info(f"Failed to create: {failed_creations}")
    logger.info("=" * 60)
    logger.info("PROJECT CREATION SESSION ENDED")
    logger.info("=" * 60)
    
    print(f"\n✓ Detailed log saved to: project_creation_log_*.log")
    if created_projects:
        print(f"✓ JSON log of created projects saved to: {json_log_filename}")
        print(f"  (Use this file to rollback if needed)")

if __name__ == "__main__":
    main()
