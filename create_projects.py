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
from typing import List, Dict, Any
import time

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
    "name": "",  # Will be filled from 'project' column
    "application_identifier": "",  # Will be filled from 'account' column
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

def load_rate_category_mapping(filename: str = "rate_category_mapping.json") -> Dict[str, int]:
    """Load the rate category mapping from the downloaded rate categories."""
    try:
        with open(filename, 'r') as f:
            mapping = json.load(f)
        print(f"✓ Loaded rate category mapping with {len(mapping)} categories")
        return mapping
    except FileNotFoundError:
        print(f"✗ Rate category mapping file {filename} not found!")
        print("Please run download_rate_categories.py first to download rate categories from NEMO.")
        return {}
    except Exception as e:
        print(f"✗ Error loading rate category mapping: {e}")
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
    """Match projects to accounts based on Account name from the Excel file."""
    matched_projects = []
    unmatched_projects = []
    
    for project in projects:
        account_name = project.get('account_name', '').strip()
        
        if account_name and account_name in account_lookup:
            # Found matching account
            project_with_account = project.copy()
            project_with_account['account_id'] = account_lookup[account_name]
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
    if project_data.get('account_id'):
        payload["account"] = project_data['account_id']
        account_name = project_data.get('account_name', 'Unknown')
        print(f"  → Associated with account ID: {project_data['account_id']} (Account: {account_name})")
    else:
        account_name = project_data.get('account_name', 'Unknown')
        print(f"  ⚠ No account found for: {account_name}")
    
    # Set the rate category based on project_type from Excel
    project_type = project_data.get('project_type', '').strip().lower()
    if project_type and project_type in rate_mapping:
        payload["category"] = rate_mapping[project_type]
        print(f"  → Set rate category ID: {rate_mapping[project_type]} for type '{project_type}'")
    else:
        # Default to Academic if type not found or not in mapping
        for type_name, type_id in rate_mapping.items():
            if "academic" in type_name.lower():
                payload["category"] = type_id
                print(f"  → Defaulted to Academic rate category ID: {type_id}")
                break
        else:
            # If no Academic found, use the first available
            first_id = list(rate_mapping.values())[0] if rate_mapping else 1
            payload["category"] = first_id
            print(f"  → Defaulted to first available rate category ID: {first_id}")
    
    # Note: project_types is a list field in the API, but we're not setting it here
    # as it may require additional mapping or manual configuration
    
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

def push_project_to_api(project_data: Dict[str, str], api_url: str, rate_mapping: Dict[str, int]) -> bool:
    """Push a single project to the NEMO API."""
    payload = create_project_payload(project_data, rate_mapping)
    
    try:
        response = requests.post(api_url, json=payload, headers=API_HEADERS)
        
        if response.status_code == 200:  # Created
            print(f"✓ Successfully created project: {project_data['name']}")
            return True
        elif response.status_code == 400:
            print(f"✗ Bad request for project '{project_data['name']}': {response.text}")
            return False
        elif response.status_code == 401:
            print(f"✗ Authentication failed for project '{project_data['name']}': Check your NEMO_TOKEN")
            return False
        elif response.status_code == 403:
            print(f"✗ Permission denied for project '{project_data['name']}': Check your API permissions")
            return False
        elif response.status_code == 409:
            print(f"⚠ Project '{project_data['name']}' already exists (conflict)")
            return False
        else:
            print(f"✗ Failed to create project '{project_data['name']}': HTTP {response.status_code} - {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error creating project '{project_data['name']}': {e}")
        return False

def main():
    """Main function to read PTA information and create projects."""
    print("Starting project creation from SNSF PTA Excel file...")
    print(f"API Endpoint: {NEMO_PROJECTS_API_URL}")
    print("-" * 60)
    
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
    
    # Load account lookup and rate category mapping
    print("\nLoading account lookup...")
    account_lookup = load_account_lookup()
    
    if not account_lookup:
        print("Cannot proceed without account lookup. Please run download_accounts.py first.")
        return
    
    print("Loading rate category mapping...")
    rate_mapping = load_rate_category_mapping()
    
    if not rate_mapping:
        print("Cannot proceed without rate category mapping. Please run download_rate_categories.py first.")
        return
    
    print("Matching projects to accounts...")
    projects_with_accounts = match_projects_to_accounts(filtered_projects, account_lookup)
    
    print(f"\nReady to create {len(projects_with_accounts)} projects...")
    
    # Create projects via API
    successful_creations = 0
    failed_creations = 0
    
    for i, project_data in enumerate(projects_with_accounts, 1):
        print(f"\n[{i}/{len(projects_with_accounts)}] Creating project: {project_data['name']}")
        print(f"  Application Identifier: {project_data['application_identifier']}")
        
        if push_project_to_api(project_data, NEMO_PROJECTS_API_URL, rate_mapping):
            successful_creations += 1
        else:
            failed_creations += 1
        
        # Add a small delay to avoid overwhelming the API
        time.sleep(0.5)
    
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

if __name__ == "__main__":
    main()
