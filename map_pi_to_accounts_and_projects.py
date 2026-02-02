#!/usr/bin/env python3
"""
Script to:
1. Map PI Names from SNSF CSV to NEMO accounts
2. Download all projects from NEMO
3. Create Department -> Account -> Project mapping
4. Verify that projects with the same account have the same department
"""

import pandas as pd
import requests
import json
import os
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Any, Set, Tuple
from collections import defaultdict
from difflib import SequenceMatcher

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoints
NEMO_ACCOUNTS_API_URL = "https://nemo.stanford.edu/api/accounts/"
NEMO_PROJECTS_API_URL = "https://nemo.stanford.edu/api/projects/"
NEMO_DEPARTMENTS_API_URL = "https://nemo.stanford.edu/api/billing/departments/"

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
        response = requests.get(NEMO_ACCOUNTS_API_URL, headers=API_HEADERS)
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

def load_or_download_accounts() -> List[Dict[str, Any]]:
    """Load accounts from file or download from API."""
    # Try to load from file first
    if os.path.exists("nemo_accounts.json"):
        try:
            with open("nemo_accounts.json", 'r', encoding='utf-8') as f:
                accounts = json.load(f)
            print(f"✓ Loaded {len(accounts)} accounts from nemo_accounts.json")
            return accounts
        except Exception as e:
            print(f"⚠ Error loading accounts from file: {e}")
            print("Downloading from API...")
    
    # Download from API
    try:
        print("Downloading accounts from NEMO API...")
        response = requests.get(NEMO_ACCOUNTS_API_URL, headers=API_HEADERS)
        
        if response.status_code == 200:
            accounts = response.json()
            print(f"✓ Successfully downloaded {len(accounts)} accounts")
            # Save to file
            with open("nemo_accounts.json", 'w', encoding='utf-8') as f:
                json.dump(accounts, f, indent=2, ensure_ascii=False)
            print(f"✓ Saved accounts to nemo_accounts.json")
            return accounts
        else:
            print(f"✗ Failed to download accounts: HTTP {response.status_code} - {response.text}")
            return []
    except Exception as e:
        print(f"✗ Error downloading accounts: {e}")
        return []

def load_or_download_projects() -> List[Dict[str, Any]]:
    """Load projects from file or download from API."""
    # Try to load from file first
    if os.path.exists("nemo_projects.json"):
        try:
            with open("nemo_projects.json", 'r', encoding='utf-8') as f:
                projects = json.load(f)
            print(f"✓ Loaded {len(projects)} projects from nemo_projects.json")
            return projects
        except Exception as e:
            print(f"⚠ Error loading projects from file: {e}")
            print("Downloading from API...")
    
    # Download from API
    try:
        print("Downloading projects from NEMO API...")
        response = requests.get(NEMO_PROJECTS_API_URL, headers=API_HEADERS)
        
        if response.status_code == 200:
            projects = response.json()
            print(f"✓ Successfully downloaded {len(projects)} projects")
            # Save to file
            with open("nemo_projects.json", 'w', encoding='utf-8') as f:
                json.dump(projects, f, indent=2, ensure_ascii=False)
            print(f"✓ Saved projects to nemo_projects.json")
            return projects
        else:
            print(f"✗ Failed to download projects: HTTP {response.status_code} - {response.text}")
            return []
    except Exception as e:
        print(f"✗ Error downloading projects: {e}")
        return []

def load_or_download_departments() -> List[Dict[str, Any]]:
    """Load departments from file or download from API."""
    # Try to load from file first
    if os.path.exists("nemo_departments.json"):
        try:
            with open("nemo_departments.json", 'r', encoding='utf-8') as f:
                departments = json.load(f)
            print(f"✓ Loaded {len(departments)} departments from nemo_departments.json")
            return departments
        except Exception as e:
            print(f"⚠ Error loading departments from file: {e}")
            print("Downloading from API...")
    
    # Download from API
    try:
        print("Downloading departments from NEMO API...")
        response = requests.get(NEMO_DEPARTMENTS_API_URL, headers=API_HEADERS)
        
        if response.status_code == 200:
            departments = response.json()
            print(f"✓ Successfully downloaded {len(departments)} departments")
            # Save to file
            with open("nemo_departments.json", 'w', encoding='utf-8') as f:
                json.dump(departments, f, indent=2, ensure_ascii=False)
            print(f"✓ Saved departments to nemo_departments.json")
            return departments
        else:
            print(f"✗ Failed to download departments: HTTP {response.status_code} - {response.text}")
            return []
    except Exception as e:
        print(f"✗ Error downloading departments: {e}")
        return []

def normalize_name(name: str) -> str:
    """Normalize a name for comparison."""
    if not name or pd.isna(name):
        return ""
    return str(name).strip().lower()

def find_best_match(pi_name: str, account_names: List[str], threshold: float = 0.8) -> Tuple[str, float]:
    """Find the best matching account name for a PI name."""
    normalized_pi = normalize_name(pi_name)
    best_match = None
    best_score = 0.0
    
    for account_name in account_names:
        normalized_account = normalize_name(account_name)
        if normalized_pi == normalized_account:
            return account_name, 1.0
        
        # Calculate similarity
        score = SequenceMatcher(None, normalized_pi, normalized_account).ratio()
        if score > best_score:
            best_score = score
            best_match = account_name
    
    if best_score >= threshold:
        return best_match, best_score
    return None, best_score

def read_pi_names_from_csv(file_path: str) -> List[Dict[str, str]]:
    """Read PI Names and Departments from the SNSF CSV file."""
    try:
        df = pd.read_csv(file_path)
        print(f"✓ Successfully read {file_path}")
        print(f"Shape: {df.shape}")
        
        # Find PI Name column
        pi_column = None
        for col in df.columns:
            if 'PI Name' in col or 'pi name' in col.lower():
                pi_column = col
                break
        
        if pi_column is None:
            print("✗ Could not find 'PI Name' column in CSV")
            print(f"Available columns: {df.columns.tolist()}")
            return []
        
        print(f"✓ Found PI Name column: '{pi_column}'")
        
        # Find Department 1 column
        dept_column = None
        for col in df.columns:
            if 'Department 1' in col or 'department 1' in col.lower():
                dept_column = col
                break
        
        if dept_column is None:
            print("⚠ Could not find 'Department 1' column in CSV")
        else:
            print(f"✓ Found Department column: '{dept_column}'")
        
        # Extract unique PI names with departments
        pi_names = []
        seen_pis = set()
        
        for _, row in df.iterrows():
            pi_name = row[pi_column]
            if pd.notna(pi_name):
                pi_name_str = str(pi_name).strip()
                if pi_name_str and pi_name_str.lower() not in ['none', 'null', 'nan', '']:
                    # Get department if available
                    department = None
                    if dept_column and pd.notna(row.get(dept_column)):
                        dept_str = str(row[dept_column]).strip()
                        if dept_str and dept_str.lower() not in ['none', 'null', 'nan', '']:
                            department = dept_str
                    
                    if pi_name_str not in seen_pis:
                        seen_pis.add(pi_name_str)
                        pi_names.append({
                            'pi_name': pi_name_str,
                            'email': str(row.get('Email', '')).strip() if pd.notna(row.get('Email')) else '',
                            'department': department
                        })
                    else:
                        # If PI already seen, update department if this one is not None
                        for pi_info in pi_names:
                            if pi_info['pi_name'] == pi_name_str and not pi_info.get('department') and department:
                                pi_info['department'] = department
                                break
        
        print(f"✓ Found {len(pi_names)} unique PI names")
        pis_with_dept = sum(1 for pi in pi_names if pi.get('department'))
        print(f"✓ Found {pis_with_dept} PIs with department information")
        return pi_names
    except Exception as e:
        print(f"✗ Error reading CSV file: {e}")
        return []

def create_pi_to_account_mapping(pi_names: List[Dict[str, str]], accounts: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Create a mapping from PI names to accounts."""
    account_names = [acc.get('name', '') for acc in accounts if acc.get('name')]
    account_lookup = {acc.get('name', ''): acc for acc in accounts if acc.get('name')}
    
    pi_to_account = {}
    exact_matches = 0
    fuzzy_matches = 0
    no_matches = 0
    
    print("\nMatching PI names to accounts...")
    
    for pi_info in pi_names:
        pi_name = pi_info['pi_name']
        pi_email = pi_info.get('email', '')
        pi_department = pi_info.get('department')
        
        # Try exact match first
        matched_account = None
        match_type = None
        
        # Check exact match
        if pi_name in account_lookup:
            matched_account = account_lookup[pi_name]
            match_type = 'exact'
            exact_matches += 1
        else:
            # Try fuzzy match
            best_match, score = find_best_match(pi_name, account_names, threshold=0.85)
            if best_match:
                matched_account = account_lookup[best_match]
                match_type = f'fuzzy ({score:.1%})'
                fuzzy_matches += 1
            else:
                no_matches += 1
        
        pi_to_account[pi_name] = {
            'pi_name': pi_name,
            'pi_email': pi_email,
            'department': pi_department,
            'account': matched_account,
            'match_type': match_type if matched_account else 'no_match',
            'account_id': matched_account.get('id') if matched_account else None,
            'account_name': matched_account.get('name') if matched_account else None
        }
    
    print(f"✓ Exact matches: {exact_matches}")
    print(f"✓ Fuzzy matches: {fuzzy_matches}")
    print(f"✗ No matches: {no_matches}")
    
    return pi_to_account

def create_department_name_to_id_mapping(departments: List[Dict[str, Any]]) -> Dict[str, int]:
    """Create a mapping from department names to department IDs."""
    dept_mapping = {}
    for dept in departments:
        dept_name = dept.get('name', '').strip()
        dept_id = dept.get('id')
        if dept_name and dept_id:
            dept_mapping[dept_name] = dept_id
            # Also add normalized version (case-insensitive)
            dept_mapping[dept_name.lower()] = dept_id
    
    print(f"✓ Created department name to ID mapping for {len(set(dept_mapping.values()))} departments")
    return dept_mapping

def create_account_to_department_mapping(pi_to_account: Dict[str, Dict[str, Any]], dept_name_to_id: Dict[str, int]) -> Dict[int, int]:
    """Create a mapping from account IDs to department IDs."""
    account_to_dept = {}
    matched = 0
    unmatched = 0
    
    print("\nMapping accounts to departments...")
    
    for pi_name, pi_info in pi_to_account.items():
        account_id = pi_info.get('account_id')
        dept_name = pi_info.get('department')
        
        if account_id and dept_name:
            # Try exact match first
            dept_id = dept_name_to_id.get(dept_name)
            if not dept_id:
                # Try case-insensitive match
                dept_id = dept_name_to_id.get(dept_name.lower())
            
            if dept_id:
                account_to_dept[account_id] = dept_id
                matched += 1
            else:
                unmatched += 1
                print(f"  ⚠ Could not find department ID for: '{dept_name}' (Account: {pi_info.get('account_name')})")
    
    print(f"✓ Matched {matched} accounts to departments")
    if unmatched > 0:
        print(f"⚠ Could not match {unmatched} accounts to departments")
    
    return account_to_dept

def update_project_department(project_id: int, department_id: int, logger=None) -> Tuple[bool, str]:
    """Update a project's department via the NEMO API."""
    update_url = f"{NEMO_PROJECTS_API_URL}{project_id}/"
    payload = {'department': department_id}
    
    try:
        response = requests.patch(update_url, json=payload, headers=API_HEADERS)
        
        if response.status_code == 200:
            if logger:
                logger.info(f"SUCCESS: Updated project ID {project_id} with department={department_id}")
            return True, 'success'
        elif response.status_code == 400:
            error_msg = response.text
            if logger:
                logger.error(f"FAILED: Bad request for project {project_id} - {error_msg}")
            return False, f'bad_request: {error_msg}'
        elif response.status_code == 401:
            if logger:
                logger.error(f"FAILED: Authentication failed for project {project_id}")
            return False, 'auth_failed'
        elif response.status_code == 403:
            if logger:
                logger.error(f"FAILED: Permission denied for project {project_id}")
            return False, 'permission_denied'
        elif response.status_code == 404:
            if logger:
                logger.error(f"FAILED: Project {project_id} not found")
            return False, 'not_found'
        else:
            error_msg = response.text
            if logger:
                logger.error(f"FAILED: HTTP {response.status_code} for project {project_id} - {error_msg}")
            return False, f'error_{response.status_code}: {error_msg}'
            
    except requests.exceptions.RequestException as e:
        if logger:
            logger.error(f"FAILED: Network error updating project {project_id}: {e}")
        return False, f'network_error: {str(e)}'

def create_department_account_project_mapping(projects: List[Dict[str, Any]], departments: List[Dict[str, Any]]) -> Dict[int, Dict[int, List[Dict[str, Any]]]]:
    """Create a mapping: Department ID -> Account ID -> List of Projects."""
    dept_lookup = {dept.get('id'): dept.get('name', 'Unknown') for dept in departments}
    
    mapping = defaultdict(lambda: defaultdict(list))
    
    for project in projects:
        dept_id = project.get('department')
        account_id = project.get('account')
        
        if dept_id is not None and account_id is not None:
            mapping[dept_id][account_id].append(project)
    
    print(f"✓ Created mapping for {len(mapping)} departments")
    print(f"✓ Total account-department pairs: {sum(len(accounts) for accounts in mapping.values())}")
    
    return dict(mapping)

def verify_account_department_consistency(projects: List[Dict[str, Any]]) -> Tuple[Dict[int, Set[int]], List[Dict[str, Any]]]:
    """Verify that projects with the same account have the same department.
    
    Returns:
        Tuple of (account_to_departments mapping, inconsistencies list)
    """
    account_to_departments = defaultdict(set)
    inconsistencies = []
    
    for project in projects:
        account_id = project.get('account')
        dept_id = project.get('department')
        
        if account_id is not None:
            if dept_id is not None:
                account_to_departments[account_id].add(dept_id)
            else:
                # Project has account but no department
                inconsistencies.append({
                    'project_id': project.get('id'),
                    'project_name': project.get('name', 'Unknown'),
                    'account_id': account_id,
                    'issue': 'Project has account but no department'
                })
    
    # Find accounts with multiple departments
    for account_id, dept_ids in account_to_departments.items():
        if len(dept_ids) > 1:
            # Find projects for this account
            account_projects = [p for p in projects if p.get('account') == account_id]
            for project in account_projects:
                inconsistencies.append({
                    'project_id': project.get('id'),
                    'project_name': project.get('name', 'Unknown'),
                    'account_id': account_id,
                    'account_departments': list(dept_ids),
                    'project_department': project.get('department'),
                    'issue': f'Account has multiple departments: {list(dept_ids)}'
                })
    
    return dict(account_to_departments), inconsistencies

def main():
    """Main function."""
    print("=" * 80)
    print("PI TO ACCOUNT MAPPING AND PROJECT ANALYSIS")
    print("=" * 80)
    
    # Test API connection
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Step 1: Read PI Names from CSV
    print("\n" + "-" * 80)
    print("STEP 1: Reading PI Names from CSV")
    print("-" * 80)
    csv_file = "SNSF-Data/Copy of List_Faculty_SNSF .csv"
    pi_names = read_pi_names_from_csv(csv_file)
    
    if not pi_names:
        print("No PI names found. Cannot proceed.")
        return
    
    # Step 2: Load or download accounts
    print("\n" + "-" * 80)
    print("STEP 2: Loading/Downloading Accounts")
    print("-" * 80)
    accounts = load_or_download_accounts()
    
    if not accounts:
        print("No accounts found. Cannot proceed.")
        return
    
    # Step 3: Create PI to Account mapping
    print("\n" + "-" * 80)
    print("STEP 3: Creating PI to Account Mapping")
    print("-" * 80)
    pi_to_account = create_pi_to_account_mapping(pi_names, accounts)
    
    # Save PI to Account mapping (convert to serializable format)
    pi_to_account_serializable = {}
    for pi_name, pi_info in pi_to_account.items():
        pi_to_account_serializable[pi_name] = {
            'pi_name': pi_info['pi_name'],
            'pi_email': pi_info.get('pi_email', ''),
            'department': pi_info.get('department'),
            'match_type': pi_info.get('match_type'),
            'account_id': pi_info.get('account_id'),
            'account_name': pi_info.get('account_name')
        }
    
    with open("pi_to_account_mapping.json", 'w', encoding='utf-8') as f:
        json.dump(pi_to_account_serializable, f, indent=2, ensure_ascii=False)
    print(f"\n✓ Saved PI to Account mapping to pi_to_account_mapping.json")
    
    # Step 4: Load or download projects
    print("\n" + "-" * 80)
    print("STEP 4: Loading/Downloading Projects")
    print("-" * 80)
    projects = load_or_download_projects()
    
    if not projects:
        print("No projects found.")
    else:
        print(f"✓ Found {len(projects)} projects")
    
    # Step 5: Load or download departments
    print("\n" + "-" * 80)
    print("STEP 5: Loading/Downloading Departments")
    print("-" * 80)
    departments = load_or_download_departments()
    
    if not departments:
        print("No departments found.")
    else:
        print(f"✓ Found {len(departments)} departments")
    
    # Step 6: Create Department -> Account -> Project mapping
    if projects and departments:
        print("\n" + "-" * 80)
        print("STEP 6: Creating Department -> Account -> Project Mapping")
        print("-" * 80)
        dept_account_project = create_department_account_project_mapping(projects, departments)
        
        # Save mapping
        # Convert to serializable format
        mapping_serializable = {}
        for dept_id, accounts_dict in dept_account_project.items():
            mapping_serializable[dept_id] = {}
            for account_id, project_list in accounts_dict.items():
                mapping_serializable[dept_id][account_id] = [
                    {
                        'id': p.get('id'),
                        'name': p.get('name'),
                        'application_identifier': p.get('application_identifier')
                    }
                    for p in project_list
                ]
        
        with open("department_account_project_mapping.json", 'w', encoding='utf-8') as f:
            json.dump(mapping_serializable, f, indent=2, ensure_ascii=False)
        print(f"✓ Saved Department -> Account -> Project mapping to department_account_project_mapping.json")
        
        # Step 7: Verify account-department consistency
        print("\n" + "-" * 80)
        print("STEP 7: Verifying Account-Department Consistency")
        print("-" * 80)
        account_to_departments, inconsistencies = verify_account_department_consistency(projects)
        
        if inconsistencies:
            print(f"\n⚠ Found {len(inconsistencies)} inconsistencies:")
            print("\nProjects with account-department issues:")
            for issue in inconsistencies[:20]:  # Show first 20
                print(f"  Project ID {issue['project_id']}: {issue['project_name']}")
                print(f"    Account ID: {issue['account_id']}")
                print(f"    Issue: {issue['issue']}")
                if 'account_departments' in issue:
                    print(f"    Account has departments: {issue['account_departments']}")
                    print(f"    Project department: {issue['project_department']}")
                print()
            
            if len(inconsistencies) > 20:
                print(f"  ... and {len(inconsistencies) - 20} more inconsistencies")
            
            # Save inconsistencies
            with open("account_department_inconsistencies.json", 'w', encoding='utf-8') as f:
                json.dump(inconsistencies, f, indent=2, ensure_ascii=False)
            print(f"✓ Saved inconsistencies to account_department_inconsistencies.json")
        else:
            print("\n✓ All projects with the same account have the same department!")
        
        # Step 8: Map departments from CSV to accounts and update projects
        if departments and projects:
            print("\n" + "-" * 80)
            print("STEP 8: Mapping Departments to Accounts and Updating Projects")
            print("-" * 80)
            
            # Create department name to ID mapping
            dept_name_to_id = create_department_name_to_id_mapping(departments)
            
            # Create account to department mapping
            account_to_dept = create_account_to_department_mapping(pi_to_account, dept_name_to_id)
            
            if not account_to_dept:
                print("⚠ No account-to-department mappings found. Cannot update projects.")
            else:
                # Save account to department mapping
                with open("account_to_department_mapping.json", 'w', encoding='utf-8') as f:
                    json.dump(account_to_dept, f, indent=2, ensure_ascii=False)
                print(f"✓ Saved account to department mapping to account_to_department_mapping.json")
                
                # Set up logging
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                log_filename = f"project_department_update_log_{timestamp}.log"
                logger = logging.getLogger('project_department_update')
                logger.setLevel(logging.DEBUG)
                logger.handlers = []
                file_handler = logging.FileHandler(log_filename, encoding='utf-8')
                file_handler.setLevel(logging.DEBUG)
                console_handler = logging.StreamHandler()
                console_handler.setLevel(logging.INFO)
                formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
                file_handler.setFormatter(formatter)
                console_handler.setFormatter(formatter)
                logger.addHandler(file_handler)
                logger.addHandler(console_handler)
                
                # Group projects by account
                account_projects = defaultdict(list)
                for project in projects:
                    account_id = project.get('account')
                    if account_id and account_id in account_to_dept:
                        account_projects[account_id].append(project)
                
                print(f"\nFound {len(account_projects)} accounts with department mappings")
                print(f"Total projects to update: {sum(len(projs) for projs in account_projects.values())}")
                
                # Confirm before updating
                print("\n⚠ WARNING: This will update projects in NEMO!")
                response = input("Do you want to proceed with updating projects? (yes/no): ")
                
                if response.lower() not in ['yes', 'y']:
                    print("Cancelled. No projects were updated.")
                else:
                    # Update projects
                    successful = 0
                    failed = 0
                    skipped = 0
                    
                    print("\nUpdating projects...")
                    logger.info("=" * 80)
                    logger.info("PROJECT DEPARTMENT UPDATE SESSION STARTED")
                    logger.info("=" * 80)
                    
                    for account_id, account_projs in account_projects.items():
                        dept_id = account_to_dept[account_id]
                        account_name = next((acc.get('name') for acc in accounts if acc.get('id') == account_id), 'Unknown')
                        dept_name = next((dept.get('name') for dept in departments if dept.get('id') == dept_id), 'Unknown')
                        
                        print(f"\nAccount: {account_name} (ID: {account_id}) -> Department: {dept_name} (ID: {dept_id})")
                        print(f"  Updating {len(account_projs)} projects...")
                        
                        for i, project in enumerate(account_projs, 1):
                            project_id = project.get('id')
                            project_name = project.get('name', 'Unknown')
                            current_dept = project.get('department')
                            
                            # Skip if already has the correct department
                            if current_dept == dept_id:
                                skipped += 1
                                continue
                            
                            print(f"  [{i}/{len(account_projs)}] Project {project_id}: {project_name}")
                            success, status = update_project_department(project_id, dept_id, logger)
                            
                            if success:
                                successful += 1
                                print(f"    ✓ Updated")
                            else:
                                failed += 1
                                print(f"    ✗ Failed: {status}")
                            
                            # Small delay to avoid overwhelming API
                            time.sleep(0.3)
                    
                    logger.info("=" * 80)
                    logger.info("PROJECT DEPARTMENT UPDATE SUMMARY")
                    logger.info("=" * 80)
                    logger.info(f"Successfully updated: {successful}")
                    logger.info(f"Failed: {failed}")
                    logger.info(f"Skipped (already correct): {skipped}")
                    logger.info("=" * 80)
                    logger.info("PROJECT DEPARTMENT UPDATE SESSION ENDED")
                    logger.info("=" * 80)
                    
                    print(f"\n{'='*80}")
                    print("UPDATE SUMMARY")
                    print(f"{'='*80}")
                    print(f"Successfully updated: {successful}")
                    print(f"Failed: {failed}")
                    print(f"Skipped (already correct): {skipped}")
                    print(f"\n✓ Detailed log saved to: {log_filename}")
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"PI Names processed: {len(pi_names)}")
    print(f"Accounts loaded: {len(accounts)}")
    print(f"Projects loaded: {len(projects) if projects else 0}")
    print(f"Departments loaded: {len(departments) if departments else 0}")
    print(f"\n✓ PI to Account mapping saved to: pi_to_account_mapping.json")
    if projects and departments:
        print(f"✓ Department -> Account -> Project mapping saved to: department_account_project_mapping.json")
        if inconsistencies:
            print(f"⚠ Found {len(inconsistencies)} account-department inconsistencies")
            print(f"  Saved to: account_department_inconsistencies.json")

if __name__ == "__main__":
    main()
