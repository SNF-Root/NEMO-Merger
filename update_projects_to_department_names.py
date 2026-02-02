#!/usr/bin/env python3
"""
Script to update projects from old department IDs (without "Department" suffix)
to new department IDs (with "Department" suffix).

For example: "Chemistry" (ID 9) -> "Chemistry Department" (ID 38)
"""

import requests
import json
import os
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Any, Tuple
from collections import defaultdict

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoints
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

def normalize_department_name(name: str) -> str:
    """Normalize a department name for comparison."""
    if not name:
        return ""
    return str(name).strip().lower()

def create_department_mapping(departments: List[Dict[str, Any]]) -> Dict[int, int]:
    """Create a mapping from old department ID (without "Department") to new department ID (with "Department").
    
    Returns:
        Dictionary mapping old_dept_id -> new_dept_id
    """
    dept_mapping = {}
    
    # Create lookup dictionaries
    dept_by_name = {}
    dept_by_normalized = {}
    
    for dept in departments:
        dept_id = dept.get('id')
        dept_name = dept.get('name', '').strip()
        if dept_id and dept_name:
            dept_by_name[dept_name] = dept_id
            normalized = normalize_department_name(dept_name)
            dept_by_normalized[normalized] = dept_id
    
    # Find pairs where one name is the other + " Department"
    for dept in departments:
        dept_id = dept.get('id')
        dept_name = dept.get('name', '').strip()
        
        if not dept_name or not dept_id:
            continue
        
        # Check if this department name ends with " Department"
        if dept_name.endswith(" Department"):
            # Find the base name (without " Department")
            base_name = dept_name[:-11].strip()  # Remove " Department"
            
            # Look for a department with just the base name
            base_dept_id = dept_by_name.get(base_name)
            if base_dept_id and base_dept_id != dept_id:
                # Found a match: base_name -> dept_name
                dept_mapping[base_dept_id] = dept_id
                print(f"  Found mapping: '{base_name}' (ID {base_dept_id}) -> '{dept_name}' (ID {dept_id})")
    
    print(f"\n✓ Created {len(dept_mapping)} department mappings")
    return dept_mapping

def update_project_department(project_id: int, department_id: int, logger: logging.Logger) -> Tuple[bool, str]:
    """Update a project's department via the NEMO API."""
    update_url = f"{NEMO_PROJECTS_API_URL}{project_id}/"
    payload = {'department': department_id}
    
    try:
        response = requests.patch(update_url, json=payload, headers=API_HEADERS)
        
        if response.status_code == 200:
            logger.info(f"SUCCESS: Updated project ID {project_id} with department={department_id}")
            return True, 'success'
        elif response.status_code == 400:
            error_msg = response.text
            logger.error(f"FAILED: Bad request for project {project_id} - {error_msg}")
            return False, f'bad_request: {error_msg}'
        elif response.status_code == 401:
            logger.error(f"FAILED: Authentication failed for project {project_id}")
            return False, 'auth_failed'
        elif response.status_code == 403:
            logger.error(f"FAILED: Permission denied for project {project_id}")
            return False, 'permission_denied'
        elif response.status_code == 404:
            logger.error(f"FAILED: Project {project_id} not found")
            return False, 'not_found'
        else:
            error_msg = response.text
            logger.error(f"FAILED: HTTP {response.status_code} for project {project_id} - {error_msg}")
            return False, f'error_{response.status_code}: {error_msg}'
            
    except requests.exceptions.RequestException as e:
        logger.error(f"FAILED: Network error updating project {project_id}: {e}")
        return False, f'network_error: {str(e)}'

def setup_logging() -> logging.Logger:
    """Set up logging to file with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"department_name_update_log_{timestamp}.log"
    
    # Create logger
    logger = logging.getLogger('department_name_update')
    logger.setLevel(logging.DEBUG)
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
    
    logger.info("=" * 80)
    logger.info("DEPARTMENT NAME UPDATE SESSION STARTED")
    logger.info("=" * 80)
    logger.info(f"Log file: {log_filename}")
    
    return logger

def main():
    """Main function."""
    print("=" * 80)
    print("UPDATE PROJECTS TO DEPARTMENT NAMES WITH 'DEPARTMENT' SUFFIX")
    print("=" * 80)
    
    # Test API connection
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Step 1: Load or download departments
    print("\n" + "-" * 80)
    print("STEP 1: Loading/Downloading Departments")
    print("-" * 80)
    departments = load_or_download_departments()
    
    if not departments:
        print("No departments found. Cannot proceed.")
        return
    
    # Step 2: Create department mapping (old ID -> new ID)
    print("\n" + "-" * 80)
    print("STEP 2: Creating Department ID Mapping")
    print("-" * 80)
    dept_mapping = create_department_mapping(departments)
    
    if not dept_mapping:
        print("⚠ No department mappings found. Nothing to update.")
        return
    
    # Save mapping
    with open("department_id_mapping.json", 'w', encoding='utf-8') as f:
        json.dump(dept_mapping, f, indent=2, ensure_ascii=False)
    print(f"✓ Saved department ID mapping to department_id_mapping.json")
    
    # Create reverse lookup for display
    dept_id_to_name = {dept.get('id'): dept.get('name', 'Unknown') for dept in departments}
    
    # Step 3: Load or download projects
    print("\n" + "-" * 80)
    print("STEP 3: Loading/Downloading Projects")
    print("-" * 80)
    projects = load_or_download_projects()
    
    if not projects:
        print("No projects found. Cannot proceed.")
        return
    
    # Step 4: Find projects that need updating
    print("\n" + "-" * 80)
    print("STEP 4: Finding Projects to Update")
    print("-" * 80)
    
    projects_to_update = []
    for project in projects:
        current_dept_id = project.get('department')
        if current_dept_id and current_dept_id in dept_mapping:
            new_dept_id = dept_mapping[current_dept_id]
            projects_to_update.append({
                'project': project,
                'old_dept_id': current_dept_id,
                'new_dept_id': new_dept_id,
                'old_dept_name': dept_id_to_name.get(current_dept_id, 'Unknown'),
                'new_dept_name': dept_id_to_name.get(new_dept_id, 'Unknown')
            })
    
    if not projects_to_update:
        print("✓ No projects need updating. All projects already use departments with 'Department' suffix.")
        return
    
    print(f"✓ Found {len(projects_to_update)} projects to update")
    
    # Group by department for better display
    dept_groups = defaultdict(list)
    for item in projects_to_update:
        dept_groups[item['old_dept_id']].append(item)
    
    print(f"\nProjects grouped by department:")
    for old_dept_id, items in sorted(dept_groups.items()):
        old_name = items[0]['old_dept_name']
        new_name = items[0]['new_dept_name']
        print(f"  {old_name} (ID {old_dept_id}) -> {new_name} (ID {items[0]['new_dept_id']}): {len(items)} projects")
    
    # Step 5: Update projects
    print("\n" + "-" * 80)
    print("STEP 5: Updating Projects")
    print("-" * 80)
    
    # Set up logging
    logger = setup_logging()
    
    # Confirm before updating
    print("\n⚠ WARNING: This will update projects in NEMO!")
    print(f"About to update {len(projects_to_update)} projects.")
    response = input("Do you want to proceed? (yes/no): ")
    
    if response.lower() not in ['yes', 'y']:
        print("Cancelled. No projects were updated.")
        return
    
    # Update projects
    successful = 0
    failed = 0
    
    print("\nUpdating projects...")
    
    for i, item in enumerate(projects_to_update, 1):
        project = item['project']
        project_id = project.get('id')
        project_name = project.get('name', 'Unknown')
        old_dept_id = item['old_dept_id']
        new_dept_id = item['new_dept_id']
        old_dept_name = item['old_dept_name']
        new_dept_name = item['new_dept_name']
        
        print(f"\n[{i}/{len(projects_to_update)}] Project {project_id}: {project_name}")
        print(f"  Updating: {old_dept_name} (ID {old_dept_id}) -> {new_dept_name} (ID {new_dept_id})")
        
        success, status = update_project_department(project_id, new_dept_id, logger)
        
        if success:
            successful += 1
            print(f"  ✓ Updated successfully")
        else:
            failed += 1
            print(f"  ✗ Failed: {status}")
        
        # Small delay to avoid overwhelming API
        time.sleep(0.3)
    
    logger.info("=" * 80)
    logger.info("DEPARTMENT NAME UPDATE SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total projects to update: {len(projects_to_update)}")
    logger.info(f"Successfully updated: {successful}")
    logger.info(f"Failed: {failed}")
    logger.info("=" * 80)
    logger.info("DEPARTMENT NAME UPDATE SESSION ENDED")
    logger.info("=" * 80)
    
    # Summary
    print(f"\n{'='*80}")
    print("UPDATE SUMMARY")
    print(f"{'='*80}")
    print(f"Total projects to update: {len(projects_to_update)}")
    print(f"Successfully updated: {successful}")
    print(f"Failed: {failed}")
    if len(projects_to_update) > 0:
        print(f"Success rate: {(successful/len(projects_to_update)*100):.1f}%")
    print(f"\n✓ Detailed log saved to: department_name_update_log_*.log")
    print(f"✓ Department ID mapping saved to: department_id_mapping.json")

if __name__ == "__main__":
    main()
