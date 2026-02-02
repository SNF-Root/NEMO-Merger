#!/usr/bin/env python3
"""
Script to create NEMO departments from SNSF CSV file.
Extracts unique departments from "Department 1" column, uploads them to the API,
then downloads all departments with their IDs.
"""

import pandas as pd
import requests
import json
import os
from dotenv import load_dotenv
from typing import List, Dict, Any, Set, Tuple
import time
import logging
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoint for departments
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
        response = requests.get(NEMO_DEPARTMENTS_API_URL, headers=API_HEADERS)
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

def read_snsf_csv(file_path: str) -> pd.DataFrame:
    """Read the SNSF CSV file and return a DataFrame."""
    try:
        df = pd.read_csv(file_path)
        print(f"✓ Successfully read {file_path}")
        print(f"Shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        print(f"✗ Error reading {file_path}: {e}")
        exit(1)

def extract_unique_departments(df: pd.DataFrame) -> Set[str]:
    """Extract unique departments from the 'Department 1' column."""
    # The column name might have newlines, so we need to find it
    dept_column = None
    for col in df.columns:
        if 'Department 1' in col or 'department 1' in col.lower():
            dept_column = col
            break
    
    if dept_column is None:
        print("✗ Could not find 'Department 1' column in CSV")
        print(f"Available columns: {df.columns.tolist()}")
        exit(1)
    
    print(f"✓ Found department column: '{dept_column}'")
    
    # Extract unique non-null departments
    departments = set()
    for dept in df[dept_column]:
        if pd.notna(dept):
            dept_str = str(dept).strip()
            if dept_str and dept_str.lower() not in ['none', 'null', 'nan', '']:
                departments.add(dept_str)
    
    print(f"✓ Found {len(departments)} unique departments")
    return departments

def download_existing_departments() -> List[Dict[str, Any]]:
    """Download all existing departments from the NEMO API."""
    try:
        print("Downloading existing departments from NEMO API...")
        response = requests.get(NEMO_DEPARTMENTS_API_URL, headers=API_HEADERS)
        
        if response.status_code == 200:
            departments = response.json()
            print(f"✓ Successfully downloaded {len(departments)} departments")
            return departments
        else:
            print(f"✗ Failed to download departments: HTTP {response.status_code} - {response.text}")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error downloading departments: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"✗ Error parsing JSON response: {e}")
        return []

def filter_new_departments(departments: Set[str], existing_departments: List[Dict[str, Any]]) -> Set[str]:
    """Filter out departments that already exist in NEMO."""
    existing_names = set()
    for dept in existing_departments:
        if 'name' in dept:
            existing_names.add(str(dept['name']).strip())
    
    new_departments = departments - existing_names
    
    print(f"✓ Found {len(existing_names)} existing departments")
    print(f"✓ Found {len(new_departments)} new departments to create")
    
    if len(departments) - len(new_departments) > 0:
        print(f"⚠ Skipping {len(departments) - len(new_departments)} departments that already exist")
    
    return new_departments

def create_department_payload(department_name: str, display_order: int) -> Dict[str, Any]:
    """Create a department payload for the API."""
    return {
        "name": department_name,
        "display_order": display_order
    }

def upload_department(department_name: str, display_order: int, logger: logging.Logger) -> Tuple[bool, Dict[str, Any]]:
    """Upload a single department to the NEMO API.
    
    Args:
        department_name: Name of the department
        display_order: Display order for the department
        logger: Logger instance
    
    Returns:
        Tuple of (success: bool, response_data: Dict)
    """
    payload = create_department_payload(department_name, display_order)
    
    try:
        response = requests.post(NEMO_DEPARTMENTS_API_URL, json=payload, headers=API_HEADERS)
        
        if response.status_code == 201:  # Created
            response_data = response.json()
            dept_id = response_data.get('id', 'Unknown')
            print(f"✓ Successfully created department: {department_name} (ID: {dept_id})")
            logger.info(f"SUCCESS - Department '{department_name}' created with ID: {dept_id}")
            logger.debug(f"Department payload: {json.dumps(payload, indent=2)}")
            logger.debug(f"API response: {json.dumps(response_data, indent=2)}")
            return True, response_data
        elif response.status_code == 200:  # OK (sometimes used for creation)
            response_data = response.json()
            dept_id = response_data.get('id', 'Unknown')
            print(f"✓ Successfully created department: {department_name} (ID: {dept_id})")
            logger.info(f"SUCCESS - Department '{department_name}' created with ID: {dept_id}")
            logger.debug(f"Department payload: {json.dumps(payload, indent=2)}")
            logger.debug(f"API response: {json.dumps(response_data, indent=2)}")
            return True, response_data
        elif response.status_code == 400:
            error_msg = response.text
            print(f"✗ Bad request for department '{department_name}': {error_msg}")
            logger.error(f"BAD REQUEST - Department '{department_name}': {error_msg}")
            logger.debug(f"Department payload: {json.dumps(payload, indent=2)}")
            return False, {}
        elif response.status_code == 401:
            error_msg = "Authentication failed: Check your NEMO_TOKEN"
            print(f"✗ Authentication failed for department '{department_name}': Check your NEMO_TOKEN")
            logger.error(f"AUTHENTICATION FAILED - Department '{department_name}': {error_msg}")
            return False, {}
        elif response.status_code == 403:
            error_msg = "Permission denied: Check your API permissions"
            print(f"✗ Permission denied for department '{department_name}': Check your API permissions")
            logger.error(f"PERMISSION DENIED - Department '{department_name}': {error_msg}")
            return False, {}
        elif response.status_code == 409:
            error_msg = "Department already exists (conflict)"
            print(f"⚠ Department '{department_name}' already exists (conflict)")
            logger.warning(f"CONFLICT - Department '{department_name}' already exists")
            return False, {}
        else:
            error_msg = response.text
            print(f"✗ Failed to create department '{department_name}': HTTP {response.status_code} - {error_msg}")
            logger.error(f"FAILED - HTTP {response.status_code} for Department '{department_name}': {error_msg}")
            return False, {}
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error creating department '{department_name}': {e}")
        logger.error(f"NETWORK ERROR - Department '{department_name}': {str(e)}")
        return False, {}
    except Exception as e:
        print(f"✗ Unexpected error creating department '{department_name}': {e}")
        logger.error(f"UNEXPECTED ERROR - Department '{department_name}': {str(e)}")
        return False, {}

def save_departments_to_file(departments: List[Dict[str, Any]], filename: str = "nemo_departments.json"):
    """Save departments to a local JSON file."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(departments, f, indent=2, ensure_ascii=False)
        print(f"✓ Successfully saved {len(departments)} departments to {filename}")
    except Exception as e:
        print(f"✗ Error saving departments to file: {e}")

def create_department_lookup(departments: List[Dict[str, Any]]) -> Dict[str, int]:
    """Create a lookup dictionary mapping department names to their IDs."""
    lookup = {}
    for dept in departments:
        if 'name' in dept and 'id' in dept:
            name = str(dept['name']).strip()
            lookup[name] = dept['id']
    
    print(f"✓ Created department lookup for {len(lookup)} departments")
    return lookup

def setup_logging() -> logging.Logger:
    """Set up logging to file with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"department_creation_log_{timestamp}.log"
    
    # Create logger
    logger = logging.getLogger('department_creation')
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
    logger.info("DEPARTMENT CREATION SESSION STARTED")
    logger.info("=" * 60)
    logger.info(f"Log file: {log_filename}")
    
    return logger

def main():
    """Main function to extract departments, upload them, and download the results."""
    # Set up logging
    logger = setup_logging()
    
    print("Starting department creation from SNSF CSV file...")
    print(f"API Endpoint: {NEMO_DEPARTMENTS_API_URL}")
    print("-" * 60)
    logger.info(f"API Endpoint: {NEMO_DEPARTMENTS_API_URL}")
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Step 1: Read the CSV file
    csv_file = "SNSF-Data/Copy of List_Faculty_SNSF .csv"
    print(f"\nStep 1: Reading CSV file: {csv_file}")
    df = read_snsf_csv(csv_file)
    
    # Step 2: Extract unique departments
    print("\nStep 2: Extracting unique departments...")
    unique_departments = extract_unique_departments(df)
    
    if not unique_departments:
        print("No departments found to create!")
        return
    
    # Step 3: Download existing departments to check for duplicates
    print("\nStep 3: Checking for existing departments...")
    existing_departments = download_existing_departments()
    
    # Step 4: Filter out departments that already exist
    print("\nStep 4: Filtering new departments...")
    new_departments = filter_new_departments(unique_departments, existing_departments)
    
    # Step 5: Upload new departments
    if new_departments:
        print(f"\nStep 5: Uploading {len(new_departments)} new departments...")
        successful_uploads = 0
        failed_uploads = 0
        
        # Calculate starting display_order (number of existing departments + 1)
        starting_display_order = len(existing_departments) + 1
        print(f"Starting display_order at: {starting_display_order}")
        
        for i, dept_name in enumerate(sorted(new_departments), 1):
            display_order = starting_display_order + i - 1
            print(f"\n[{i}/{len(new_departments)}] Creating department: {dept_name} (display_order: {display_order})")
            success, response_data = upload_department(dept_name, display_order, logger)
            if success:
                successful_uploads += 1
            else:
                failed_uploads += 1
            
            # Add a small delay to avoid overwhelming the API
            time.sleep(0.5)
        
        print(f"\n✓ Successfully uploaded {successful_uploads} departments")
        if failed_uploads > 0:
            print(f"⚠ Failed to upload {failed_uploads} departments")
    else:
        print("\n✓ No new departments to upload (all already exist)")
    
    # Step 6: Download all departments (including newly created ones)
    print("\nStep 6: Downloading all departments with their IDs...")
    all_departments = download_existing_departments()
    
    if all_departments:
        # Save to JSON file
        save_departments_to_file(all_departments, "nemo_departments.json")
        
        # Create and save lookup
        department_lookup = create_department_lookup(all_departments)
        with open("department_lookup.json", 'w', encoding='utf-8') as f:
            json.dump(department_lookup, f, indent=2, ensure_ascii=False)
        print("✓ Saved department lookup to department_lookup.json")
        
        # Show sample
        print("\nSample departments:")
        for i, dept in enumerate(all_departments[:10]):
            dept_id = dept.get('id', 'N/A')
            dept_name = dept.get('name', 'N/A')
            print(f"  {i+1}. ID: {dept_id}, Name: {dept_name}")
        
        if len(all_departments) > 10:
            print(f"  ... and {len(all_departments) - 10} more departments")
    else:
        print("⚠ No departments downloaded")
    
    # Summary
    print("\n" + "=" * 60)
    print("DEPARTMENT CREATION SUMMARY")
    print("=" * 60)
    print(f"Total unique departments in CSV: {len(unique_departments)}")
    if new_departments:
        print(f"New departments uploaded: {len(new_departments)}")
    print(f"Total departments in NEMO: {len(all_departments) if all_departments else 0}")
    
    logger.info("=" * 60)
    logger.info("DEPARTMENT CREATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total unique departments in CSV: {len(unique_departments)}")
    if new_departments:
        logger.info(f"New departments uploaded: {len(new_departments)}")
    logger.info(f"Total departments in NEMO: {len(all_departments) if all_departments else 0}")
    logger.info("=" * 60)
    logger.info("DEPARTMENT CREATION SESSION ENDED")
    logger.info("=" * 60)
    
    print(f"\n✓ Detailed log saved to: department_creation_log_*.log")
    print(f"✓ Departments saved to: nemo_departments.json")
    print(f"✓ Department lookup saved to: department_lookup.json")

if __name__ == "__main__":
    main()
