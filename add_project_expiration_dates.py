#!/usr/bin/env python3
"""
Script to add expiration dates to projects from the Internal User Tracking Excel file.
Downloads projects from the API, matches them by PTA (application_identifier) to the Excel file,
and updates the expires_on field with the PTA End Date.
"""

import pandas as pd
import requests
import json
import os
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
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

def download_projects() -> List[Dict[str, Any]]:
    """Download all projects from the NEMO API."""
    try:
        print("Downloading projects from NEMO API...")
        response = requests.get(NEMO_PROJECTS_API_URL, headers=API_HEADERS)
        
        if response.status_code == 200:
            projects = response.json()
            print(f"✓ Successfully downloaded {len(projects)} projects")
            return projects
        else:
            print(f"✗ Failed to download projects: HTTP {response.status_code} - {response.text}")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error downloading projects: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"✗ Error parsing JSON response: {e}")
        return []

def read_excel_file(file_path: str) -> pd.DataFrame:
    """Read the Internal User Tracking Excel file and return a DataFrame."""
    try:
        df = pd.read_excel(file_path)
        print(f"✓ Successfully read {file_path}")
        print(f"  Shape: {df.shape}")
        print(f"  Columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        print(f"✗ Error reading {file_path}: {e}")
        exit(1)

def create_pta_to_end_date_mapping(df: pd.DataFrame) -> Dict[str, str]:
    """Create a mapping from PTA to PTA End Date from the Excel file.
    
    Returns a dictionary mapping PTA (string) to end date (ISO format string).
    If multiple rows have the same PTA, uses the latest end date.
    """
    mapping = {}
    
    # Filter out rows where PTA or PTA End Date is NaN
    df_filtered = df.dropna(subset=['PTA', 'PTA End Date'])
    
    for _, row in df_filtered.iterrows():
        pta = str(row['PTA']).strip()
        end_date = row['PTA End Date']
        
        # Skip if PTA is empty or invalid
        if not pta or pta.lower() in ['none', 'null', 'nan']:
            continue
        
        # Convert date to ISO format string (YYYY-MM-DD)
        if pd.isna(end_date):
            continue
        
        # Handle datetime objects
        if isinstance(end_date, pd.Timestamp):
            end_date_str = end_date.strftime('%Y-%m-%d')
        elif isinstance(end_date, datetime):
            end_date_str = end_date.strftime('%Y-%m-%d')
        else:
            # Try to parse as string
            try:
                end_date_str = pd.to_datetime(end_date).strftime('%Y-%m-%d')
            except:
                print(f"⚠ Warning: Could not parse end date for PTA {pta}: {end_date}")
                continue
        
        # If PTA already exists, keep the latest date
        if pta in mapping:
            if end_date_str > mapping[pta]:
                mapping[pta] = end_date_str
        else:
            mapping[pta] = end_date_str
    
    print(f"✓ Created PTA to end date mapping for {len(mapping)} PTAs")
    return mapping

def match_projects_to_end_dates(projects: List[Dict[str, Any]], pta_to_end_date: Dict[str, str]) -> List[Dict[str, Any]]:
    """Match projects to their end dates based on application_identifier (PTA).
    
    Returns a list of dictionaries with project_id, pta, end_date, and project info.
    """
    matched_projects = []
    unmatched_ptas = set()
    
    for project in projects:
        # Get PTA from application_identifier field
        pta = None
        if 'application_identifier' in project and project['application_identifier']:
            pta = str(project['application_identifier']).strip()
        elif 'PTA' in project and project['PTA']:
            pta = str(project['PTA']).strip()
        elif 'pta' in project and project['pta']:
            pta = str(project['pta']).strip()
        
        if not pta or pta.lower() in ['none', 'null']:
            continue
        
        # Check if we have an end date for this PTA
        if pta in pta_to_end_date:
            matched_projects.append({
                'project_id': project.get('id'),
                'pta': pta,
                'end_date': pta_to_end_date[pta],
                'project_name': project.get('name', 'Unknown'),
                'current_expires_on': project.get('expires_on')
            })
        else:
            unmatched_ptas.add(pta)
    
    if unmatched_ptas:
        print(f"⚠ Found {len(unmatched_ptas)} projects with PTAs not in Excel file")
        if len(unmatched_ptas) <= 10:
            for pta in sorted(unmatched_ptas):
                print(f"  - {pta}")
        else:
            for pta in sorted(list(unmatched_ptas))[:10]:
                print(f"  - {pta}")
            print(f"  ... and {len(unmatched_ptas) - 10} more")
    
    print(f"✓ Matched {len(matched_projects)} projects to end dates")
    return matched_projects

def update_project_expiration(project_id: int, end_date: str, logger: logging.Logger) -> bool:
    """Update a project's expiration date via the NEMO API.
    
    Returns True if successful, False otherwise.
    """
    update_url = f"{NEMO_PROJECTS_API_URL}{project_id}/"
    payload = {'expires_on': end_date}
    
    try:
        response = requests.patch(update_url, json=payload, headers=API_HEADERS)
        
        if response.status_code == 200:
            logger.info(f"SUCCESS: Updated project ID {project_id} with expires_on={end_date}")
            return True
        elif response.status_code == 400:
            error_msg = response.text
            print(f"✗ Bad request for project {project_id}: {error_msg}")
            logger.error(f"FAILED: Bad request for project {project_id} - {error_msg}")
            logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
            return False
        elif response.status_code == 401:
            print(f"✗ Authentication failed for project {project_id}: Check your NEMO_TOKEN")
            logger.error(f"FAILED: Authentication failed for project {project_id}")
            return False
        elif response.status_code == 403:
            print(f"✗ Permission denied for project {project_id}: Check your API permissions")
            logger.error(f"FAILED: Permission denied for project {project_id}")
            return False
        elif response.status_code == 404:
            print(f"✗ Project {project_id} not found")
            logger.error(f"FAILED: Project {project_id} not found")
            return False
        else:
            error_msg = response.text
            print(f"✗ Failed to update project {project_id}: HTTP {response.status_code} - {error_msg}")
            logger.error(f"FAILED: HTTP {response.status_code} for project {project_id} - {error_msg}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error updating project {project_id}: {e}")
        logger.error(f"FAILED: Network error for project {project_id} - {str(e)}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error updating project {project_id}: {e}")
        logger.error(f"FAILED: Unexpected error for project {project_id} - {str(e)}")
        return False

def setup_logging() -> logging.Logger:
    """Set up logging to file with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"logs/add_project_expiration_{timestamp}.log"
    
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Create logger
    logger = logging.getLogger('project_expiration')
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
    logger.info("PROJECT EXPIRATION DATE UPDATE SESSION STARTED")
    logger.info("=" * 60)
    logger.info(f"Log file: {log_filename}")
    
    return logger

def main():
    """Main function to add expiration dates to projects."""
    # Set up logging
    logger = setup_logging()
    
    print("Starting project expiration date update...")
    print(f"API Endpoint: {NEMO_PROJECTS_API_URL}")
    print("-" * 60)
    logger.info(f"API Endpoint: {NEMO_PROJECTS_API_URL}")
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Download projects from API
    projects = download_projects()
    if not projects:
        print("No projects downloaded. Cannot proceed.")
        return
    
    # Read Excel file
    excel_file = "SNSF-Data/Internal User Tracking and Emails.xlsx"
    print(f"\nReading Excel file: {excel_file}")
    df = read_excel_file(excel_file)
    
    # Create PTA to end date mapping
    print("\nCreating PTA to end date mapping...")
    pta_to_end_date = create_pta_to_end_date_mapping(df)
    
    if not pta_to_end_date:
        print("No PTA to end date mappings found. Cannot proceed.")
        return
    
    # Match projects to end dates
    print("\nMatching projects to end dates...")
    matched_projects = match_projects_to_end_dates(projects, pta_to_end_date)
    
    if not matched_projects:
        print("No projects matched to end dates. Nothing to update.")
        return
    
    # Filter out projects that already have the correct expiration date
    projects_to_update = []
    already_correct = []
    
    for project in matched_projects:
        current_expires = project.get('current_expires_on')
        new_expires = project['end_date']
        
        # If current expiration is None or different, add to update list
        if current_expires is None or str(current_expires) != new_expires:
            projects_to_update.append(project)
        else:
            already_correct.append(project)
    
    print(f"\n✓ Found {len(projects_to_update)} projects that need expiration date updates")
    print(f"✓ Found {len(already_correct)} projects that already have the correct expiration date")
    
    if not projects_to_update:
        print("All matched projects already have the correct expiration dates. Nothing to update.")
        return
    
    # Update projects
    print(f"\nUpdating {len(projects_to_update)} projects...")
    logger.info(f"Starting to update {len(projects_to_update)} projects...")
    
    successful_updates = 0
    failed_updates = 0
    
    for i, project in enumerate(projects_to_update, 1):
        project_id = project['project_id']
        pta = project['pta']
        end_date = project['end_date']
        project_name = project['project_name']
        
        print(f"\n[{i}/{len(projects_to_update)}] Updating project: {project_name} (ID: {project_id})")
        print(f"  PTA: {pta}")
        print(f"  Setting expires_on to: {end_date}")
        
        if update_project_expiration(project_id, end_date, logger):
            successful_updates += 1
            print(f"  ✓ Successfully updated project {project_id}")
        else:
            failed_updates += 1
            print(f"  ✗ Failed to update project {project_id}")
        
        # Add a small delay to avoid overwhelming the API
        time.sleep(0.5)
    
    # Summary
    print("\n" + "=" * 60)
    print("PROJECT EXPIRATION DATE UPDATE SUMMARY")
    print("=" * 60)
    print(f"Total projects downloaded: {len(projects)}")
    print(f"PTAs in Excel file: {len(pta_to_end_date)}")
    print(f"Projects matched to end dates: {len(matched_projects)}")
    print(f"Projects already correct: {len(already_correct)}")
    print(f"Projects updated: {successful_updates}")
    print(f"Projects failed: {failed_updates}")
    if len(projects_to_update) > 0:
        print(f"Success rate: {(successful_updates/len(projects_to_update)*100):.1f}%")
    
    # Log summary
    logger.info("=" * 60)
    logger.info("PROJECT EXPIRATION DATE UPDATE SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total projects downloaded: {len(projects)}")
    logger.info(f"PTAs in Excel file: {len(pta_to_end_date)}")
    logger.info(f"Projects matched to end dates: {len(matched_projects)}")
    logger.info(f"Projects already correct: {len(already_correct)}")
    logger.info(f"Projects updated: {successful_updates}")
    logger.info(f"Projects failed: {failed_updates}")
    logger.info("=" * 60)
    logger.info("PROJECT EXPIRATION DATE UPDATE SESSION ENDED")
    logger.info("=" * 60)
    
    print(f"\n✓ Detailed log saved to: logs/add_project_expiration_*.log")

if __name__ == "__main__":
    main()

