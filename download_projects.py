#!/usr/bin/env python3
"""
Script to download all projects from NEMO API and save them locally.
This is needed before creating projects to check for duplicates by PTA (application_identifier).
"""

import requests
import json
import os
import csv
from dotenv import load_dotenv
from typing import List, Dict, Any, Set

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

def save_projects_to_file(projects: List[Dict[str, Any]], filename: str = "nemo_projects.json"):
    """Save projects to a local JSON file."""
    try:
        with open(filename, 'w') as f:
            json.dump(projects, f, indent=2)
        print(f"✓ Successfully saved {len(projects)} projects to {filename}")
    except Exception as e:
        print(f"✗ Error saving projects to file: {e}")

def save_projects_to_csv(projects: List[Dict[str, Any]], filename: str = "nemo_projects.csv"):
    """Save projects to a CSV file, sorted by ID in ascending order."""
    if not projects:
        print("No projects to save to CSV")
        return
    
    try:
        # Sort projects by ID in ascending order
        sorted_projects = sorted(projects, key=lambda x: x.get('id', 0))
        
        # Get all unique keys from all projects to create comprehensive headers
        all_keys = set()
        for project in sorted_projects:
            all_keys.update(project.keys())
        
        # Define column order (ID first, then others alphabetically)
        fieldnames = ['id'] + sorted([k for k in all_keys if k != 'id'])
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for project in sorted_projects:
                # Convert None values to empty strings for CSV
                row = {k: ('' if v is None else v) for k, v in project.items()}
                writer.writerow(row)
        
        print(f"✓ Successfully saved {len(sorted_projects)} projects to {filename} (sorted by ID)")
    except Exception as e:
        print(f"✗ Error saving projects to CSV: {e}")

def create_pta_lookup(projects: List[Dict[str, Any]]) -> Dict[str, int]:
    """Create a lookup dictionary mapping PTAs (application_identifier) to project IDs."""
    lookup = {}
    for project in projects:
        # Check for PTA field (could be 'PTA', 'pta', 'application_identifier', etc.)
        pta = None
        if 'application_identifier' in project and project['application_identifier']:
            pta = str(project['application_identifier']).strip()
        elif 'PTA' in project and project['PTA']:
            pta = str(project['PTA']).strip()
        elif 'pta' in project and project['pta']:
            pta = str(project['pta']).strip()
        
        if pta and pta.lower() != 'none' and pta.lower() != 'null' and 'id' in project:
            lookup[pta] = project['id']
    
    print(f"✓ Created PTA lookup for {len(lookup)} projects")
    return lookup

def create_project_name_lookup(projects: List[Dict[str, Any]]) -> Dict[str, int]:
    """Create a lookup dictionary mapping project names to project IDs."""
    lookup = {}
    for project in projects:
        if 'name' in project and project['name'] and 'id' in project:
            name = str(project['name']).strip()
            if name and name.lower() != 'none' and name.lower() != 'null':
                lookup[name] = project['id']
    
    print(f"✓ Created project name lookup for {len(lookup)} projects")
    return lookup

def get_existing_ptas(projects: List[Dict[str, Any]]) -> Set[str]:
    """Extract all existing PTAs (application_identifiers) as a set."""
    ptas = set()
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
            ptas.add(pta)
    
    print(f"✓ Found {len(ptas)} unique PTAs in existing projects")
    return ptas

def main():
    """Main function to download and save projects."""
    print("Starting project download from NEMO API...")
    print(f"API Endpoint: {NEMO_PROJECTS_API_URL}")
    print("-" * 60)
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Download projects
    projects = download_projects()
    
    if not projects:
        print("No projects downloaded. Cannot proceed.")
        return
    
    # Save projects to file
    save_projects_to_file(projects)
    
    # Save projects to CSV (sorted by ID)
    save_projects_to_csv(projects)
    
    # Create and save PTA lookup
    pta_lookup = create_pta_lookup(projects)
    
    # Save PTA lookup to a separate file for easy access
    with open("pta_lookup.json", 'w') as f:
        json.dump(pta_lookup, f, indent=2)
    print("✓ Saved PTA lookup to pta_lookup.json")
    
    # Create and save project name lookup
    project_name_lookup = create_project_name_lookup(projects)
    
    # Save project name lookup to a separate file
    with open("project_name_lookup.json", 'w') as f:
        json.dump(project_name_lookup, f, indent=2)
    print("✓ Saved project name lookup to project_name_lookup.json")
    
    # Create and save existing PTAs set
    existing_ptas = get_existing_ptas(projects)
    
    # Save existing PTAs as a list (JSON doesn't support sets)
    with open("existing_ptas.json", 'w') as f:
        json.dump(sorted(list(existing_ptas)), f, indent=2)
    print("✓ Saved existing PTAs list to existing_ptas.json")
    
    # Show sample of projects
    print("\nSample projects:")
    for i, project in enumerate(projects[:5]):
        pta = project.get('application_identifier') or project.get('PTA') or project.get('pta') or 'N/A'
        print(f"  {i+1}. ID: {project.get('id', 'N/A')}, Name: {project.get('name', 'N/A')}, PTA: {pta}")
    
    if len(projects) > 5:
        print(f"  ... and {len(projects) - 5} more projects")
    
    print(f"\n✓ Project download complete! {len(projects)} projects saved locally.")
    print(f"✓ Found {len(existing_ptas)} unique PTAs in existing projects.")
    print("You can now run create_projects.py to create new projects (duplicates will be filtered out).")

if __name__ == "__main__":
    main()

