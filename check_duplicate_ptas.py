#!/usr/bin/env python3
"""
Script to check for duplicate PTAs in downloaded projects.
This helps identify if multiple projects share the same PTA, which could cause
confusion when assigning projects to users.
"""

import json
import os
import csv
import requests
from dotenv import load_dotenv
from typing import List, Dict, Any, DefaultDict
from collections import defaultdict
from datetime import datetime

# Load environment variables
load_dotenv()

# NEMO API endpoint for projects
NEMO_PROJECTS_API_URL = "https://nemo.stanford.edu/api/projects/"

# Get NEMO token from environment
NEMO_TOKEN = os.getenv('NEMO_TOKEN')
API_HEADERS = {
    'Authorization': f'Token {NEMO_TOKEN}',
    'Content-Type': 'application/json',
    'Accept': 'application/json'
} if NEMO_TOKEN else {}

def load_projects_from_file(filename: str = "nemo_projects.json") -> List[Dict[str, Any]]:
    """Load projects from local JSON file."""
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                projects = json.load(f)
                print(f"✓ Loaded {len(projects)} projects from {filename}")
                return projects
        else:
            print(f"⚠ File {filename} not found")
            return []
    except Exception as e:
        print(f"✗ Error loading projects from file: {e}")
        return []

def download_projects() -> List[Dict[str, Any]]:
    """Download all projects from the NEMO API."""
    try:
        if not NEMO_TOKEN:
            print("⚠ NEMO_TOKEN not found, cannot download from API")
            return []
        
        print("Downloading projects from NEMO API...")
        response = requests.get(NEMO_PROJECTS_API_URL, headers=API_HEADERS)
        
        if response.status_code == 200:
            projects = response.json()
            print(f"✓ Successfully downloaded {len(projects)} projects")
            return projects
        else:
            print(f"✗ Failed to download projects: HTTP {response.status_code}")
            return []
    except Exception as e:
        print(f"✗ Error downloading projects: {e}")
        return []

def extract_pta_from_project(project: Dict[str, Any]) -> str:
    """Extract PTA (application_identifier) from a project."""
    # Check for PTA field (could be 'PTA', 'pta', 'application_identifier', etc.)
    if 'application_identifier' in project and project['application_identifier']:
        pta = str(project['application_identifier']).strip()
        if pta and pta.lower() not in ['none', 'null', '']:
            return pta
    elif 'PTA' in project and project['PTA']:
        pta = str(project['PTA']).strip()
        if pta and pta.lower() not in ['none', 'null', '']:
            return pta
    elif 'pta' in project and project['pta']:
        pta = str(project['pta']).strip()
        if pta and pta.lower() not in ['none', 'null', '']:
            return pta
    return None

def find_duplicate_ptas(projects: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Find all PTAs that appear in multiple projects.
    
    Returns a dictionary mapping PTA -> list of projects with that PTA.
    """
    pta_to_projects: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    
    for project in projects:
        pta = extract_pta_from_project(project)
        if pta:
            pta_to_projects[pta].append(project)
    
    # Filter to only include PTAs with duplicates
    duplicates = {pta: projs for pta, projs in pta_to_projects.items() if len(projs) > 1}
    
    return duplicates

def export_duplicates_to_csv(duplicates: Dict[str, List[Dict[str, Any]]], pta_lookup: Dict[str, int] = None) -> str:
    """Export duplicate PTAs to a CSV file.
    
    Returns the filename of the created CSV.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"duplicate_ptas_{timestamp}.csv"
    
    # Find maximum number of projects for any PTA (to determine number of columns)
    max_projects = max(len(projs) for projs in duplicates.values()) if duplicates else 0
    
    # Create CSV with dynamic columns
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        # Build header row
        header = ['PTA', 'Number of Projects']
        for i in range(1, max_projects + 1):
            header.extend([
                f'Project ID {i}',
                f'Project Name {i}',
                f'Active {i}',
                f'Account ID {i}'
            ])
        if pta_lookup:
            header.append('Mapped Project ID (pta_lookup.json)')
        
        writer = csv.writer(f)
        writer.writerow(header)
        
        # Sort by number of duplicates (most duplicates first)
        sorted_duplicates = sorted(duplicates.items(), key=lambda x: len(x[1]), reverse=True)
        
        # Write data rows
        for pta, projs in sorted_duplicates:
            row = [pta, len(projs)]
            
            # Add project information
            for proj in projs:
                row.extend([
                    proj.get('id', 'N/A'),
                    proj.get('name', 'N/A'),
                    proj.get('active', 'N/A'),
                    proj.get('account', 'N/A')
                ])
            
            # Pad with empty cells if fewer projects than max
            while len(row) < 2 + (max_projects * 4):
                row.extend(['', '', '', ''])
            
            # Add mapped project ID from pta_lookup.json
            if pta_lookup:
                mapped_id = pta_lookup.get(pta, 'Not in lookup')
                row.append(mapped_id)
            
            writer.writerow(row)
    
    return filename

def main():
    """Main function to check for duplicate PTAs."""
    print("=" * 60)
    print("CHECKING FOR DUPLICATE PTAs IN PROJECTS")
    print("=" * 60)
    print()
    
    # Try to load from file first
    projects = load_projects_from_file()
    
    # If no file, try to download
    if not projects:
        projects = download_projects()
    
    if not projects:
        print("✗ No projects available. Please run download_projects.py first or ensure API access.")
        return
    
    print(f"\nAnalyzing {len(projects)} projects...")
    
    # Find duplicates
    duplicates = find_duplicate_ptas(projects)
    
    # Statistics
    total_projects_with_pta = sum(1 for p in projects if extract_pta_from_project(p))
    unique_ptas = len(set(extract_pta_from_project(p) for p in projects if extract_pta_from_project(p)))
    duplicate_ptas_count = len(duplicates)
    projects_in_duplicates = sum(len(projs) for projs in duplicates.values())
    
    print("\n" + "=" * 60)
    print("STATISTICS")
    print("=" * 60)
    print(f"Total projects analyzed: {len(projects)}")
    print(f"Projects with PTA: {total_projects_with_pta}")
    print(f"Unique PTAs: {unique_ptas}")
    print(f"PTAs with duplicates: {duplicate_ptas_count}")
    print(f"Projects involved in duplicates: {projects_in_duplicates}")
    
    if duplicate_ptas_count > 0:
        print(f"\n⚠ WARNING: Found {duplicate_ptas_count} PTA(s) that appear in multiple projects!")
        print("This could cause incorrect project assignments.")
    else:
        print("\n✓ No duplicate PTAs found. All PTAs are unique.")
    
    # Show details of duplicates
    if duplicates:
        print("\n" + "=" * 60)
        print("DUPLICATE PTA DETAILS")
        print("=" * 60)
        
        # Sort by number of duplicates (most duplicates first)
        sorted_duplicates = sorted(duplicates.items(), key=lambda x: len(x[1]), reverse=True)
        
        for pta, projs in sorted_duplicates:
            print(f"\nPTA: {pta}")
            print(f"  Appears in {len(projs)} project(s):")
            for i, proj in enumerate(projs, 1):
                proj_id = proj.get('id', 'N/A')
                proj_name = proj.get('name', 'N/A')
                proj_active = proj.get('active', 'N/A')
                print(f"    {i}. Project ID: {proj_id}, Name: {proj_name}, Active: {proj_active}")
        
        # Summary by duplicate count
        print("\n" + "=" * 60)
        print("SUMMARY BY DUPLICATE COUNT")
        print("=" * 60)
        count_distribution = defaultdict(int)
        for pta, projs in duplicates.items():
            count_distribution[len(projs)] += 1
        
        for count in sorted(count_distribution.keys(), reverse=True):
            num_ptas = count_distribution[count]
            print(f"PTAs appearing in {count} project(s): {num_ptas}")
        
        # Show which project IDs would be assigned (from pta_lookup.json)
        print("\n" + "=" * 60)
        print("PTA LOOKUP TABLE IMPACT")
        print("=" * 60)
        print("Checking pta_lookup.json to see which project ID is currently mapped...")
        
        pta_lookup = None
        try:
            if os.path.exists("pta_lookup.json"):
                with open("pta_lookup.json", 'r') as f:
                    pta_lookup = json.load(f)
                
                conflicts = []
                for pta, projs in sorted_duplicates:
                    if pta in pta_lookup:
                        mapped_id = pta_lookup[pta]
                        proj_ids = [p.get('id') for p in projs]
                        if mapped_id not in proj_ids:
                            conflicts.append((pta, mapped_id, proj_ids))
                        else:
                            print(f"\nPTA {pta}: Maps to Project ID {mapped_id} (one of {len(projs)} projects)")
                            print(f"  Other projects with same PTA: {[p.get('id') for p in projs if p.get('id') != mapped_id]}")
                
                if conflicts:
                    print("\n⚠ CONFLICTS FOUND:")
                    print("The following PTAs map to project IDs that don't match any of the duplicate projects:")
                    for pta, mapped_id, proj_ids in conflicts:
                        print(f"  PTA {pta}: Maps to {mapped_id}, but projects are {proj_ids}")
            else:
                print("pta_lookup.json not found. Skipping lookup table check.")
        except Exception as e:
            print(f"⚠ Error checking pta_lookup.json: {e}")
        
        # Export to CSV
        print("\n" + "=" * 60)
        print("EXPORTING TO CSV")
        print("=" * 60)
        csv_filename = export_duplicates_to_csv(duplicates, pta_lookup)
        print(f"✓ Exported duplicate PTAs to: {csv_filename}")
    
    print("\n" + "=" * 60)
    print("ANALYSIS COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()

