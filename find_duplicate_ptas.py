#!/usr/bin/env python3
"""
Script to find duplicate PTAs (application_identifier) in nemo_projects.csv
and show which project IDs use the same PTA.
"""

import csv
from collections import defaultdict

def find_duplicate_ptas(csv_file):
    """
    Find PTAs that are used by multiple projects.
    
    Returns:
        dict: Dictionary mapping PTA to list of project IDs
    """
    pta_to_projects = defaultdict(list)
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            project_id = row['id']
            pta = row['application_identifier']
            project_name = row['project_name']
            
            # Store project info for this PTA
            pta_to_projects[pta].append({
                'id': project_id,
                'name': project_name
            })
    
    # Filter to only PTAs that appear more than once
    duplicate_ptas = {
        pta: projects 
        for pta, projects in pta_to_projects.items() 
        if len(projects) > 1
    }
    
    return duplicate_ptas

def main():
    csv_file = 'nemo_projects.csv'
    
    print("Finding duplicate PTAs in nemo_projects.csv...")
    print("=" * 80)
    
    duplicate_ptas = find_duplicate_ptas(csv_file)
    
    if not duplicate_ptas:
        print("No duplicate PTAs found!")
        return
    
    # Sort by number of projects (descending)
    sorted_ptas = sorted(
        duplicate_ptas.items(), 
        key=lambda x: len(x[1]), 
        reverse=True
    )
    
    print(f"\nFound {len(duplicate_ptas)} PTAs used by multiple projects:\n")
    
    total_duplicate_projects = 0
    for pta, projects in sorted_ptas:
        num_projects = len(projects)
        total_duplicate_projects += num_projects
        print(f"PTA: {pta}")
        print(f"  Used by {num_projects} projects:")
        for project in projects:
            print(f"    - Project ID: {project['id']:>5} | {project['name']}")
        print()
    
    print("=" * 80)
    print(f"Summary:")
    print(f"  Total duplicate PTAs: {len(duplicate_ptas)}")
    print(f"  Total projects using duplicate PTAs: {total_duplicate_projects}")
    print(f"  Expected unique projects (if 1:1): {len(duplicate_ptas)}")
    print(f"  Extra projects: {total_duplicate_projects - len(duplicate_ptas)}")

if __name__ == '__main__':
    main()

