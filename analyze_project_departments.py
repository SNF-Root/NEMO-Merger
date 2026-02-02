#!/usr/bin/env python3
"""
Script to analyze project departments:
- Count projects without departments
- Show frequency breakdown by department
"""

import requests
import json
import os
from dotenv import load_dotenv
from typing import List, Dict, Any
from collections import Counter, defaultdict

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

def load_or_download_projects(force_download: bool = True) -> List[Dict[str, Any]]:
    """Load projects from file or download from API.
    
    Args:
        force_download: If True, always download fresh data from API
    """
    # Always download fresh data to ensure accuracy
    if force_download:
        print("Downloading fresh projects from NEMO API...")
    else:
        # Try to load from file first
        if os.path.exists("nemo_projects.json"):
            try:
                with open("nemo_projects.json", 'r', encoding='utf-8') as f:
                    projects = json.load(f)
                print(f"⚠ Loaded {len(projects)} projects from nemo_projects.json (may be outdated)")
                print("⚠ Consider using fresh data for accurate results")
                return projects
            except Exception as e:
                print(f"⚠ Error loading projects from file: {e}")
                print("Downloading from API...")
    
    # Download from API
    try:
        if not force_download:
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

def analyze_project_departments(projects: List[Dict[str, Any]], departments: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze project departments and return statistics."""
    # Create department ID to name mapping
    dept_id_to_name = {dept.get('id'): dept.get('name', 'Unknown') for dept in departments}
    
    # Count projects by department
    dept_counter = Counter()
    projects_without_dept = []
    projects_with_dept = []
    
    for project in projects:
        project_id = project.get('id')
        project_name = project.get('name', 'Unknown')
        
        # Check if department field exists and is explicitly null
        # Only count as "without department" if department is None/null
        # Any other value (including 0, False, empty string) counts as "with department"
        
        # Get department value - check both 'department' key and value
        dept_id = project.get('department')
        
        # Only count as "without department" if department is explicitly None/null
        # Check: if key doesn't exist OR value is None/null
        if dept_id is None:
            # Department is null or doesn't exist
            projects_without_dept.append({
                'id': project_id,
                'name': project_name,
                'application_identifier': project.get('application_identifier'),
                'department_value': dept_id  # Will be None
            })
        else:
            # Department has a value (even if it's 0, False, empty string, etc.)
            # All non-null values count as "with department"
            dept_name = dept_id_to_name.get(dept_id, f'Unknown (ID: {dept_id})')
            dept_counter[dept_name] += 1
            projects_with_dept.append({
                'id': project_id,
                'name': project_name,
                'department_id': dept_id,
                'department_name': dept_name,
                'application_identifier': project.get('application_identifier')
            })
    
    return {
        'total_projects': len(projects),
        'projects_without_department': len(projects_without_dept),
        'projects_with_department': len(projects_with_dept),
        'department_frequency': dict(dept_counter),
        'projects_without_dept_list': projects_without_dept,
        'projects_with_dept_list': projects_with_dept
    }

def main():
    """Main function."""
    print("=" * 80)
    print("PROJECT DEPARTMENT ANALYSIS")
    print("=" * 80)
    
    # Test API connection
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Load or download projects (always download fresh to ensure accuracy)
    print("\n" + "-" * 80)
    print("Loading Projects (Downloading Fresh Data)")
    print("-" * 80)
    projects = load_or_download_projects(force_download=True)
    
    if not projects:
        print("No projects found. Cannot proceed.")
        return
    
    # Load or download departments
    print("\n" + "-" * 80)
    print("Loading Departments")
    print("-" * 80)
    departments = load_or_download_departments()
    
    if not departments:
        print("⚠ No departments found. Department names will show as IDs.")
    
    # Analyze projects
    print("\n" + "-" * 80)
    print("Analyzing Projects")
    print("-" * 80)
    
    # First, let's verify a few sample projects to ensure we're reading the data correctly
    print("\nVerifying sample projects...")
    sample_count = min(5, len(projects))
    for i, project in enumerate(projects[:sample_count]):
        dept_value = project.get('department')
        dept_type = type(dept_value).__name__
        print(f"  Project {project.get('id')}: department = {dept_value} (type: {dept_type})")
    
    stats = analyze_project_departments(projects, departments)
    
    # Double-check: verify some projects marked as "without department"
    print(f"\nVerifying projects marked as 'without department'...")
    verify_count = min(10, len(stats['projects_without_dept_list']))
    for i, proj in enumerate(stats['projects_without_dept_list'][:verify_count]):
        # Find the full project data
        full_project = next((p for p in projects if p.get('id') == proj['id']), None)
        if full_project:
            actual_dept = full_project.get('department')
            if actual_dept is not None:
                print(f"  ⚠ WARNING: Project {proj['id']} ({proj['name']}) marked as 'without department' but has department={actual_dept}")
            else:
                print(f"  ✓ Project {proj['id']} ({proj['name']}): department={actual_dept} (correctly identified)")
    
    # Display results
    print("\n" + "=" * 80)
    print("ANALYSIS RESULTS")
    print("=" * 80)
    
    print(f"\nTotal Projects: {stats['total_projects']}")
    print(f"Projects WITH Department: {stats['projects_with_department']} ({(stats['projects_with_department']/stats['total_projects']*100):.1f}%)")
    print(f"Projects WITHOUT Department: {stats['projects_without_department']} ({(stats['projects_without_department']/stats['total_projects']*100):.1f}%)")
    
    # Department frequency breakdown
    print("\n" + "-" * 80)
    print("DEPARTMENT FREQUENCY BREAKDOWN")
    print("-" * 80)
    
    if stats['department_frequency']:
        # Sort by frequency (descending)
        sorted_depts = sorted(stats['department_frequency'].items(), key=lambda x: x[1], reverse=True)
        
        print(f"\n{'Department Name':<60} {'Count':<10} {'Percentage':<10}")
        print("-" * 80)
        
        for dept_name, count in sorted_depts:
            percentage = (count / stats['total_projects'] * 100)
            print(f"{dept_name:<60} {count:<10} {percentage:>6.1f}%")
        
        print("-" * 80)
        print(f"{'TOTAL':<60} {stats['projects_with_department']:<10} {100.0:>6.1f}%")
    else:
        print("\nNo projects with departments found.")
    
    # Save detailed results
    print("\n" + "-" * 80)
    print("Saving Results")
    print("-" * 80)
    
    # Save summary statistics
    summary = {
        'total_projects': stats['total_projects'],
        'projects_without_department': stats['projects_without_department'],
        'projects_with_department': stats['projects_with_department'],
        'department_frequency': stats['department_frequency']
    }
    
    with open("project_department_analysis.json", 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print("✓ Saved summary to project_department_analysis.json")
    
    # Save projects without departments
    if stats['projects_without_dept_list']:
        with open("projects_without_department.json", 'w', encoding='utf-8') as f:
            json.dump(stats['projects_without_dept_list'], f, indent=2, ensure_ascii=False)
        print(f"✓ Saved {len(stats['projects_without_dept_list'])} projects without departments to projects_without_department.json")
    
    # Save projects grouped by department
    projects_by_dept = defaultdict(list)
    for project in stats['projects_with_dept_list']:
        dept_name = project['department_name']
        projects_by_dept[dept_name].append({
            'id': project['id'],
            'name': project['name'],
            'application_identifier': project.get('application_identifier')
        })
    
    with open("projects_by_department.json", 'w', encoding='utf-8') as f:
        json.dump(dict(projects_by_dept), f, indent=2, ensure_ascii=False)
    print(f"✓ Saved projects grouped by department to projects_by_department.json")
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    main()
