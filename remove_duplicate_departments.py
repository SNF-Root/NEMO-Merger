#!/usr/bin/env python3
"""
Script to remove duplicate departments from NEMO.
Deletes departments that are exact duplicates (same normalized name).
Keeps departments with "Department" in the name, deletes those without.
"""

import json
import requests
import os
from dotenv import load_dotenv
from typing import List, Dict, Tuple
import time

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

def normalize_name(name: str) -> str:
    """Normalize a department name for comparison."""
    # Remove "Department" suffix
    normalized = name.replace(" Department", "").strip()
    # Normalize ampersands
    normalized = normalized.replace(" & ", " and ").replace("&", " and ")
    # Convert to lowercase for comparison
    return normalized.lower()

def load_departments(filename: str = "nemo_departments.json") -> List[Dict]:
    """Load departments from JSON file."""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            departments = json.load(f)
        print(f"✓ Loaded {len(departments)} departments from {filename}")
        return departments
    except Exception as e:
        print(f"✗ Error loading departments: {e}")
        return []

def find_duplicate_departments(departments: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """Find departments that are exact duplicates (same normalized name).
    Keeps departments with 'Department' in the name, deletes those without.
    
    Returns:
        Tuple of (departments_to_delete, departments_to_keep) where both are lists of duplicate pairs
    """
    duplicates_to_delete = []
    duplicates_to_keep = []
    seen_normalized = {}
    
    for dept in departments:
        name = dept.get('name', '')
        dept_id = dept.get('id')
        normalized = normalize_name(name)
        has_department = 'Department' in name
        
        if normalized in seen_normalized:
            # Found a duplicate - keep the one with "Department" in the name
            existing_dept = seen_normalized[normalized]
            existing_name = existing_dept.get('name', '')
            existing_has_department = 'Department' in existing_name
            
            if has_department and not existing_has_department:
                # Current department has "Department", existing doesn't - keep current, delete existing
                duplicates_to_delete.append(existing_dept)
                duplicates_to_keep.append(dept)
                seen_normalized[normalized] = dept  # Update to keep the one with "Department"
            elif not has_department and existing_has_department:
                # Existing department has "Department", current doesn't - keep existing, delete current
                duplicates_to_delete.append(dept)
                duplicates_to_keep.append(existing_dept)
            else:
                # Both have or both don't have "Department" - keep the one with lower ID as fallback
                existing_id = existing_dept.get('id')
                if dept_id > existing_id:
                    duplicates_to_delete.append(dept)
                    duplicates_to_keep.append(existing_dept)
                else:
                    duplicates_to_delete.append(existing_dept)
                    duplicates_to_keep.append(dept)
                    seen_normalized[normalized] = dept
        else:
            seen_normalized[normalized] = dept
    
    return duplicates_to_delete, duplicates_to_keep

def delete_department_by_id(dept_id: int, dept_name: str) -> Tuple[bool, str]:
    """
    Delete a single department from NEMO API by ID.
    
    Returns:
        Tuple of (success: bool, status: str) where status is 'success', 'not_found', or 'error'
    """
    delete_url = f"{NEMO_DEPARTMENTS_API_URL}{dept_id}/"
    
    try:
        response = requests.delete(delete_url, headers=API_HEADERS)
        
        if response.status_code == 204:  # No Content (successful deletion)
            print(f"✓ Successfully deleted department ID {dept_id} ({dept_name})")
            return (True, 'success')
        elif response.status_code == 404:
            print(f"⚠ Department ID {dept_id} ({dept_name}) not found - may already be deleted")
            return (False, 'not_found')
        elif response.status_code == 400:
            print(f"✗ Bad request for department ID {dept_id} ({dept_name}): {response.text}")
            return (False, 'error')
        elif response.status_code == 401:
            print(f"✗ Authentication failed: Check your NEMO_TOKEN")
            return (False, 'error')
        elif response.status_code == 403:
            print(f"✗ Permission denied for department ID {dept_id} ({dept_name}): Check your API permissions")
            return (False, 'error')
        else:
            print(f"✗ Failed to delete department ID {dept_id} ({dept_name}): HTTP {response.status_code}")
            if response.text:
                print(f"  Response: {response.text}")
            return (False, 'error')
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error deleting department ID {dept_id} ({dept_name}): {e}")
        return (False, 'error')

def main():
    """Main function to find and delete duplicate departments."""
    print("Finding and removing duplicate departments from NEMO...")
    print(f"API Endpoint: {NEMO_DEPARTMENTS_API_URL}")
    print("-" * 80)
    
    # Load departments
    departments = load_departments()
    
    if not departments:
        print("No departments loaded. Cannot proceed.")
        return
    
    # Find duplicates
    print("\nAnalyzing departments for duplicates...")
    duplicates_to_delete, duplicates_to_keep = find_duplicate_departments(departments)
    
    if not duplicates_to_delete:
        print("\n✓ No duplicate departments found!")
        return
    
    # Sort by ID for easier reading
    duplicates_to_delete.sort(key=lambda x: x.get('id', 0))
    duplicates_to_keep.sort(key=lambda x: x.get('id', 0))
    
    print(f"\n{'='*80}")
    print(f"FOUND {len(duplicates_to_delete)} DUPLICATE DEPARTMENTS TO DELETE")
    print(f"{'='*80}")
    print("\nDepartments to DELETE (without 'Department' in name):")
    for dept in duplicates_to_delete:
        dept_id = dept.get('id', 'N/A')
        dept_name = dept.get('name', 'N/A')
        display_order = dept.get('display_order', 'N/A')
        print(f"  ID {dept_id:3d} (order {display_order:3d}): \"{dept_name}\"")
    
    print("\nDepartments to KEEP (with 'Department' in name):")
    for dept in duplicates_to_keep:
        dept_id = dept.get('id', 'N/A')
        dept_name = dept.get('name', 'N/A')
        display_order = dept.get('display_order', 'N/A')
        print(f"  ID {dept_id:3d} (order {display_order:3d}): \"{dept_name}\"")
    
    # Confirm before proceeding
    print(f"\n{'='*80}")
    print("⚠ WARNING: This will permanently delete departments from NEMO!")
    print(f"About to delete {len(duplicates_to_delete)} departments.")
    print(f"{'='*80}")
    response = input("\nDo you want to continue? (yes/no): ")
    
    if response.lower() not in ['yes', 'y']:
        print("Cancelled. No departments were deleted.")
        return
    
    # Delete departments
    successful = 0
    failed = 0
    not_found = 0
    
    print(f"\n{'='*80}")
    print("DELETING DEPARTMENTS...")
    print(f"{'='*80}")
    
    for dept in duplicates_to_delete:
        dept_id = dept.get('id')
        dept_name = dept.get('name', 'Unknown')
        
        success, status = delete_department_by_id(dept_id, dept_name)
        
        if success:
            successful += 1
        elif status == 'not_found':
            not_found += 1
        else:
            failed += 1
        
        time.sleep(0.5)  # Small delay between requests
    
    # Summary
    print(f"\n{'='*80}")
    print("DEPARTMENT DELETION SUMMARY")
    print(f"{'='*80}")
    print(f"Total departments to delete: {len(duplicates_to_delete)}")
    print(f"Successfully deleted: {successful}")
    print(f"Not found (may already be deleted): {not_found}")
    print(f"Failed to delete: {failed}")
    if len(duplicates_to_delete) > 0:
        print(f"Success rate: {(successful/len(duplicates_to_delete)*100):.1f}%")

if __name__ == "__main__":
    main()
