#!/usr/bin/env python3
"""
Script to check which accounts have associated projects.
Reads nemo_accounts.json and nemo_projects.json, then creates a CSV
showing all accounts and whether they have a project associated with them.
"""

import json
import csv
from typing import List, Dict, Any, Set
from collections import defaultdict

def load_accounts(filename: str = "nemo_accounts.json") -> List[Dict[str, Any]]:
    """Load accounts from JSON file."""
    try:
        with open(filename, 'r') as f:
            accounts = json.load(f)
        print(f"✓ Loaded {len(accounts)} accounts from {filename}")
        return accounts
    except FileNotFoundError:
        print(f"✗ Error: {filename} not found")
        return []
    except json.JSONDecodeError as e:
        print(f"✗ Error parsing {filename}: {e}")
        return []

def load_projects(filename: str = "nemo_projects.json") -> List[Dict[str, Any]]:
    """Load projects from JSON file."""
    try:
        with open(filename, 'r') as f:
            projects = json.load(f)
        print(f"✓ Loaded {len(projects)} projects from {filename}")
        return projects
    except FileNotFoundError:
        print(f"✗ Error: {filename} not found")
        return []
    except json.JSONDecodeError as e:
        print(f"✗ Error parsing {filename}: {e}")
        return []

def create_account_project_mapping(projects: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    """Create a mapping of account IDs to their associated projects."""
    account_projects = defaultdict(list)
    
    for project in projects:
        account_id = project.get('account')
        if account_id is not None:
            account_projects[account_id].append(project)
    
    print(f"✓ Found projects for {len(account_projects)} unique accounts")
    return dict(account_projects)

def create_accounts_with_projects_csv(
    accounts: List[Dict[str, Any]],
    account_projects: Dict[int, List[Dict[str, Any]]],
    filename: str = "accounts_with_projects.csv"
):
    """Create a CSV file showing all accounts and whether they have projects."""
    if not accounts:
        print("No accounts to process")
        return
    
    # Sort accounts by ID
    sorted_accounts = sorted(accounts, key=lambda x: x.get('id', 0))
    
    # Prepare CSV data
    csv_data = []
    
    for account in sorted_accounts:
        account_id = account.get('id')
        account_name = account.get('name', '')
        account_type = account.get('type', '')
        account_active = account.get('active', False)
        account_start_date = account.get('start_date', '')
        
        # Check if account has projects
        projects = account_projects.get(account_id, [])
        has_project = len(projects) > 0
        project_count = len(projects)
        
        # Get project names if any
        project_names = ', '.join([p.get('name', '') for p in projects[:3]])  # Show first 3
        if project_count > 3:
            project_names += f" ... (+{project_count - 3} more)"
        
        csv_data.append({
            'account_id': account_id,
            'account_name': account_name,
            'account_type': account_type,
            'account_active': account_active,
            'account_start_date': account_start_date if account_start_date else '',
            'has_project': 'Yes' if has_project else 'No',
            'project_count': project_count,
            'project_names': project_names if has_project else ''
        })
    
    # Write to CSV
    fieldnames = [
        'account_id',
        'account_name',
        'account_type',
        'account_active',
        'account_start_date',
        'has_project',
        'project_count',
        'project_names'
    ]
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_data)
        
        print(f"✓ Successfully created {filename}")
        
        # Print summary statistics
        total_accounts = len(csv_data)
        accounts_with_projects = sum(1 for row in csv_data if row['has_project'] == 'Yes')
        accounts_without_projects = total_accounts - accounts_with_projects
        
        print(f"\nSummary:")
        print(f"  Total accounts: {total_accounts}")
        print(f"  Accounts with projects: {accounts_with_projects}")
        print(f"  Accounts without projects: {accounts_without_projects}")
        
    except Exception as e:
        print(f"✗ Error creating CSV file: {e}")

def main():
    """Main function."""
    print("Checking accounts for associated projects...")
    print("-" * 60)
    
    # Load accounts
    accounts = load_accounts("nemo_accounts.json")
    if not accounts:
        print("Cannot proceed without accounts.")
        return
    
    # Load projects
    projects = load_projects("nemo_projects.json")
    if not projects:
        print("Warning: No projects found. All accounts will show 'No' for has_project.")
    
    # Create mapping of account IDs to projects
    account_projects = create_account_project_mapping(projects)
    
    # Create CSV
    create_accounts_with_projects_csv(accounts, account_projects, "accounts_with_projects.csv")
    
    print("\n✓ Complete!")

if __name__ == "__main__":
    main()

