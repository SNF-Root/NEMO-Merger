#!/usr/bin/env python3
"""
Script to download all tools and users, then create a table showing tools and their super users.
Matches super user IDs to user records to display user information.
"""

import requests
import json
import os
import csv
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoints
NEMO_TOOLS_API_URL = "https://nemo.stanford.edu/api/tools/"
NEMO_USERS_API_URL = "https://nemo.stanford.edu/api/users/"

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

def test_api_connection(api_url: str, name: str) -> bool:
    """Test the API connection and authentication."""
    print(f"Testing {name} API connection...")
    try:
        response = requests.get(api_url, headers=API_HEADERS)
        if response.status_code == 200:
            print(f"✓ {name} API connection successful")
            return True
        elif response.status_code == 401:
            print(f"✗ Authentication failed for {name}: Check your NEMO_TOKEN")
            return False
        elif response.status_code == 403:
            print(f"✗ Permission denied for {name}: Check your API permissions")
            return False
        else:
            print(f"✗ {name} API connection failed: HTTP {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error connecting to {name} API: {e}")
        return False

def download_all_items(api_url: str, item_name: str) -> List[Dict[str, Any]]:
    """Download all items from a NEMO API endpoint."""
    print(f"Downloading {item_name} from {api_url}...")
    
    all_items = []
    page = 1
    
    while True:
        try:
            params = {'page': page}
            response = requests.get(api_url, headers=API_HEADERS, params=params)
            
            if response.status_code == 200:
                response_data = response.json()
                
                # Check if this is a paginated response
                if 'results' in response_data:
                    items = response_data['results']
                    print(f"  Page {page}: Retrieved {len(items)} {item_name}")
                else:
                    # Direct list response
                    items = response_data
                    print(f"  Retrieved {len(items)} {item_name} (no pagination)")
                
                if not items:
                    break
                
                all_items.extend(items)
                
                # Check if there are more pages
                if 'next' in response_data and response_data['next']:
                    page += 1
                else:
                    break
                    
            elif response.status_code == 401:
                print(f"✗ Authentication failed: Check your NEMO_TOKEN")
                return []
            elif response.status_code == 403:
                print(f"✗ Permission denied: Check your API permissions")
                return []
            else:
                print(f"✗ Failed to download {item_name}: HTTP {response.status_code}")
                return []
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Network error downloading {item_name}: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"✗ Error parsing API response: {e}")
            return []
    
    print(f"✓ Total {item_name} downloaded: {len(all_items)}")
    return all_items

def create_user_lookup(users: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """Create a lookup dictionary mapping user IDs to user information."""
    lookup = {}
    for user in users:
        if 'id' in user:
            user_id = user['id']
            lookup[user_id] = {
                'id': user_id,
                'username': user.get('username', 'N/A'),
                'email': user.get('email', 'N/A'),
                'first_name': user.get('first_name', ''),
                'last_name': user.get('last_name', ''),
                'full_name': f"{user.get('first_name', '')} {user.get('last_name', '')}".strip() or 'N/A'
            }
    
    print(f"✓ Created user lookup for {len(lookup)} users")
    return lookup

def get_superuser_info(superuser_id: int, user_lookup: Dict[int, Dict[str, Any]]) -> Dict[str, Any]:
    """Get super user information from user lookup."""
    if superuser_id in user_lookup:
        return user_lookup[superuser_id]
    else:
        return {
            'id': superuser_id,
            'username': 'NOT FOUND',
            'email': 'NOT FOUND',
            'first_name': '',
            'last_name': '',
            'full_name': 'NOT FOUND'
        }

def create_tool_superusers_table(tools: List[Dict[str, Any]], user_lookup: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Create a table of tools and their super users."""
    table_data = []
    
    for tool in tools:
        tool_id = tool.get('id', 'N/A')
        tool_name = tool.get('name', 'N/A')
        tool_category = tool.get('_category', '')
        tool_location = tool.get('_location', '')
        superusers = tool.get('_superusers', [])
        
        if superusers:
            # Tool has super users - create a row for each super user
            for superuser_id in superusers:
                superuser_info = get_superuser_info(superuser_id, user_lookup)
                table_data.append({
                    'tool_id': tool_id,
                    'tool_name': tool_name,
                    'tool_category': tool_category,
                    'tool_location': tool_location,
                    'superuser_id': superuser_info['id'],
                    'superuser_username': superuser_info['username'],
                    'superuser_email': superuser_info['email'],
                    'superuser_first_name': superuser_info['first_name'],
                    'superuser_last_name': superuser_info['last_name'],
                    'superuser_full_name': superuser_info['full_name']
                })
        else:
            # Tool has no super users - create a row with empty super user fields
            table_data.append({
                'tool_id': tool_id,
                'tool_name': tool_name,
                'tool_category': tool_category,
                'tool_location': tool_location,
                'superuser_id': '',
                'superuser_username': '',
                'superuser_email': '',
                'superuser_first_name': '',
                'superuser_last_name': '',
                'superuser_full_name': ''
            })
    
    return table_data

def save_table_to_csv(table_data: List[Dict[str, Any]], filename: str):
    """Save the table data to a CSV file."""
    if not table_data:
        print("No data to save to CSV")
        return
    
    try:
        # Define column order
        fieldnames = [
            'tool_id',
            'tool_name',
            'tool_category',
            'tool_location',
            'superuser_id',
            'superuser_username',
            'superuser_email',
            'superuser_first_name',
            'superuser_last_name',
            'superuser_full_name'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for row in table_data:
                # Convert None values to empty strings for CSV
                clean_row = {k: ('' if v is None else v) for k, v in row.items()}
                writer.writerow(clean_row)
        
        print(f"✓ Successfully saved {len(table_data)} rows to {filename}")
    except Exception as e:
        print(f"✗ Error saving table to CSV: {e}")

def main():
    """Main function to download tools and users, then create super users table."""
    print("=" * 60)
    print("TOOL SUPER USERS TABLE GENERATOR")
    print("=" * 60)
    print()
    
    # Test API connections
    if not test_api_connection(NEMO_TOOLS_API_URL, "Tools"):
        print("Cannot proceed without valid Tools API connection.")
        return
    
    if not test_api_connection(NEMO_USERS_API_URL, "Users"):
        print("Cannot proceed without valid Users API connection.")
        return
    
    print()
    
    # Download tools
    tools = download_all_items(NEMO_TOOLS_API_URL, "tools")
    if not tools:
        print("No tools downloaded. Cannot proceed.")
        return
    
    print()
    
    # Download users
    users = download_all_items(NEMO_USERS_API_URL, "users")
    if not users:
        print("No users downloaded. Cannot proceed.")
        return
    
    print()
    
    # Create user lookup
    user_lookup = create_user_lookup(users)
    
    print()
    
    # Create table
    print("Creating tool super users table...")
    table_data = create_tool_superusers_table(tools, user_lookup)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"tool_superusers_{timestamp}.csv"
    
    # Save to CSV
    save_table_to_csv(table_data, csv_filename)
    
    # Print summary statistics
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total tools processed: {len(tools)}")
    print(f"Total users downloaded: {len(users)}")
    print(f"Total table rows: {len(table_data)}")
    
    # Count tools with super users
    tools_with_superusers = sum(1 for tool in tools if tool.get('_superusers', []))
    tools_without_superusers = len(tools) - tools_with_superusers
    
    print(f"Tools with super users: {tools_with_superusers}")
    print(f"Tools without super users: {tools_without_superusers}")
    
    # Count unique super users
    all_superuser_ids = set()
    for tool in tools:
        all_superuser_ids.update(tool.get('_superusers', []))
    
    print(f"Unique super users found: {len(all_superuser_ids)}")
    
    print()
    print(f"✓ Table saved to: {csv_filename}")
    print("=" * 60)

if __name__ == "__main__":
    main()
