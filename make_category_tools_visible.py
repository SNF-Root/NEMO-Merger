#!/usr/bin/env python3
"""
Script to make tools visible in specific categories.
Finds tools in categories: McCullough/*, Moore/*, Shriram/*, and Spilker/*
and sets their visible field to true.
"""

import requests
import json
import os
from dotenv import load_dotenv
from typing import List, Dict, Any

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoints
NEMO_TOOLS_API_URL = "https://nemo.stanford.edu/api/tools/"

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

# Categories to filter (with wildcard support)
TARGET_CATEGORIES = [
    "McCullough/",
    "Moore/",
    "Shriram/",
    "Spilker/"
]

def test_api_connection(api_url: str) -> bool:
    """Test the API connection and authentication."""
    try:
        response = requests.get(api_url, headers=API_HEADERS)
        if response.status_code == 200:
            print(f"✓ API connection successful: {api_url}")
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

def download_tools(api_url: str) -> List[Dict[str, Any]]:
    """Download all tools from the NEMO API."""
    print(f"Downloading tools from {api_url}...")
    
    all_tools = []
    page = 1
    
    while True:
        try:
            # Add pagination parameters
            params = {'page': page}
            response = requests.get(api_url, headers=API_HEADERS, params=params)
            
            if response.status_code == 200:
                response_data = response.json()
                
                # Check if this is a paginated response
                if 'results' in response_data:
                    tools = response_data['results']
                    print(f"  Page {page}: Retrieved {len(tools)} tools")
                else:
                    # Direct list response
                    tools = response_data
                    print(f"  Retrieved {len(tools)} tools (no pagination)")
                
                if not tools:
                    break
                
                all_tools.extend(tools)
                
                # Check if there are more pages
                if 'next' in response_data and response_data['next']:
                    page += 1
                else:
                    break
                    
            elif response.status_code == 401:
                print("✗ Authentication failed: Check your NEMO_TOKEN")
                return []
            elif response.status_code == 403:
                print("✗ Permission denied: Check your API permissions")
                return []
            else:
                print(f"✗ Failed to download tools: HTTP {response.status_code}")
                print(f"  Response: {response.text[:200]}")
                return []
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Network error downloading tools: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"✗ Error parsing API response: {e}")
            return []
    
    print(f"✓ Total tools downloaded: {len(all_tools)}")
    return all_tools

def filter_tools_by_category(tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter tools that match the target categories."""
    matching_tools = []
    
    for tool in tools:
        # Get _category field (tools use _category, not category)
        category = tool.get('_category')
        
        # Handle case where category might be int, None, or string
        if category is not None:
            # Convert to string if it's not already
            category_str = str(category) if not isinstance(category, str) else category
            
            # Check if category matches any of our target patterns
            for target_cat in TARGET_CATEGORIES:
                if category_str.startswith(target_cat):
                    matching_tools.append(tool)
                    break
    
    return matching_tools

def update_tool_visibility(tool_id: int, visible: bool) -> bool:
    """Update a tool's visible field via the NEMO API."""
    update_url = f"{NEMO_TOOLS_API_URL}{tool_id}/"
    try:
        payload = {
            'visible': visible
        }
        
        response = requests.patch(update_url, json=payload, headers=API_HEADERS)
        
        if response.status_code == 200:
            return True
        else:
            print(f"✗ Failed to update tool {tool_id}: HTTP {response.status_code}")
            print(f"  Error response: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error updating tool {tool_id}: {e}")
        return False

def main():
    """Main function to download tools, find matching ones, and update visibility."""
    print("=" * 60)
    print("Making category tools visible")
    print("=" * 60)
    print(f"Target categories: {', '.join(TARGET_CATEGORIES)}")
    print("-" * 60)
    
    # Test API connection
    print("\nTesting API connection...")
    if not test_api_connection(NEMO_TOOLS_API_URL):
        print("Cannot proceed without valid tools API connection.")
        return
    
    # Download tools
    print("\nDownloading tools...")
    tools = download_tools(NEMO_TOOLS_API_URL)
    
    if not tools:
        print("No tools downloaded. Cannot proceed.")
        return
    
    # Filter tools by category
    print("\nFiltering tools by category...")
    matching_tools = filter_tools_by_category(tools)
    
    if not matching_tools:
        print("No tools found matching the target categories.")
        return
    
    # Filter to only tools that are currently not visible
    tools_to_update = [tool for tool in matching_tools if tool.get('visible') == False]
    
    print(f"\n✓ Found {len(matching_tools)} tools matching target categories")
    print(f"  {len(tools_to_update)} tools need visibility update (currently visible: false)")
    
    if not tools_to_update:
        print("All matching tools are already visible. Nothing to update.")
        return
    
    # Show preview of tools to update
    print(f"\nTools to update:")
    for tool in tools_to_update:
        tool_id = tool.get('id')
        tool_name = tool.get('name', 'Unknown')
        category = tool.get('_category', 'Unknown')
        category_str = str(category) if category is not None else 'Unknown'
        print(f"  - ID {tool_id}: {tool_name} (Category: {category_str})")
    
    # Update tools
    print(f"\nUpdating {len(tools_to_update)} tools...")
    success_count = 0
    failed_count = 0
    
    for tool in tools_to_update:
        tool_id = tool.get('id')
        tool_name = tool.get('name', 'Unknown')
        
        if update_tool_visibility(tool_id, True):
            success_count += 1
            print(f"  ✓ Updated tool {tool_id}: {tool_name}")
        else:
            failed_count += 1
            print(f"  ✗ Failed to update tool {tool_id}: {tool_name}")
    
    print("\n" + "=" * 60)
    print("VISIBILITY UPDATE SUMMARY")
    print("=" * 60)
    print(f"Tools found in target categories: {len(matching_tools)}")
    print(f"Tools needing update: {len(tools_to_update)}")
    print(f"Successfully updated: {success_count}")
    print(f"Failed updates: {failed_count}")
    print("=" * 60)

if __name__ == "__main__":
    main()

