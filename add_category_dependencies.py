#!/usr/bin/env python3
"""
Script to add dependencies to tools in specific categories.
Finds tools in categories: McCullough/*, Moore/*, Shriram/*, and Spilker/*
and adds them as fully_dependent_tools to resource ID 62 (Joint-NEMO-Launch).
"""

import requests
import json
import os
from dotenv import load_dotenv
from typing import List, Dict, Any

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoints
NEMO_RESOURCES_API_URL = "https://nemo.stanford.edu/api/resources/"
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

# Target resource ID to update
TARGET_RESOURCE_ID = 62
TARGET_RESOURCE_NAME = "Joint-NEMO-Launch"

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

def get_resource_by_id(resource_id: int) -> Dict[str, Any]:
    """Get a specific resource by ID from the API."""
    try:
        url = f"{NEMO_RESOURCES_API_URL}{resource_id}/"
        response = requests.get(url, headers=API_HEADERS)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"✗ Failed to get resource {resource_id}: HTTP {response.status_code}")
            return {}
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error getting resource {resource_id}: {e}")
        return {}

def update_resource_dependencies(resource_id: int, fully_dependent_tools: List[int]) -> bool:
    """Update a resource's fully_dependent_tools via the NEMO API."""
    update_url = f"{NEMO_RESOURCES_API_URL}{resource_id}/"
    try:
        payload = {
            'fully_dependent_tools': fully_dependent_tools
        }
        print(f"  Updating resource {resource_id} with {len(fully_dependent_tools)} dependencies")
        print(f"  Payload: {payload}")
        
        response = requests.patch(update_url, json=payload, headers=API_HEADERS)
        
        if response.status_code == 200:
            print(f"  ✓ Successfully updated resource {resource_id}")
            return True
        else:
            print(f"✗ Failed to update resource {resource_id}: HTTP {response.status_code}")
            print(f"  Error response: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error updating resource {resource_id}: {e}")
        return False

def main():
    """Main function to download resources, find matching tools, and update dependencies."""
    print("=" * 60)
    print("Adding category dependencies to Joint-NEMO-Launch")
    print("=" * 60)
    print(f"Target categories: {', '.join(TARGET_CATEGORIES)}")
    print(f"Target resource: ID {TARGET_RESOURCE_ID} ({TARGET_RESOURCE_NAME})")
    print("-" * 60)
    
    # Test API connections
    print("\nTesting API connections...")
    if not test_api_connection(NEMO_TOOLS_API_URL):
        print("Cannot proceed without valid tools API connection.")
        return
    
    if not test_api_connection(NEMO_RESOURCES_API_URL):
        print("Cannot proceed without valid resources API connection.")
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
    
    print(f"\n✓ Found {len(matching_tools)} tools matching target categories:")
    tool_ids = []
    for tool in matching_tools:
        tool_id = tool.get('id')
        tool_name = tool.get('name', 'Unknown')
        category = tool.get('_category', 'Unknown')
        # Convert category to string for display
        category_str = str(category) if category is not None else 'Unknown'
        if tool_id:
            tool_ids.append(tool_id)
            print(f"  - ID {tool_id}: {tool_name} (Category: {category_str})")
    
    if not tool_ids:
        print("No valid tool IDs found. Cannot proceed.")
        return
    
    # Get current resource state
    print(f"\nFetching current state of resource {TARGET_RESOURCE_ID}...")
    current_resource = get_resource_by_id(TARGET_RESOURCE_ID)
    
    if not current_resource:
        print(f"Could not fetch resource {TARGET_RESOURCE_ID}. Cannot proceed.")
        return
    
    print(f"Current resource: {current_resource.get('name', 'Unknown')}")
    current_dependencies = current_resource.get('fully_dependent_tools', [])
    print(f"Current fully_dependent_tools: {current_dependencies}")
    
    # Merge with existing dependencies (avoid duplicates)
    new_dependencies = list(set(current_dependencies + tool_ids))
    new_dependencies.sort()
    
    print(f"\nNew fully_dependent_tools will be: {new_dependencies}")
    print(f"  Adding {len(tool_ids)} new tool IDs")
    if len(new_dependencies) > len(current_dependencies):
        print(f"  Total dependencies: {len(new_dependencies)} (was {len(current_dependencies)})")
    
    # Update the resource
    print(f"\nUpdating resource {TARGET_RESOURCE_ID}...")
    if update_resource_dependencies(TARGET_RESOURCE_ID, new_dependencies):
        print(f"\n✓ Successfully updated resource {TARGET_RESOURCE_ID} ({TARGET_RESOURCE_NAME})")
        print(f"  Added {len(tool_ids)} tools as fully dependent")
    else:
        print(f"\n✗ Failed to update resource {TARGET_RESOURCE_ID}")
    
    print("\n" + "=" * 60)
    print("DEPENDENCY UPDATE SUMMARY")
    print("=" * 60)
    print(f"Tools found in target categories: {len(matching_tools)}")
    print(f"Tool IDs added: {len(tool_ids)}")
    print(f"Total dependencies after update: {len(new_dependencies)}")
    print("=" * 60)

if __name__ == "__main__":
    main()

