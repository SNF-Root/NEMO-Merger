#!/usr/bin/env python3
"""
Script to modify tool categories by adding 'Allen/' prefix except for 'moore' and 'SNSF' categories.
"""

import requests
import os
from dotenv import load_dotenv
from typing import List, Dict, Any

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoint for tools
NEMO_TOOLS_API_URL = "https://nemo-plan.stanford.edu/api/tools/"

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

def test_api_connection() -> bool:
    """Test the API connection and authentication."""
    try:
        response = requests.get(NEMO_TOOLS_API_URL, headers=API_HEADERS)
        if response.status_code == 200:
            print("✓ API connection successful")
            return True
        elif response.status_code == 401:
            print("✗ Authentication failed: Check your NEMO_TOKEN")
            return False
        else:
            print(f"✗ API connection failed: HTTP {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error connecting to API: {e}")
        return False

def download_tools() -> List[Dict[str, Any]]:
    """Download the list of tools from NEMO API."""
    try:
        response = requests.get(NEMO_TOOLS_API_URL, headers=API_HEADERS)
        if response.status_code == 200:
            tools = response.json()
            print(f"✓ Successfully downloaded {len(tools)} tools")
            return tools
        else:
            print(f"✗ Failed to download tools: HTTP {response.status_code}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error downloading tools: {e}")
        return []

def modify_tool_categories(tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:

    excluded_categories = ['Moore', 'SNSF','Spilker','SNSF-Removed','McCullough','Shriram','SNSF Admin']

    modified_tools = []

    for tool in tools:
        if '_category' in tool and tool['_category']:
            category = tool['_category']
            if category not in excluded_categories:
                if not category.startswith('Allen/'):
                    tool['_category'] = f'Allen/{category}'
        modified_tools.append(tool)

    return modified_tools

def update_tool(tool_id: int, payload: Dict[str, Any]) -> bool:
    """Update a tool's information via the NEMO API."""
    update_url = f"{NEMO_TOOLS_API_URL}{tool_id}/"
    try:
        print(f"  Sending payload: {payload}")
        response = requests.patch(update_url, json=payload, headers=API_HEADERS)
        if response.status_code == 200:
            print(f"  ✓ API response: {response.json()}")
            return True
        else:
            print(f"✗ Failed to update tool {tool_id}: HTTP {response.status_code} - {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error updating tool {tool_id}: {e}")
        return False

def main():
    """Main function to download and modify tool categories."""
    print("Starting tool category modification...")
    print(f"API Endpoint: {NEMO_TOOLS_API_URL}")
    print("-" * 60)

    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return

    # Download tools
    tools = download_tools()
    if not tools:
        print("No tools downloaded. Cannot proceed.")
        return

    # Modify tool categories
    modified_tools = modify_tool_categories(tools)
    
    # Update tools
    success_count = 0
    for tool in modified_tools:
        print(f"Debug: Tool {tool['id']} '{tool['name']}' has category: '{tool['_category']}'")
        if update_tool(tool['id'], {'_category': tool['_category']}):
            success_count += 1
            print(f"✓ Updated tool {tool['id']}: {tool['name']} → {tool['_category']}")
        else:
            print(f"✗ Failed to update tool {tool['id']}: {tool['name']}")

    print("\n" + "=" * 60)
    print("TOOL CATEGORY MODIFICATION SUMMARY")
    print("=" * 60)
    print(f"Total tools processed: {len(tools)}")
    print(f"Successfully updated: {success_count}")
    print(f"Failed updates: {len(tools) - success_count}")

if __name__ == "__main__":
    main()
