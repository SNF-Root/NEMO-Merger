#!/usr/bin/env python3
"""
Script to remove 'Allen/' prefix from tools with any nested category structure.
This removes 'Allen/' from tools with categories like: Allen/Spilker, Allen/SNSF/Admin, Allen/Shriram/Equipment, etc.
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

def find_tools_to_fix(tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Find tools that need the 'Allen/' prefix removed."""
    tools_to_fix = []

    for tool in tools:
        if '_category' in tool and tool['_category']:
            category = tool['_category']
            if category.startswith('Allen/'):
                # Remove 'Allen/' prefix from any nested structure
                new_category = category.replace('Allen/', '', 1)  # Only replace first occurrence
                tool['new_category'] = new_category
                tools_to_fix.append(tool)
                print(f"Found tool to fix: {tool['name']} - {category} → {new_category}")

    return tools_to_fix

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
    """Main function to remove 'Allen/' prefix from specific tools."""
    print("Starting removal of 'Allen/' prefix from specific tools...")
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

    # Find tools that need fixing
    tools_to_fix = find_tools_to_fix(tools)
    
    if not tools_to_fix:
        print("✓ No tools found that need the 'Allen/' prefix removed.")
        return

    print(f"\nFound {len(tools_to_fix)} tools that need fixing.")
    
    # Update tools
    success_count = 0
    for tool in tools_to_fix:
        print(f"\nFixing tool {tool['id']}: {tool['name']}")
        if update_tool(tool['id'], {'_category': tool['new_category']}):
            success_count += 1
            print(f"✓ Updated tool {tool['id']}: {tool['name']} → {tool['new_category']}")
        else:
            print(f"✗ Failed to update tool {tool['id']}: {tool['name']}")

    print("\n" + "=" * 60)
    print("ALLEN PREFIX REMOVAL SUMMARY")
    print("=" * 60)
    print(f"Total tools found needing fixes: {len(tools_to_fix)}")
    print(f"Successfully updated: {success_count}")
    print(f"Failed updates: {len(tools_to_fix) - success_count}")

if __name__ == "__main__":
    main()
