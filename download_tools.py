#!/usr/bin/env python3
"""
Script to download all tools from the NEMO API and create a lookup mapping.
This creates a mapping from tool names to tool IDs for use in rate creation.
"""

import requests
import json
import os
from dotenv import load_dotenv
from typing import Dict, Any

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoint for tools
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

def test_api_connection():
    """Test the API connection and authentication."""
    try:
        response = requests.get(NEMO_TOOLS_API_URL, headers=API_HEADERS)
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

def download_tools(api_url: str) -> list:
    """Download all tools from the NEMO API."""
    print("Downloading tools from NEMO API...")
    
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
                return []
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Network error downloading tools: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"✗ Error parsing API response: {e}")
            return []
    
    print(f"✓ Total tools downloaded: {len(all_tools)}")
    return all_tools

def save_tools_to_json(tools: list, filename: str = "tools_download.json"):
    """Save the downloaded tools to a JSON file."""
    try:
        with open(filename, 'w') as f:
            json.dump(tools, f, indent=2)
        print(f"✓ Tools saved to {filename}")
    except Exception as e:
        print(f"✗ Error saving tools to {filename}: {e}")

def create_tool_lookup(tools: list) -> Dict[str, int]:
    """Create a lookup mapping from tool names to tool IDs."""
    tool_lookup = {}
    
    for tool in tools:
        if 'name' in tool and 'id' in tool:
            tool_name = tool['name']
            tool_id = tool['id']
            tool_lookup[tool_name] = tool_id
    
    print(f"✓ Created tool lookup with {len(tool_lookup)} tools")
    return tool_lookup

def save_tool_lookup(tool_lookup: Dict[str, int], filename: str = "tool_lookup.json"):
    """Save the tool lookup to a JSON file."""
    try:
        with open(filename, 'w') as f:
            json.dump(tool_lookup, f, indent=2)
        print(f"✓ Tool lookup saved to {filename}")
    except Exception as e:
        print(f"✗ Error saving tool lookup to {filename}: {e}")

def main():
    """Main function to download tools and create lookup."""
    print("Starting tool download from NEMO API...")
    print(f"API Endpoint: {NEMO_TOOLS_API_URL}")
    print("-" * 60)
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Download tools
    tools = download_tools(NEMO_TOOLS_API_URL)
    
    if not tools:
        print("No tools downloaded. Cannot proceed.")
        return
    
    # Save raw tools data
    save_tools_to_json(tools)
    
    # Create and save tool lookup
    tool_lookup = create_tool_lookup(tools)
    save_tool_lookup(tool_lookup)
    
    # Show sample of the lookup
    print("\nSample tool lookup (first 10 tools):")
    count = 0
    for tool_name, tool_id in tool_lookup.items():
        if count < 10:
            print(f"  {tool_name} → ID {tool_id}")
            count += 1
        else:
            break
    
    if len(tool_lookup) > 10:
        print(f"  ... and {len(tool_lookup) - 10} more tools")
    
    print("\n" + "=" * 60)
    print("TOOL DOWNLOAD SUMMARY")
    print("=" * 60)
    print(f"Total tools downloaded: {len(tools)}")
    print(f"Tools in lookup: {len(tool_lookup)}")
    print(f"✓ Tool lookup ready for use in rate creation!")
    print("\nFiles created:")
    print(f"  - tools_download.json (raw tool data)")
    print(f"  - tool_lookup.json (name → ID mapping)")

if __name__ == "__main__":
    main()
