#!/usr/bin/env python3
"""
Script to push tools from SNSF Excel files to NEMO API endpoint.
Reads tool names from SNC, SNL, and SMF Tools.xlsx files and pushes them to the API.
"""

import pandas as pd
import requests
import json
import os
from typing import List, Dict, Any
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoint
NEMO_API_URL = "https://nemo-plan.stanford.edu/api/tools/"

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

# Template for tool data (only name will be filled, rest will be filled manually)
TOOL_TEMPLATE = {
    "id": None,  # Will be assigned by API
    "name": "",  # This is what we'll fill from Excel
    "visible": True,
    "_description": "",  # Will be filled manually
    "_serial": "",  # Changed from None to empty string
    "_image": "",  # Changed from None to empty string
    "_tool_calendar_color": "#33ad33",
    "_category": "SNSF",  # Will be filled manually
    "_operational": True,
    "_properties": "",  # Changed from None to empty string
    "_location": "",  # Will be filled manually
    "_phone_number": "",  # Changed from None to empty string
    "_notification_email_address": "",  # Changed from None to empty string
    "_qualifications_never_expire": False,
    "_ask_to_leave_area_when_done_using": False,
    "_grant_badge_reader_access_upon_qualification": "",  # Changed from None to empty string
    "_reservation_horizon": 7,
    "_minimum_usage_block_time": "",  # Changed from None to empty string
    "_maximum_usage_block_time": "",  # Changed from None to empty string
    "_maximum_reservations_per_day": "",  # Changed from None to empty string
    "_maximum_future_reservations": "",  # Changed from None to empty string
    "_minimum_time_between_reservations": "",  # Changed from None to empty string
    "_maximum_future_reservation_time": "",  # Changed from None to empty string
    "_missed_reservation_threshold": "",  # Changed from None to empty string
    "_max_delayed_logoff": "",  # Changed from None to empty string
    "_logout_grace_period": "",  # Changed from None to empty string
    "_reservation_required": False,
    "_pre_usage_questions": "",
    "_post_usage_questions": "",
    "_policy_off_between_times": False,
    "_policy_off_start_time": "",  # Changed from None to empty string
    "_policy_off_end_time": "",  # Changed from None to empty string
    "_policy_off_weekend": False,
    "_operation_mode": 0,
    "_allow_user_shadowing_verification_request": True,
    "parent_tool": "",  # Changed from None to empty string
    "_primary_owner": "400",  # Changed from None to empty string
    "_interlock": "",  # Changed from None to empty string
    "_requires_area_access": "",  # Changed from None to empty string
    "_grant_physical_access_level_upon_qualification": "",  # Changed from None to empty string
    "_backup_owners": [],
    "_superusers": [],
    "_staff": [],
    "_adjustment_request_reviewers": [],
    "_grant_access_for_qualification_levels": [],
    "_shadowing_verification_request_qualification_levels": [],
    "_shadowing_verification_reviewers": []
}

def read_tool_names_from_excel(file_path: str) -> List[str]:
    """Read tool names from an Excel file."""
    try:
        df = pd.read_excel(file_path)
        if 'name' in df.columns:
            # Filter out NaN values and empty strings
            tool_names = df['name'].dropna().astype(str).str.strip()
            tool_names = tool_names[tool_names != ''].tolist()
            print(f"Found {len(tool_names)} tools in {file_path}")
            return tool_names
        else:
            print(f"Warning: No 'name' column found in {file_path}")
            return []
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []

def create_tool_payload(tool_name: str) -> Dict[str, Any]:
    """Create a tool payload with the given name."""
    payload = TOOL_TEMPLATE.copy()
    payload["name"] = tool_name
    
    # Clean up the payload - remove empty strings that might cause API issues
    cleaned_payload = {}
    for key, value in payload.items():
        if value == "":
            # Skip empty strings for optional fields
            continue
        cleaned_payload[key] = value
    
    return cleaned_payload

def push_tool_to_api(tool_name: str, api_url: str) -> bool:
    """Push a single tool to the NEMO API."""
    payload = create_tool_payload(tool_name)
    
    # Debug: Print the payload being sent (for first few tools)
    if tool_name in ["Occupancy: 3D Printer and Wirebonder", "Spilker 004 Office Computer", "SPM Park NX-10"]:
        print(f"Debug - Payload for {tool_name}: {json.dumps(payload, indent=2)}")
    
    try:
        response = requests.post(api_url, json=payload, headers=API_HEADERS)
        
        if response.status_code == 201:  # Created
            print(f"✓ Successfully pushed tool: {tool_name}")
            return True
        elif response.status_code == 400:
            print(f"✗ Bad request for tool '{tool_name}': {response.text}")
            return False
        elif response.status_code == 401:
            print(f"✗ Authentication failed for tool '{tool_name}': Check your NEMO_TOKEN")
            return False
        elif response.status_code == 403:
            print(f"✗ Permission denied for tool '{tool_name}': Check your API permissions")
            return False
        elif response.status_code == 409:
            print(f"⚠ Tool '{tool_name}' already exists (conflict)")
            return False
        else:
            print(f"✗ Failed to push tool '{tool_name}': HTTP {response.status_code} - {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error pushing tool '{tool_name}': {e}")
        return False

def test_api_connection():
    """Test the API connection and authentication."""
    try:
        response = requests.get(NEMO_API_URL, headers=API_HEADERS)
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

def main():
    """Main function to read tools and push to API."""
    print("Starting tool push to NEMO API...")
    print(f"API Endpoint: {NEMO_API_URL}")
    print("-" * 50)
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Excel files to process
    excel_files = [
        "SNSF-Data/SNC Tools.xlsx",
        "SNSF-Data/SNL Tools.xlsx", 
        "SNSF-Data/SMF Tools.xlsx"
    ]
    
    all_tool_names = []
    
    # Read tool names from all Excel files
    for file_path in excel_files:
        tool_names = read_tool_names_from_excel(file_path)
        all_tool_names.extend(tool_names)
    
    print(f"\nTotal unique tools found: {len(set(all_tool_names))}")
    print("-" * 50)
    
    if not all_tool_names:
        print("No tools found to push!")
        return
    
    print(f"\nReady to push {len(all_tool_names)} tools to NEMO API...")
    
    # Push tools to API
    successful_pushes = 0
    failed_pushes = 0
    
    for i, tool_name in enumerate(all_tool_names, 1):
        print(f"\n[{i}/{len(all_tool_names)}] Pushing: {tool_name}")
        
        if push_tool_to_api(tool_name, NEMO_API_URL):
            successful_pushes += 1
        else:
            failed_pushes += 1
        
        # Add a small delay to avoid overwhelming the API
        time.sleep(0.5)
    
    # Summary
    print("\n" + "=" * 50)
    print("PUSH SUMMARY")
    print("=" * 50)
    print(f"Total tools processed: {len(all_tool_names)}")
    print(f"Successful pushes: {successful_pushes}")
    print(f"Failed pushes: {failed_pushes}")
    print(f"Success rate: {(successful_pushes/len(all_tool_names)*100):.1f}%")
    
    if failed_pushes > 0:
        print(f"\nNote: {failed_pushes} tools failed to push.")
        print("These may need to be added manually or have their data corrected.")

if __name__ == "__main__":
    main()

