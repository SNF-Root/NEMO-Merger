#!/usr/bin/env python3
"""
Script to push tools from downloaded NEMO tools JSON to NEMO API endpoint.
Reads tools from tools_download.json, excludes Allen/* category tools, and pushes them to the API.
"""

import requests
import json
import os
from typing import List, Dict, Any, Tuple
import time
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# NEMO API endpoint
NEMO_API_URL = "https://nemo.stanford.edu/api/tools/"

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

# Default primary owner ID
DEFAULT_PRIMARY_OWNER = 46

def read_tools_from_json(file_path: str) -> List[Dict[str, Any]]:
    """Read tools from downloaded JSON file, excluding Allen/* category tools."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            all_tools = json.load(f)
        
        # Filter out tools with Allen/* category prefix
        filtered_tools = []
        excluded_count = 0
        
        for tool in all_tools:
            category = tool.get('_category', '')
            if category and str(category).startswith('Allen/'):
                excluded_count += 1
                continue
            filtered_tools.append(tool)
        
        print(f"Found {len(all_tools)} total tools in {file_path}")
        print(f"Excluded {excluded_count} tools with Allen/* category prefix")
        print(f"Processing {len(filtered_tools)} tools")
        return filtered_tools
    except FileNotFoundError:
        print(f"Error: File {file_path} not found")
        return []
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON from {file_path}: {e}")
        return []
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []

def clean_tool_payload(tool: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and prepare tool payload for API, removing id and setting default primary owner.
    Keeps the structure matching the API response format with null values preserved.
    """
    # Create a copy to avoid modifying the original
    payload = tool.copy()
    
    # Remove id field (API will assign it)
    payload.pop('id', None)
    
    # Set default primary owner
    payload['_primary_owner'] = DEFAULT_PRIMARY_OWNER
    
    # Set visible to false
    payload['visible'] = False
    
    # Keep null values as null - don't convert to empty strings
    # The API expects null for optional fields, not empty strings
    
    # For _properties, ensure it's a dict (convert null to empty dict)
    if '_properties' in payload:
        if payload['_properties'] is None:
            payload['_properties'] = {}
        elif not isinstance(payload['_properties'], dict):
            # If it's a string, try to parse it, otherwise use empty dict
            if isinstance(payload['_properties'], str) and payload['_properties'].strip():
                try:
                    payload['_properties'] = json.loads(payload['_properties'])
                except:
                    payload['_properties'] = {}
            else:
                payload['_properties'] = {}
    
    # For _image, if it's null, omit it from the payload
    # The API doesn't accept null for _image field
    if '_image' in payload and payload['_image'] is None:
        payload.pop('_image', None)
    
    # For string fields that are None, keep them as None (null in JSON)
    # Don't convert to empty strings as the API expects null for optional fields
    
    return payload

def push_tool_to_api(tool: Dict[str, Any], api_url: str, logger: logging.Logger) -> Tuple[bool, Dict[str, Any]]:
    """Push a single tool to the NEMO API.
    
    Returns:
        Tuple of (success: bool, response_data: Dict)
    """
    tool_name = tool.get('name', 'Unknown')
    payload = clean_tool_payload(tool)
    
    try:
        response = requests.post(api_url, json=payload, headers=API_HEADERS)
        
        if response.status_code == 201:  # Created
            response_data = response.json()
            tool_id = response_data.get('id', 'Unknown')
            print(f"✓ Successfully pushed tool: {tool_name}")
            logger.info(f"SUCCESS - Tool '{tool_name}' created with ID: {tool_id}")
            logger.debug(f"Tool payload: {json.dumps(payload, indent=2)}")
            logger.debug(f"API response: {json.dumps(response_data, indent=2)}")
            return True, response_data
        elif response.status_code == 400:
            error_msg = response.text
            print(f"✗ Bad request for tool '{tool_name}': {error_msg}")
            logger.error(f"BAD REQUEST - Tool '{tool_name}': {error_msg}")
            logger.debug(f"Tool payload: {json.dumps(payload, indent=2)}")
            return False, {}
        elif response.status_code == 401:
            error_msg = "Authentication failed: Check your NEMO_TOKEN"
            print(f"✗ Authentication failed for tool '{tool_name}': Check your NEMO_TOKEN")
            logger.error(f"AUTHENTICATION FAILED - Tool '{tool_name}': {error_msg}")
            return False, {}
        elif response.status_code == 403:
            error_msg = "Permission denied: Check your API permissions"
            print(f"✗ Permission denied for tool '{tool_name}': Check your API permissions")
            logger.error(f"PERMISSION DENIED - Tool '{tool_name}': {error_msg}")
            return False, {}
        elif response.status_code == 409:
            error_msg = "Tool already exists (conflict)"
            print(f"⚠ Tool '{tool_name}' already exists (conflict)")
            logger.warning(f"CONFLICT - Tool '{tool_name}' already exists")
            return False, {}
        else:
            error_msg = f"HTTP {response.status_code} - {response.text}"
            print(f"✗ Failed to push tool '{tool_name}': {error_msg}")
            logger.error(f"FAILED - Tool '{tool_name}': {error_msg}")
            logger.debug(f"Tool payload: {json.dumps(payload, indent=2)}")
            return False, {}
            
    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        print(f"✗ Network error pushing tool '{tool_name}': {error_msg}")
        logger.error(f"NETWORK ERROR - Tool '{tool_name}': {error_msg}")
        return False, {}

def test_api_connection(logger: logging.Logger) -> bool:
    """Test the API connection and authentication."""
    try:
        response = requests.get(NEMO_API_URL, headers=API_HEADERS)
        if response.status_code == 200:
            print("✓ API connection successful")
            logger.info("API connection test: SUCCESS")
            return True
        elif response.status_code == 401:
            print("✗ Authentication failed: Check your NEMO_TOKEN")
            logger.error("API connection test: AUTHENTICATION FAILED")
            return False
        elif response.status_code == 403:
            print("✗ Permission denied: Check your API permissions")
            logger.error("API connection test: PERMISSION DENIED")
            return False
        else:
            print(f"✗ API connection failed: HTTP {response.status_code}")
            logger.error(f"API connection test: FAILED - HTTP {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error connecting to API: {e}")
        logger.error(f"API connection test: NETWORK ERROR - {e}")
        return False

def setup_logging() -> Tuple[logging.Logger, str]:
    """Set up logging to file with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"tool_creation_log_{timestamp}.log"
    json_log_filename = f"created_tools_{timestamp}.json"
    
    # Create logger
    logger = logging.getLogger('tool_creation')
    logger.setLevel(logging.DEBUG)
    
    # Remove existing handlers
    logger.handlers = []
    
    # File handler for detailed logging
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    
    # Console handler for important messages
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info("=" * 60)
    logger.info("TOOL CREATION SESSION STARTED")
    logger.info("=" * 60)
    logger.info(f"Log file: {log_filename}")
    logger.info(f"JSON log file: {json_log_filename}")
    
    return logger, json_log_filename

def main():
    """Main function to read tools from JSON and push to API."""
    # Set up logging
    logger, json_log_filename = setup_logging()
    
    print("Starting tool push to NEMO API...")
    print(f"API Endpoint: {NEMO_API_URL}")
    print(f"Default Primary Owner: {DEFAULT_PRIMARY_OWNER}")
    print("-" * 50)
    logger.info(f"API Endpoint: {NEMO_API_URL}")
    logger.info(f"Default Primary Owner: {DEFAULT_PRIMARY_OWNER}")
    
    # Test API connection first
    if not test_api_connection(logger):
        print("Cannot proceed without valid API connection.")
        logger.error("Cannot proceed without valid API connection.")
        return
    
    # Read tools from downloaded JSON file
    json_file = "tools_download.json"
    logger.info(f"Reading tools from: {json_file}")
    tools = read_tools_from_json(json_file)
    
    if not tools:
        print("No tools found to push!")
        logger.warning("No tools found to push!")
        return
    
    logger.info(f"Total tools to process: {len(tools)}")
    print(f"\nReady to push {len(tools)} tools to NEMO API...")
    print("-" * 50)
    
    # Push tools to API
    successful_pushes = 0
    failed_pushes = 0
    created_tools = []
    
    for i, tool in enumerate(tools, 1):
        tool_name = tool.get('name', 'Unknown')
        category = tool.get('_category', '')
        location = tool.get('_location', '')
        
        location_str = f" (location: {location})" if location else ""
        category_str = f" (category: {category})" if category else ""
        print(f"\n[{i}/{len(tools)}] Pushing: {tool_name}{category_str}{location_str}")
        logger.info(f"[{i}/{len(tools)}] Processing tool: {tool_name}")
        
        success, response_data = push_tool_to_api(tool, NEMO_API_URL, logger)
        
        if success:
            successful_pushes += 1
            # Store created tool info
            created_tool = {
                'original_tool': tool,
                'created_tool': response_data,
                'timestamp': datetime.now().isoformat(),
                'status': 'SUCCESS'
            }
            created_tools.append(created_tool)
        else:
            failed_pushes += 1
            # Store failed tool info
            failed_tool = {
                'original_tool': tool,
                'timestamp': datetime.now().isoformat(),
                'status': 'FAILED'
            }
            created_tools.append(failed_tool)
        
        # Add a small delay to avoid overwhelming the API
        time.sleep(0.5)
    
    # Save created tools to JSON file
    try:
        with open(json_log_filename, 'w', encoding='utf-8') as f:
            json.dump({
                'session_timestamp': datetime.now().isoformat(),
                'total_processed': len(tools),
                'successful_creates': successful_pushes,
                'failed_creates': failed_pushes,
                'success_rate': f"{(successful_pushes/len(tools)*100):.1f}%",
                'tools': created_tools
            }, f, indent=2, ensure_ascii=False)
        logger.info(f"Created tools saved to: {json_log_filename}")
    except Exception as e:
        logger.error(f"Error saving JSON log file: {e}")
    
    # Summary
    print("\n" + "=" * 50)
    print("PUSH SUMMARY")
    print("=" * 50)
    print(f"Total tools processed: {len(tools)}")
    print(f"Successful pushes: {successful_pushes}")
    print(f"Failed pushes: {failed_pushes}")
    print(f"Success rate: {(successful_pushes/len(tools)*100):.1f}%")
    print(f"\n✓ Detailed log saved to: tool_creation_log_*.log")
    print(f"✓ Created tools JSON saved to: {json_log_filename}")
    
    logger.info("=" * 60)
    logger.info("TOOL CREATION SESSION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total tools processed: {len(tools)}")
    logger.info(f"Successful pushes: {successful_pushes}")
    logger.info(f"Failed pushes: {failed_pushes}")
    logger.info(f"Success rate: {(successful_pushes/len(tools)*100):.1f}%")
    logger.info("=" * 60)
    logger.info("TOOL CREATION SESSION ENDED")
    logger.info("=" * 60)
    
    if failed_pushes > 0:
        print(f"\nNote: {failed_pushes} tools failed to push.")
        print("These may need to be added manually or have their data corrected.")
        logger.warning(f"{failed_pushes} tools failed to push. Check log for details.")

if __name__ == "__main__":
    main()

