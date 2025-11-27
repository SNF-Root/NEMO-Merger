#!/usr/bin/env python3
"""
Script to assign core facilities to tools based on CSV data.
1. Downloads core facilities from API
2. Downloads or loads tools
3. Reads CSV with tool names and core facility names (like "XSA", "SMF", "FAB", "EIM")
4. Matches tool names to tool IDs and core facility names to core facility IDs
5. Assigns core facilities to tools using the 'core_facility' field

Note: The CSV 'core_facility' column contains facility names that match the 'name' field
in the core facilities API (e.g., "XSA", "SMF", "FAB", "EIM").
"""

import requests
import json
import os
import logging
import pandas as pd
import argparse
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional

# Load environment variables from .env file
load_dotenv()

# Set up logging first (before token check so we can log errors)
log_filename = f"logs/assign_core_facilities_to_tools_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
os.makedirs('logs', exist_ok=True)  # Ensure logs directory exists
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()  # Also log to console
    ]
)
logger = logging.getLogger(__name__)

# NEMO API endpoints
NEMO_CORE_FACILITIES_API_URL = "https://nemo.stanford.edu/api/billing/core_facilities/"
NEMO_TOOLS_API_URL = "https://nemo.stanford.edu/api/tools/"

# Get NEMO token from environment
NEMO_TOKEN = os.getenv('NEMO_TOKEN')
if not NEMO_TOKEN:
    print("Error: NEMO_TOKEN not found in environment variables or .env file")
    print("Please create a .env file with: NEMO_TOKEN=your_token_here")
    print("Or set the environment variable: export NEMO_TOKEN=your_token_here")
    logger.error("NEMO_TOKEN not found in environment variables or .env file")
    exit(1)
else:
    logger.info("NEMO_TOKEN found in environment")

# API headers with authentication
API_HEADERS = {
    'Authorization': f'Token {NEMO_TOKEN}',
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

def test_api_connection(api_url: str, name: str) -> bool:
    """Test the API connection and authentication."""
    logger.info(f"Testing {name} API connection: {api_url}")
    try:
        response = requests.get(api_url, headers=API_HEADERS)
        if response.status_code == 200:
            print(f"✓ {name} API connection successful")
            logger.info(f"{name} API connection test successful")
            return True
        elif response.status_code == 401:
            print(f"✗ Authentication failed for {name}: Check your NEMO_TOKEN")
            logger.error(f"{name} API authentication failed: Check your NEMO_TOKEN")
            return False
        elif response.status_code == 403:
            print(f"✗ Permission denied for {name}: Check your API permissions")
            logger.error(f"{name} API permission denied: Check your API permissions")
            return False
        else:
            print(f"✗ {name} API connection failed: HTTP {response.status_code}")
            logger.error(f"{name} API connection failed: HTTP {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error connecting to {name} API: {e}")
        logger.error(f"Network error connecting to {name} API: {e}")
        return False

def download_all_items(api_url: str, item_name: str) -> List[Dict[str, Any]]:
    """Download all items from a NEMO API endpoint."""
    print(f"Downloading {item_name} from {api_url}...")
    logger.info(f"Downloading {item_name} from {api_url}")
    
    all_items = []
    page = 1
    
    while True:
        try:
            params = {'page': page}
            response = requests.get(api_url, headers=API_HEADERS, params=params)
            logger.debug(f"{item_name} API response status: {response.status_code}")
            
            if response.status_code == 200:
                response_data = response.json()
                
                # Handle both list response and paginated response format
                if isinstance(response_data, list):
                    items = response_data
                elif 'results' in response_data:
                    items = response_data['results']
                else:
                    items = response_data
                
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
                logger.error(f"Authentication failed downloading {item_name}: Check your NEMO_TOKEN")
                return []
            elif response.status_code == 403:
                print(f"✗ Permission denied: Check your API permissions")
                logger.error(f"Permission denied downloading {item_name}: Check your API permissions")
                return []
            else:
                print(f"✗ Failed to download {item_name}: HTTP {response.status_code}")
                logger.error(f"Failed to download {item_name}: HTTP {response.status_code} - {response.text[:200]}")
                return []
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Network error downloading {item_name}: {e}")
            logger.error(f"Network error downloading {item_name}: {e}", exc_info=True)
            return []
        except json.JSONDecodeError as e:
            print(f"✗ Error parsing API response: {e}")
            logger.error(f"Error parsing API response for {item_name}: {e}", exc_info=True)
            return []
    
    print(f"  Retrieved {len(all_items)} {item_name}")
    logger.info(f"Successfully downloaded {len(all_items)} {item_name}")
    return all_items

def load_tools_from_file(filename: str = "tools_download.json") -> Optional[List[Dict[str, Any]]]:
    """Load tools from a local JSON file if it exists."""
    if os.path.exists(filename):
        try:
            with open(filename, 'r') as f:
                tools = json.load(f)
            print(f"✓ Loaded {len(tools)} tools from {filename}")
            logger.info(f"Loaded {len(tools)} tools from {filename}")
            return tools
        except Exception as e:
            print(f"✗ Error loading tools from {filename}: {e}")
            logger.error(f"Error loading tools from {filename}: {e}")
            return None
    return None

def create_tool_lookup(tools: List[Dict[str, Any]]) -> Dict[str, int]:
    """Create a lookup mapping from tool names to tool IDs."""
    logger.info(f"Creating tool lookup from {len(tools)} tools")
    lookup = {}
    duplicate_count = 0
    
    for tool in tools:
        tool_id = tool.get('id')
        tool_name = tool.get('name', '').strip()
        
        if tool_id and tool_name:
            # Handle duplicate names - keep the first one
            if tool_name not in lookup:
                lookup[tool_name] = tool_id
                logger.debug(f"Mapped tool '{tool_name}' -> ID {tool_id}")
            else:
                duplicate_count += 1
                print(f"⚠ Warning: Duplicate tool name '{tool_name}' (IDs: {lookup[tool_name]}, {tool_id})")
                logger.warning(f"Duplicate tool name '{tool_name}' (IDs: {lookup[tool_name]}, {tool_id})")
        else:
            logger.debug(f"Skipping tool with missing ID or name: {tool}")
    
    print(f"✓ Created tool lookup with {len(lookup)} entries")
    logger.info(f"Created tool lookup with {len(lookup)} entries ({duplicate_count} duplicates found)")
    return lookup

def create_core_facility_lookup(core_facilities: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Create a lookup mapping from core facility names to core facility IDs.
    The CSV uses facility names (like "XSA", "SMF", "FAB", "EIM") which match the 'name' field in the API.
    """
    logger.info(f"Creating core facility lookup from {len(core_facilities)} facilities")
    lookup = {}
    duplicate_count = 0
    
    for facility in core_facilities:
        facility_id = facility.get('id')
        facility_name = facility.get('name', '').strip()
        
        if facility_id and facility_name:
            # Match by name (the CSV codes like "XSA" match the facility names)
            if facility_name not in lookup:
                lookup[facility_name] = facility_id
                logger.debug(f"Mapped core facility name '{facility_name}' -> ID {facility_id}")
            else:
                duplicate_count += 1
                print(f"⚠ Warning: Duplicate core facility name '{facility_name}' (IDs: {lookup[facility_name]}, {facility_id})")
                logger.warning(f"Duplicate core facility name '{facility_name}' (IDs: {lookup[facility_name]}, {facility_id})")
        else:
            logger.debug(f"Skipping facility with missing ID or name: {facility}")
    
    print(f"✓ Created core facility lookup with {len(lookup)} entries")
    logger.info(f"Created core facility lookup with {len(lookup)} entries ({duplicate_count} duplicates found)")
    return lookup

def read_csv_file(filename: str) -> List[Dict[str, Any]]:
    """Read CSV file and return list of rows."""
    print(f"Reading CSV from {filename}...")
    logger.info(f"Reading CSV from {filename}")
    
    try:
        df = pd.read_csv(filename)
        
        # Convert to list of dictionaries
        rows = df.to_dict('records')
        
        print(f"✓ Read {len(rows)} rows from {filename}")
        logger.info(f"Read {len(rows)} rows from {filename}")
        
        # Show column names for debugging
        print(f"  Columns found: {', '.join(df.columns.tolist())}")
        logger.info(f"Columns found: {', '.join(df.columns.tolist())}")
        
        return rows
        
    except Exception as e:
        print(f"✗ Error reading CSV: {e}")
        logger.error(f"Error reading CSV {filename}: {e}", exc_info=True)
        return []

def process_csv_rows(rows: List[Dict[str, Any]], tool_lookup: Dict[str, int], core_facility_lookup: Dict[str, int]) -> List[Dict[str, Any]]:
    """
    Process CSV rows and match tool names to tool IDs and core facility names (from CSV) to core facility IDs.
    Note: The CSV 'core_facility' column contains facility names like "XSA", "SMF", "FAB", "EIM" which match
    the 'name' field in the core facilities API.
    Returns list of assignments to make.
    """
    logger.info(f"Processing {len(rows)} CSV rows")
    assignments = []
    missing_tool_count = 0
    missing_facility_count = 0
    
    # Find column names (case-insensitive)
    if not rows:
        return assignments
    
    df_columns = list(rows[0].keys())
    tool_col = None
    facility_col = None
    
    # Find tool column
    for col in df_columns:
        if col.lower().strip() in ['tool', 'tool name', 'tool_name', 'equipment']:
            tool_col = col
            break
    
    # Find core facility column
    for col in df_columns:
        if col.lower().strip() in ['core_facility', 'core facility', 'facility', 'facility_code', 'facility code']:
            facility_col = col
            break
    
    if not tool_col:
        print("✗ Error: Could not find 'tool' column in CSV")
        logger.error("Could not find 'tool' column in CSV")
        return assignments
    
    if not facility_col:
        print("✗ Error: Could not find 'core_facility' column in CSV")
        logger.error("Could not find 'core_facility' column in CSV")
        return assignments
    
    print(f"  Using column '{tool_col}' for tool names")
    print(f"  Using column '{facility_col}' for core facility names")
    logger.info(f"Using column '{tool_col}' for tool names")
    logger.info(f"Using column '{facility_col}' for core facility names")
    
    for idx, row in enumerate(rows, 1):
        # Get tool name and core facility name (from CSV)
        tool_name = str(row.get(tool_col, '')).strip()
        facility_name = str(row.get(facility_col, '')).strip()
        
        # Skip rows with missing data
        if not tool_name or tool_name.lower() in ['nan', 'none', 'null', '']:
            logger.debug(f"Row {idx}: Skipping - missing tool name")
            continue
        
        if not facility_name or facility_name.lower() in ['nan', 'none', 'null', '']:
            logger.debug(f"Row {idx}: Skipping - missing core facility name")
            continue
        
        # Match tool name to tool ID
        tool_id = tool_lookup.get(tool_name)
        if not tool_id:
            missing_tool_count += 1
            print(f"⚠ Row {idx}: Tool '{tool_name}' not found in tool lookup")
            logger.warning(f"Row {idx}: Tool '{tool_name}' not found in tool lookup")
            continue
        
        # Match core facility name to core facility ID (CSV names like "XSA" match API facility names)
        facility_id = core_facility_lookup.get(facility_name)
        if not facility_id:
            missing_facility_count += 1
            print(f"⚠ Row {idx}: Core facility '{facility_name}' not found in core facility lookup")
            logger.warning(f"Row {idx}: Core facility '{facility_name}' not found in core facility lookup")
            continue
        
        # Add assignment
        assignments.append({
            'tool_name': tool_name,
            'tool_id': tool_id,
            'facility_name': facility_name,
            'facility_id': facility_id,
            'row': idx
        })
        logger.debug(f"Row {idx}: Matched '{tool_name}' (ID: {tool_id}) -> '{facility_name}' (Facility ID: {facility_id})")
    
    print(f"✓ Processed {len(rows)} rows: {len(assignments)} valid assignments")
    if missing_tool_count > 0:
        print(f"  ⚠ {missing_tool_count} rows with missing tools")
    if missing_facility_count > 0:
        print(f"  ⚠ {missing_facility_count} rows with missing core facilities")
    
    logger.info(f"Processed {len(rows)} rows: {len(assignments)} valid assignments ({missing_tool_count} missing tools, {missing_facility_count} missing facilities)")
    return assignments

def update_tool_core_facility(tool_id: int, facility_id: int, tool_name: str = None, facility_name: str = None) -> bool:
    """Update a tool's core facility field via the NEMO API."""
    update_url = f"{NEMO_TOOLS_API_URL}{tool_id}/"
    tool_display = f"'{tool_name}'" if tool_name else f"ID {tool_id}"
    facility_display = f"'{facility_name}'" if facility_name else f"ID {facility_id}"
    logger.info(f"Updating tool {tool_id} {tool_display} with core facility {facility_id} {facility_display}")
    
    try:
        # Use 'core_facility' field (verified to work with the API)
        payload = {
            'core_facility': facility_id
        }
        logger.debug(f"Tool {tool_id} update payload: {json.dumps(payload)}")
        
        response = requests.patch(update_url, json=payload, headers=API_HEADERS)
        logger.debug(f"Tool {tool_id} update response status: {response.status_code}")
        
        if response.status_code == 200:
            logger.info(f"Successfully updated tool {tool_id} {tool_display} with core facility {facility_id} {facility_display}")
            return True
        else:
            print(f"✗ Failed to update tool {tool_id}: HTTP {response.status_code}")
            print(f"  Error response: {response.text[:200]}")
            logger.error(f"Failed to update tool {tool_id} {tool_display}: HTTP {response.status_code} - {response.text[:200]}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error updating tool {tool_id}: {e}")
        logger.error(f"Network error updating tool {tool_id} {tool_display}: {e}", exc_info=True)
        return False

def main():
    """Main function to process CSV and assign core facilities to tools."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Assign core facilities to tools based on CSV data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default CSV file (SNSF-Data/tool-core_facility.csv) and default tools JSON file
  python3 assign_core_facilities_to_tools.py
  
  # Use custom CSV file with default tools JSON file
  python3 assign_core_facilities_to_tools.py "custom_file.csv"
  
  # Use custom CSV file and custom tools JSON file
  python3 assign_core_facilities_to_tools.py "custom_file.csv" --tools tools.json
        """
    )
    parser.add_argument('csv_file', 
                       nargs='?',  # Make it optional
                       default='SNSF-Data/tool-core_facility.csv',
                       help='Path to CSV file with tool names and core facility codes (default: SNSF-Data/tool-core_facility.csv)')
    parser.add_argument('--tools', 
                       default='tools_download.json',
                       help='Path to tools JSON file (default: tools_download.json). If file does not exist, tools will be downloaded from API.')
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Starting core facility assignment to tools script")
    logger.info(f"Log file: {log_filename}")
    logger.info(f"CSV file: {args.csv_file}")
    logger.info(f"Tools file: {args.tools}")
    logger.info("=" * 60)
    
    print("=" * 60)
    print("Assigning Core Facilities to Tools")
    print("=" * 60)
    print("\nThis script will:")
    print("1. Download core facilities from API")
    print("2. Load or download tools")
    print("3. Read CSV with tool names and core facility codes")
    print("4. Match tool names to tool IDs and core facility codes to core facility IDs")
    print("5. Assign core facilities to tools")
    print("-" * 60)
    print(f"\nConfiguration:")
    print(f"  CSV file: {args.csv_file}")
    print(f"  Tools file: {args.tools}")
    print("-" * 60)
    
    if not os.path.exists(args.csv_file):
        print(f"✗ File not found: {args.csv_file}")
        logger.error(f"File not found: {args.csv_file}")
        return
    
    csv_file = args.csv_file
    tools_file = args.tools
    
    # Test API connections
    print("\nTesting API connections...")
    logger.info("Testing API connections...")
    if not test_api_connection(NEMO_CORE_FACILITIES_API_URL, "Core Facilities"):
        print("Cannot proceed without valid core facilities API connection.")
        logger.error("Cannot proceed without valid core facilities API connection.")
        return
    
    if not test_api_connection(NEMO_TOOLS_API_URL, "Tools"):
        print("Cannot proceed without valid tools API connection.")
        logger.error("Cannot proceed without valid tools API connection.")
        return
    
    # Step 1: Download core facilities
    print("\n" + "=" * 60)
    print("Step 1: Downloading core facilities...")
    print("=" * 60)
    logger.info("=" * 60)
    logger.info("Step 1: Downloading core facilities")
    logger.info("=" * 60)
    
    core_facilities = download_all_items(NEMO_CORE_FACILITIES_API_URL, "core facilities")
    
    if not core_facilities:
        print("No core facilities downloaded. Cannot proceed.")
        logger.error("No core facilities downloaded. Cannot proceed.")
        return
    
    core_facility_lookup = create_core_facility_lookup(core_facilities)
    
    # Show sample core facilities
    print(f"\nSample core facilities:")
    logger.info("Sample core facilities:")
    sample_count = 0
    for facility in core_facilities[:10]:
        code = facility.get('code', 'N/A')
        name = facility.get('name', 'N/A')
        facility_id = facility.get('id', 'N/A')
        print(f"  - Code: {code}, Name: {name}, ID: {facility_id}")
        logger.info(f"  - Code: {code}, Name: {name}, ID: {facility_id}")
        sample_count += 1
    if len(core_facilities) > 10:
        print(f"  ... and {len(core_facilities) - 10} more")
        logger.info(f"  ... and {len(core_facilities) - 10} more")
    
    # Step 2: Load or download tools
    print("\n" + "=" * 60)
    print("Step 2: Loading tools...")
    print("=" * 60)
    logger.info("=" * 60)
    logger.info("Step 2: Loading tools")
    logger.info("=" * 60)
    
    tools = load_tools_from_file(tools_file)
    if not tools:
        tools = download_all_items(NEMO_TOOLS_API_URL, "tools")
    
    if not tools:
        print("No tools available. Cannot proceed.")
        logger.error("No tools available. Cannot proceed.")
        return
    
    tool_lookup = create_tool_lookup(tools)
    
    # Step 3: Read CSV
    print("\n" + "=" * 60)
    print("Step 3: Reading CSV...")
    print("=" * 60)
    logger.info("=" * 60)
    logger.info("Step 3: Reading CSV")
    logger.info("=" * 60)
    
    rows = read_csv_file(csv_file)
    
    if not rows:
        print("No rows found in CSV. Cannot proceed.")
        logger.error("No rows found in CSV. Cannot proceed.")
        return
    
    # Step 4: Process CSV and create assignments
    print("\n" + "=" * 60)
    print("Step 4: Processing CSV rows...")
    print("=" * 60)
    logger.info("=" * 60)
    logger.info("Step 4: Processing CSV rows")
    logger.info("=" * 60)
    
    assignments = process_csv_rows(rows, tool_lookup, core_facility_lookup)
    
    if not assignments:
        print("No valid assignments found.")
        logger.warning("No valid assignments found.")
        return
    
    # Show preview of assignments
    print(f"\nFound {len(assignments)} valid assignments:")
    logger.info(f"Found {len(assignments)} valid assignments:")
    for assignment in assignments[:10]:
        print(f"  - Tool '{assignment['tool_name']}' (ID: {assignment['tool_id']}) → Core Facility '{assignment['facility_name']}' (ID: {assignment['facility_id']})")
        logger.info(f"  - Tool '{assignment['tool_name']}' (ID: {assignment['tool_id']}) → Core Facility '{assignment['facility_name']}' (ID: {assignment['facility_id']})")
    if len(assignments) > 10:
        print(f"  ... and {len(assignments) - 10} more")
        logger.info(f"  ... and {len(assignments) - 10} more")
    
    # Step 5: Update tools with core facilities
    print("\n" + "=" * 60)
    print("Step 5: Assigning core facilities to tools...")
    print("=" * 60)
    logger.info("=" * 60)
    logger.info("Step 5: Assigning core facilities to tools")
    logger.info("=" * 60)
    
    success_count = 0
    failed_count = 0
    skipped_count = 0
    
    for idx, assignment in enumerate(assignments, 1):
        tool_id = assignment['tool_id']
        tool_name = assignment['tool_name']
        facility_id = assignment['facility_id']
        facility_name = assignment['facility_name']
        
        logger.info(f"[{idx}/{len(assignments)}] Processing assignment: tool {tool_id} '{tool_name}' → facility {facility_id} '{facility_name}'")
        
        # Check if tool already has this core facility assigned
        # Note: core_facility field may not be in the GET response, so we'll try to update anyway
        # The API will handle it if it's already set
        tool = next((t for t in tools if t.get('id') == tool_id), None)
        if tool:
            # Try to get current facility (may not be present in GET response)
            current_facility = tool.get('core_facility')
            if current_facility == facility_id:
                print(f"  ⊘ Tool {tool_id} '{tool_name}' already has core facility {facility_id} '{facility_name}' assigned. Skipping.")
                logger.info(f"Tool {tool_id} '{tool_name}' already has core facility {facility_id} '{facility_name}' assigned. Skipping.")
                skipped_count += 1
                continue
        
        print(f"  [{idx}/{len(assignments)}] Assigning core facility '{facility_name}' (ID: {facility_id}) to tool '{tool_name}' (ID: {tool_id})...")
        if update_tool_core_facility(tool_id, facility_id, tool_name, facility_name):
            success_count += 1
            print(f"    ✓ Success")
        else:
            failed_count += 1
            print(f"    ✗ Failed")
    
    # Summary
    print("\n" + "=" * 60)
    print("ASSIGNMENT SUMMARY")
    print("=" * 60)
    logger.info("=" * 60)
    logger.info("ASSIGNMENT SUMMARY")
    logger.info("=" * 60)
    
    summary_data = {
        'core_facilities_downloaded': len(core_facilities),
        'tools_loaded': len(tools),
        'csv_rows': len(rows),
        'valid_assignments': len(assignments),
        'successfully_updated': success_count,
        'skipped': skipped_count,
        'failed': failed_count
    }
    
    print(f"Core facilities downloaded: {summary_data['core_facilities_downloaded']}")
    print(f"Tools loaded: {summary_data['tools_loaded']}")
    print(f"CSV rows: {summary_data['csv_rows']}")
    print(f"Valid assignments: {summary_data['valid_assignments']}")
    print(f"Successfully updated: {summary_data['successfully_updated']}")
    print(f"Skipped (already assigned): {summary_data['skipped']}")
    print(f"Failed updates: {summary_data['failed']}")
    print("=" * 60)
    
    logger.info(f"Core facilities downloaded: {summary_data['core_facilities_downloaded']}")
    logger.info(f"Tools loaded: {summary_data['tools_loaded']}")
    logger.info(f"CSV rows: {summary_data['csv_rows']}")
    logger.info(f"Valid assignments: {summary_data['valid_assignments']}")
    logger.info(f"Successfully updated: {summary_data['successfully_updated']}")
    logger.info(f"Skipped (already assigned): {summary_data['skipped']}")
    logger.info(f"Failed updates: {summary_data['failed']}")
    logger.info("=" * 60)
    logger.info("Script completed")

if __name__ == "__main__":
    main()

