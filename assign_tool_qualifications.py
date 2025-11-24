#!/usr/bin/env python3
"""
Script to assign tool qualifications to users based on spreadsheet data.
1. Reads spreadsheet with tool names and user emails
2. Downloads tools and creates tool name -> tool ID mapping
3. Downloads users and creates email -> user ID mapping
4. Matches tool names to tool IDs and emails to user IDs
5. Adds tool IDs to user qualifications
"""

import requests
import json
import os
import logging
import pandas as pd
import argparse
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional, Tuple

# Load environment variables from .env file
load_dotenv()

# Set up logging first (before token check so we can log errors)
log_filename = f"logs/assign_tool_qualifications_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
NEMO_TOOLS_API_URL = "https://nemo.stanford.edu/api/tools/"
NEMO_USERS_API_URL = "https://nemo.stanford.edu/api/users/"

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

def load_users_from_file(filename: str = "nemo_users.json") -> Optional[List[Dict[str, Any]]]:
    """Load users from a local JSON file if it exists."""
    if os.path.exists(filename):
        try:
            with open(filename, 'r') as f:
                users = json.load(f)
            print(f"✓ Loaded {len(users)} users from {filename}")
            logger.info(f"Loaded {len(users)} users from {filename}")
            return users
        except Exception as e:
            print(f"✗ Error loading users from {filename}: {e}")
            logger.error(f"Error loading users from {filename}: {e}")
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

def create_email_lookup(users: List[Dict[str, Any]]) -> Dict[str, int]:
    """Create a lookup mapping from email addresses to user IDs."""
    logger.info(f"Creating email lookup from {len(users)} users")
    lookup = {}
    duplicate_count = 0
    
    for user in users:
        user_id = user.get('id')
        email = user.get('email', '').strip().lower()
        
        if user_id and email and '@' in email:
            # Handle duplicate emails - keep the first one
            if email not in lookup:
                lookup[email] = user_id
                logger.debug(f"Mapped email '{email}' -> user ID {user_id}")
            else:
                duplicate_count += 1
                print(f"⚠ Warning: Duplicate email '{email}' (IDs: {lookup[email]}, {user_id})")
                logger.warning(f"Duplicate email '{email}' (IDs: {lookup[email]}, {user_id})")
        else:
            logger.debug(f"Skipping user with missing ID or email: {user}")
    
    print(f"✓ Created email lookup with {len(lookup)} entries")
    logger.info(f"Created email lookup with {len(lookup)} entries ({duplicate_count} duplicates found)")
    return lookup

def read_spreadsheet(filename: str) -> List[Dict[str, Any]]:
    """Read spreadsheet data (CSV or Excel) and return list of rows."""
    print(f"Reading spreadsheet from {filename}...")
    logger.info(f"Reading spreadsheet from {filename}")
    
    try:
        # Try Excel first, then CSV
        if filename.endswith('.xlsx') or filename.endswith('.xls'):
            df = pd.read_excel(filename)
        else:
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
        print(f"✗ Error reading spreadsheet: {e}")
        logger.error(f"Error reading spreadsheet {filename}: {e}", exc_info=True)
        return []

def find_column(df_columns: List[str], possible_names: List[str]) -> Optional[str]:
    """Find a column by checking multiple possible names (case-insensitive)."""
    df_columns_lower = [col.lower().strip() for col in df_columns]
    for name in possible_names:
        name_lower = name.lower().strip()
        if name_lower in df_columns_lower:
            idx = df_columns_lower.index(name_lower)
            return df_columns[idx]
    return None

def process_spreadsheet_rows(rows: List[Dict[str, Any]], tool_lookup: Dict[str, int], email_lookup: Dict[str, int]) -> List[Dict[str, Any]]:
    """
    Process spreadsheet rows and match tool names to tool IDs and emails to user IDs.
    Returns list of assignments to make.
    """
    logger.info(f"Processing {len(rows)} spreadsheet rows")
    assignments = []
    missing_tool_count = 0
    missing_user_count = 0
    
    # Find column names (case-insensitive)
    if not rows:
        return assignments
    
    df_columns = list(rows[0].keys())
    equipment_col = find_column(df_columns, ['equipment', 'tool', 'tool name', 'tool_name'])
    member_col = find_column(df_columns, ['member', 'email', 'user_email', 'member_email'])
    
    if not equipment_col:
        print("✗ Error: Could not find 'equipment' column in spreadsheet")
        logger.error("Could not find 'equipment' column in spreadsheet")
        return assignments
    
    if not member_col:
        print("✗ Error: Could not find 'member' or 'email' column in spreadsheet")
        logger.error("Could not find 'member' or 'email' column in spreadsheet")
        return assignments
    
    print(f"  Using column '{equipment_col}' for tool names")
    print(f"  Using column '{member_col}' for user emails")
    logger.info(f"Using column '{equipment_col}' for tool names")
    logger.info(f"Using column '{member_col}' for user emails")
    
    for idx, row in enumerate(rows, 1):
        # Get tool name and email
        tool_name = str(row.get(equipment_col, '')).strip()
        email = str(row.get(member_col, '')).strip().lower()
        
        # Skip rows with missing data
        if not tool_name or tool_name.lower() in ['nan', 'none', 'null', '']:
            logger.debug(f"Row {idx}: Skipping - missing tool name")
            continue
        
        if not email or email.lower() in ['nan', 'none', 'null', ''] or '@' not in email:
            logger.debug(f"Row {idx}: Skipping - missing or invalid email")
            continue
        
        # Match tool name to tool ID
        tool_id = tool_lookup.get(tool_name)
        if not tool_id:
            missing_tool_count += 1
            print(f"⚠ Row {idx}: Tool '{tool_name}' not found in tool lookup")
            logger.warning(f"Row {idx}: Tool '{tool_name}' not found in tool lookup")
            continue
        
        # Match email to user ID
        user_id = email_lookup.get(email)
        if not user_id:
            missing_user_count += 1
            print(f"⚠ Row {idx}: Email '{email}' not found in user lookup")
            logger.warning(f"Row {idx}: Email '{email}' not found in user lookup")
            continue
        
        # Add assignment
        assignments.append({
            'tool_name': tool_name,
            'tool_id': tool_id,
            'email': email,
            'user_id': user_id,
            'row': idx
        })
        logger.debug(f"Row {idx}: Matched '{tool_name}' (ID: {tool_id}) -> '{email}' (User ID: {user_id})")
    
    print(f"✓ Processed {len(rows)} rows: {len(assignments)} valid assignments")
    if missing_tool_count > 0:
        print(f"  ⚠ {missing_tool_count} rows with missing tools")
    if missing_user_count > 0:
        print(f"  ⚠ {missing_user_count} rows with missing users")
    
    logger.info(f"Processed {len(rows)} rows: {len(assignments)} valid assignments ({missing_tool_count} missing tools, {missing_user_count} missing users)")
    return assignments

def get_user_qualifications(user_id: int) -> Optional[List[int]]:
    """Get current qualifications for a user."""
    user_url = f"{NEMO_USERS_API_URL}{user_id}/"
    try:
        response = requests.get(user_url, headers=API_HEADERS)
        if response.status_code == 200:
            user_data = response.json()
            qualifications = user_data.get('qualifications', [])
            return qualifications if isinstance(qualifications, list) else []
        else:
            logger.error(f"Failed to get user {user_id}: HTTP {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error getting user {user_id}: {e}")
        return None

def update_user_qualifications(user_id: int, tool_id: int, email: str, tool_name: str) -> Tuple[bool, bool]:
    """
    Add a tool qualification to a user's qualifications.
    Returns (success, was_skipped) tuple.
    """
    logger.info(f"Updating user {user_id} ({email}) with tool qualification {tool_id} ({tool_name})")
    
    # Get current qualifications
    current_qualifications = get_user_qualifications(user_id)
    if current_qualifications is None:
        print(f"✗ Failed to get current qualifications for user {user_id}")
        logger.error(f"Failed to get current qualifications for user {user_id}")
        return (False, False)
    
    # Check if qualification already exists
    if tool_id in current_qualifications:
        print(f"  ⊘ User {user_id} ({email}) already has qualification for tool {tool_id} ({tool_name}). Skipping.")
        logger.info(f"User {user_id} ({email}) already has qualification for tool {tool_id} ({tool_name}). Skipping.")
        return (True, True)  # Success but skipped (already qualified)
    
    # Add the new qualification
    updated_qualifications = current_qualifications + [tool_id]
    
    # Update user via PATCH
    update_url = f"{NEMO_USERS_API_URL}{user_id}/"
    try:
        payload = {
            'qualifications': updated_qualifications
        }
        logger.debug(f"User {user_id} update payload: {json.dumps(payload)}")
        
        response = requests.patch(update_url, json=payload, headers=API_HEADERS)
        logger.debug(f"User {user_id} update response status: {response.status_code}")
        
        if response.status_code == 200:
            logger.info(f"Successfully updated user {user_id} ({email}) with tool qualification {tool_id} ({tool_name})")
            return (True, False)  # Success, not skipped
        else:
            print(f"✗ Failed to update user {user_id}: HTTP {response.status_code}")
            print(f"  Error response: {response.text[:200]}")
            logger.error(f"Failed to update user {user_id} ({email}): HTTP {response.status_code} - {response.text[:200]}")
            return (False, False)
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error updating user {user_id}: {e}")
        logger.error(f"Network error updating user {user_id} ({email}): {e}", exc_info=True)
        return (False, False)

def main():
    """Main function to process spreadsheet and assign tool qualifications."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Assign tool qualifications to users based on spreadsheet data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default spreadsheet (SNSF-Data/SNL Qualified Users.xlsx) and default JSON files
  python3 assign_tool_qualifications.py
  
  # Use default spreadsheet with custom tools and users JSON files
  python3 assign_tool_qualifications.py --tools tools.json --users users.json
  
  # Use custom spreadsheet with default JSON files
  python3 assign_tool_qualifications.py "SNSF-Data/SNC Qualified Users.xlsx"
  
  # Use custom spreadsheet and custom JSON files
  python3 assign_tool_qualifications.py "custom_spreadsheet.xlsx" --tools tools.json --users users.json
        """
    )
    parser.add_argument('spreadsheet', 
                       nargs='?',  # Make it optional
                       default='SNSF-Data/SNL Qualified Users.xlsx',
                       help='Path to spreadsheet file (CSV or Excel) with tool names and user emails (default: SNSF-Data/SNL Qualified Users.xlsx)')
    parser.add_argument('--tools', 
                       default='tools_download.json',
                       help='Path to tools JSON file (default: tools_download.json). If file does not exist, tools will be downloaded from API.')
    parser.add_argument('--users', 
                       default='nemo_users.json',
                       help='Path to users JSON file (default: nemo_users.json). If file does not exist, users will be downloaded from API.')
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Starting tool qualification assignment script")
    logger.info(f"Log file: {log_filename}")
    logger.info(f"Spreadsheet: {args.spreadsheet}")
    logger.info(f"Tools file: {args.tools}")
    logger.info(f"Users file: {args.users}")
    logger.info("=" * 60)
    
    print("=" * 60)
    print("Assigning Tool Qualifications to Users")
    print("=" * 60)
    print("\nThis script will:")
    print("1. Read spreadsheet with tool names and user emails")
    print("2. Download or load tools and create tool name -> tool ID mapping")
    print("3. Download or load users and create email -> user ID mapping")
    print("4. Match tool names to tool IDs and emails to user IDs")
    print("5. Add tool IDs to user qualifications")
    print("-" * 60)
    print(f"\nConfiguration:")
    print(f"  Spreadsheet: {args.spreadsheet}")
    print(f"  Tools file: {args.tools}")
    print(f"  Users file: {args.users}")
    print("-" * 60)
    
    if not os.path.exists(args.spreadsheet):
        print(f"✗ File not found: {args.spreadsheet}")
        logger.error(f"File not found: {args.spreadsheet}")
        return
    
    spreadsheet_file = args.spreadsheet
    tools_file = args.tools
    users_file = args.users
    
    # Test API connections
    print("\nTesting API connections...")
    logger.info("Testing API connections...")
    if not test_api_connection(NEMO_TOOLS_API_URL, "Tools"):
        print("Cannot proceed without valid tools API connection.")
        logger.error("Cannot proceed without valid tools API connection.")
        return
    
    if not test_api_connection(NEMO_USERS_API_URL, "Users"):
        print("Cannot proceed without valid users API connection.")
        logger.error("Cannot proceed without valid users API connection.")
        return
    
    # Step 1: Load or download tools
    print("\n" + "=" * 60)
    print("Step 1: Loading tools...")
    print("=" * 60)
    logger.info("=" * 60)
    logger.info("Step 1: Loading tools")
    logger.info("=" * 60)
    
    tools = load_tools_from_file(tools_file)
    if not tools:
        tools = download_all_items(NEMO_TOOLS_API_URL, "tools")
    
    if not tools:
        print("No tools available. Cannot proceed.")
        logger.error("No tools available. Cannot proceed.")
        return
    
    tool_lookup = create_tool_lookup(tools)
    
    # Step 2: Load or download users
    print("\n" + "=" * 60)
    print("Step 2: Loading users...")
    print("=" * 60)
    logger.info("=" * 60)
    logger.info("Step 2: Loading users")
    logger.info("=" * 60)
    
    users = load_users_from_file(users_file)
    if not users:
        users = download_all_items(NEMO_USERS_API_URL, "users")
    
    if not users:
        print("No users available. Cannot proceed.")
        logger.error("No users available. Cannot proceed.")
        return
    
    email_lookup = create_email_lookup(users)
    
    # Step 3: Read spreadsheet
    print("\n" + "=" * 60)
    print("Step 3: Reading spreadsheet...")
    print("=" * 60)
    logger.info("=" * 60)
    logger.info("Step 3: Reading spreadsheet")
    logger.info("=" * 60)
    
    rows = read_spreadsheet(spreadsheet_file)
    
    if not rows:
        print("No rows found in spreadsheet. Cannot proceed.")
        logger.error("No rows found in spreadsheet. Cannot proceed.")
        return
    
    # Step 4: Process spreadsheet and create assignments
    print("\n" + "=" * 60)
    print("Step 4: Processing spreadsheet rows...")
    print("=" * 60)
    logger.info("=" * 60)
    logger.info("Step 4: Processing spreadsheet rows")
    logger.info("=" * 60)
    
    assignments = process_spreadsheet_rows(rows, tool_lookup, email_lookup)
    
    if not assignments:
        print("No valid assignments found.")
        logger.warning("No valid assignments found.")
        return
    
    # Show preview of assignments
    print(f"\nFound {len(assignments)} valid assignments:")
    logger.info(f"Found {len(assignments)} valid assignments:")
    for assignment in assignments[:10]:
        print(f"  - Tool '{assignment['tool_name']}' (ID: {assignment['tool_id']}) → User '{assignment['email']}' (ID: {assignment['user_id']})")
        logger.info(f"  - Tool '{assignment['tool_name']}' (ID: {assignment['tool_id']}) → User '{assignment['email']}' (ID: {assignment['user_id']})")
    if len(assignments) > 10:
        print(f"  ... and {len(assignments) - 10} more")
        logger.info(f"  ... and {len(assignments) - 10} more")
    
    # Step 5: Update user qualifications
    print("\n" + "=" * 60)
    print("Step 5: Assigning tool qualifications to users...")
    print("=" * 60)
    logger.info("=" * 60)
    logger.info("Step 5: Assigning tool qualifications to users")
    logger.info("=" * 60)
    
    success_count = 0
    failed_count = 0
    skipped_count = 0
    
    for idx, assignment in enumerate(assignments, 1):
        tool_id = assignment['tool_id']
        tool_name = assignment['tool_name']
        user_id = assignment['user_id']
        email = assignment['email']
        
        logger.info(f"[{idx}/{len(assignments)}] Processing assignment: tool {tool_id} '{tool_name}' → user {user_id} '{email}'")
        
        print(f"  [{idx}/{len(assignments)}] Assigning tool '{tool_name}' (ID: {tool_id}) to user '{email}' (ID: {user_id})...")
        success, was_skipped = update_user_qualifications(user_id, tool_id, email, tool_name)
        if success:
            if was_skipped:
                skipped_count += 1
            else:
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
        'tools_loaded': len(tools),
        'users_loaded': len(users),
        'spreadsheet_rows': len(rows),
        'valid_assignments': len(assignments),
        'successfully_updated': success_count,
        'skipped': skipped_count,
        'failed': failed_count
    }
    
    print(f"Tools loaded: {summary_data['tools_loaded']}")
    print(f"Users loaded: {summary_data['users_loaded']}")
    print(f"Spreadsheet rows: {summary_data['spreadsheet_rows']}")
    print(f"Valid assignments: {summary_data['valid_assignments']}")
    print(f"Successfully updated: {summary_data['successfully_updated']}")
    print(f"Skipped (already qualified): {summary_data['skipped']}")
    print(f"Failed updates: {summary_data['failed']}")
    print("=" * 60)
    
    logger.info(f"Tools loaded: {summary_data['tools_loaded']}")
    logger.info(f"Users loaded: {summary_data['users_loaded']}")
    logger.info(f"Spreadsheet rows: {summary_data['spreadsheet_rows']}")
    logger.info(f"Valid assignments: {summary_data['valid_assignments']}")
    logger.info(f"Successfully updated: {summary_data['successfully_updated']}")
    logger.info(f"Skipped (already qualified): {summary_data['skipped']}")
    logger.info(f"Failed updates: {summary_data['failed']}")
    logger.info("=" * 60)
    logger.info("Script completed")

if __name__ == "__main__":
    main()

