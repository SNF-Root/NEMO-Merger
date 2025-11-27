#!/usr/bin/env python3
"""
Script to create qualification records for users based on spreadsheet data.
1. Reads spreadsheet with tool names, user emails, and qualification dates
2. Downloads tools and creates tool name -> tool ID mapping
3. Downloads users and creates email -> user ID mapping
4. Matches tool names to tool IDs and emails to user IDs
5. Creates qualification records at /api/qualifications/ endpoint

IMPORTANT NOTE: The API marks 'qualified_on' as read-only, so historical dates 
from the spreadsheet cannot be set. The API will automatically set qualified_on 
to today's date when creating each qualification record.

The script creates qualification records with the following structure:
- user: user ID
- tool: tool ID
- qualification_level: null
- qualified_on: automatically set by API to today's date (read-only field)
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
from dateutil import parser as date_parser

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
NEMO_QUALIFICATIONS_API_URL = "https://nemo.stanford.edu/api/qualifications/"

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
            # Read Excel without auto-parsing dates to preserve original values
            # We'll handle date parsing manually to ensure correct format
            df = pd.read_excel(filename, parse_dates=False)
        else:
            df = pd.read_csv(filename)
        
        # Convert to list of dictionaries
        # Use 'records' format to preserve data types
        rows = df.to_dict('records')
        
        print(f"✓ Read {len(rows)} rows from {filename}")
        logger.info(f"Read {len(rows)} rows from {filename}")
        
        # Show column names for debugging
        print(f"  Columns found: {', '.join(df.columns.tolist())}")
        logger.info(f"Columns found: {', '.join(df.columns.tolist())}")
        
        # Log sample of date column if it exists
        date_cols = [col for col in df.columns if 'date' in str(col).lower()]
        if date_cols:
            sample_col = date_cols[0]
            print(f"  Sample date values from '{sample_col}': {df[sample_col].head(3).tolist()}")
            logger.info(f"Sample date values from '{sample_col}': {df[sample_col].head(3).tolist()}")
        
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

def parse_qualification_date(date_value: Any) -> Optional[str]:
    """
    Parse a qualification date from various formats and return YYYY-MM-DD format.
    Returns None if date cannot be parsed.
    Handles pandas Timestamp, datetime objects, Excel serial numbers, and string dates.
    """
    if date_value is None:
        return None
    
    # Check for pandas NaN/NaT
    if pd.isna(date_value):
        return None
    
    date_str = str(date_value).strip()
    if not date_str or date_str.lower() in ['nan', 'none', 'null', '', 'nat']:
        return None
    
    try:
        # Handle pandas Timestamp objects (from Excel dates)
        if isinstance(date_value, pd.Timestamp):
            # Check if it's NaT (Not a Time)
            if pd.isna(date_value):
                return None
            formatted = date_value.strftime('%Y-%m-%d')
            logger.debug(f"Parsed pandas Timestamp '{date_value}' -> '{formatted}'")
            return formatted
        
        # Handle datetime objects
        if isinstance(date_value, datetime):
            formatted = date_value.strftime('%Y-%m-%d')
            logger.debug(f"Parsed datetime '{date_value}' -> '{formatted}'")
            return formatted
        
        # Handle Excel serial numbers (if pandas didn't convert them)
        try:
            if isinstance(date_value, (int, float)) and date_value > 1:
                # Excel serial number (days since 1900-01-01)
                excel_epoch = datetime(1899, 12, 30)
                parsed_date = excel_epoch + pd.Timedelta(days=date_value)
                formatted = parsed_date.strftime('%Y-%m-%d')
                logger.debug(f"Parsed Excel serial number '{date_value}' -> '{formatted}'")
                return formatted
        except (ValueError, OverflowError):
            pass
        
        # Try dateutil parser for string dates
        parsed_date = date_parser.parse(date_str)
        formatted = parsed_date.strftime('%Y-%m-%d')
        logger.debug(f"Parsed string date '{date_str}' -> '{formatted}'")
        return formatted
    except (ValueError, TypeError, AttributeError) as e:
        logger.warning(f"Could not parse date '{date_value}' (type: {type(date_value)}): {e}")
        return None

def process_spreadsheet_rows(rows: List[Dict[str, Any]], tool_lookup: Dict[str, int], email_lookup: Dict[str, int]) -> List[Dict[str, Any]]:
    """
    Process spreadsheet rows and match tool names to tool IDs and emails to user IDs.
    Also reads qualification dates from the spreadsheet.
    Returns list of assignments to make.
    """
    logger.info(f"Processing {len(rows)} spreadsheet rows")
    assignments = []
    missing_tool_count = 0
    missing_user_count = 0
    missing_date_count = 0
    
    # Find column names (case-insensitive)
    if not rows:
        return assignments
    
    df_columns = list(rows[0].keys())
    equipment_col = find_column(df_columns, ['equipment', 'tool', 'tool name', 'tool_name'])
    member_col = find_column(df_columns, ['member', 'email', 'user_email', 'member_email'])
    date_col = find_column(df_columns, ['date', 'qualification date', 'qualified_on', 'qualified on', 'qualification_date'])
    
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
    if date_col:
        print(f"  Using column '{date_col}' for qualification dates")
    else:
        print(f"  ⚠ Warning: No qualification date column found. Will use null for qualified_on.")
    logger.info(f"Using column '{equipment_col}' for tool names")
    logger.info(f"Using column '{member_col}' for user emails")
    if date_col:
        logger.info(f"Using column '{date_col}' for qualification dates")
    
    for idx, row in enumerate(rows, 1):
        # Get tool name and email
        tool_name = str(row.get(equipment_col, '')).strip()
        email = str(row.get(member_col, '')).strip().lower()
        
        # Get qualification date (optional)
        qualified_on = None
        if date_col:
            date_value = row.get(date_col)
            # Log raw value for debugging
            if idx <= 5:  # Only log first few for debugging
                logger.info(f"Row {idx}: Raw date value: '{date_value}' (type: {type(date_value).__name__})")
            qualified_on = parse_qualification_date(date_value)
            if not qualified_on:
                missing_date_count += 1
                if idx <= 5:  # Only log first few warnings
                    logger.warning(f"Row {idx}: Could not parse qualification date '{date_value}' (type: {type(date_value).__name__}), will use null")
            else:
                if idx <= 5:  # Only log first few for debugging
                    logger.info(f"Row {idx}: Parsed qualification date: '{qualified_on}' from '{date_value}'")
        
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
            'qualified_on': qualified_on,
            'row': idx
        })
        logger.debug(f"Row {idx}: Matched '{tool_name}' (ID: {tool_id}) -> '{email}' (User ID: {user_id}), date: {qualified_on}")
    
    print(f"✓ Processed {len(rows)} rows: {len(assignments)} valid assignments")
    if missing_tool_count > 0:
        print(f"  ⚠ {missing_tool_count} rows with missing tools")
    if missing_user_count > 0:
        print(f"  ⚠ {missing_user_count} rows with missing users")
    if missing_date_count > 0:
        print(f"  ⚠ {missing_date_count} rows with missing/unparseable dates (will use null)")
    
    logger.info(f"Processed {len(rows)} rows: {len(assignments)} valid assignments ({missing_tool_count} missing tools, {missing_user_count} missing users, {missing_date_count} missing dates)")
    return assignments

def verify_user_account_exists(user_id: int, email: str) -> bool:
    """Verify that a user account exists and is accessible."""
    user_url = f"{NEMO_USERS_API_URL}{user_id}/"
    try:
        response = requests.get(user_url, headers=API_HEADERS)
        if response.status_code == 200:
            user_data = response.json()
            # Check if user is active (some APIs have an 'active' or 'is_active' field)
            # If the user exists and we can get their data, consider them active
            user_email = user_data.get('email', '').strip().lower()
            if user_email and user_email == email.strip().lower():
                logger.debug(f"Verified user {user_id} ({email}) account exists and is accessible")
                return True
            else:
                logger.warning(f"User {user_id} email mismatch: expected '{email}', got '{user_email}'")
                return False
        elif response.status_code == 404:
            logger.warning(f"User {user_id} ({email}) account not found (404)")
            return False
        else:
            logger.warning(f"Failed to verify user {user_id} ({email}): HTTP {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error verifying user {user_id} ({email}): {e}")
        return False

def get_user_qualifications(user_id: int) -> Optional[List[int]]:
    """Get current qualifications for a user."""
    user_url = f"{NEMO_USERS_API_URL}{user_id}/"
    try:
        response = requests.get(user_url, headers=API_HEADERS)
        if response.status_code == 200:
            user_data = response.json()
            qualifications = user_data.get('qualifications', [])
            return qualifications if isinstance(qualifications, list) else []
        elif response.status_code == 404:
            logger.error(f"User {user_id} account not found (404)")
            return None
        else:
            logger.error(f"Failed to get user {user_id}: HTTP {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error getting user {user_id}: {e}")
        return None

def check_qualification_exists(user_id: int, tool_id: int) -> bool:
    """Check if a qualification record already exists for this user and tool."""
    try:
        # Query qualifications endpoint for this user and tool
        params = {'user': user_id, 'tool': tool_id}
        response = requests.get(NEMO_QUALIFICATIONS_API_URL, headers=API_HEADERS, params=params)
        
        if response.status_code == 200:
            data = response.json()
            # Handle both list and paginated responses
            if isinstance(data, list):
                qualifications = data
            elif isinstance(data, dict) and 'results' in data:
                qualifications = data['results']
            else:
                qualifications = []
            
            return len(qualifications) > 0
        else:
            logger.debug(f"Could not check existing qualifications: HTTP {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        logger.debug(f"Error checking existing qualifications: {e}")
        return False

def create_qualification_record(user_id: int, tool_id: int, email: str, tool_name: str, qualified_on: Optional[str] = None) -> Tuple[bool, bool]:
    """
    Create a qualification record at the /api/qualifications/ endpoint.
    
    NOTE: The API marks 'qualified_on' as read-only, so historical dates cannot be set.
    The API will automatically set qualified_on to today's date when creating a qualification.
    
    Returns (success, was_skipped) tuple.
    """
    logger.info(f"Creating qualification record: user {user_id} ({email}) -> tool {tool_id} ({tool_name})")
    if qualified_on:
        logger.warning(f"  ⚠ Note: Historical date '{qualified_on}' cannot be set - API will use today's date (qualified_on is read-only)")
    
    # First verify the user account exists and is accessible
    if not verify_user_account_exists(user_id, email):
        print(f"  ⊘ User {user_id} ({email}) account not found or not accessible. Skipping.")
        logger.warning(f"User {user_id} ({email}) account not found or not accessible. Skipping qualification creation.")
        return (False, True)  # Failed but skipped (account doesn't exist)
    
    # Check if qualification already exists
    if check_qualification_exists(user_id, tool_id):
        print(f"  ⊘ User {user_id} ({email}) already has qualification record for tool {tool_id} ({tool_name}). Skipping.")
        logger.info(f"User {user_id} ({email}) already has qualification record for tool {tool_id} ({tool_name}). Skipping.")
        return (True, True)  # Success but skipped (already qualified)
    
    # Create qualification record via POST
    # NOTE: qualified_on is read-only in the API, so we don't include it in the payload
    try:
        payload = {
            'user': user_id,
            'tool': tool_id,
            'qualification_level': None
            # qualified_on is intentionally omitted - API sets it automatically to today's date
        }
        logger.debug(f"Qualification creation payload: {json.dumps(payload, default=str)}")
        
        response = requests.post(NEMO_QUALIFICATIONS_API_URL, json=payload, headers=API_HEADERS)
        logger.debug(f"Qualification creation response status: {response.status_code}")
        logger.debug(f"Qualification creation response: {response.text[:500]}")
        
        if response.status_code in [200, 201]:  # Created (some APIs return 200 instead of 201)
            try:
                response_data = response.json()
                created_date = response_data.get('qualified_on', 'not in response')
                logger.info(f"Successfully created qualification record: user {user_id} ({email}) -> tool {tool_id} ({tool_name})")
                if qualified_on and created_date != 'not in response':
                    logger.warning(f"  ⚠ API set qualified_on to '{created_date}' (historical date '{qualified_on}' cannot be set - field is read-only)")
            except:
                pass
            return (True, False)  # Success, not skipped
        elif response.status_code == 400:
            # Check if it's a duplicate error
            error_text = response.text.lower()
            if 'already exists' in error_text or 'duplicate' in error_text:
                print(f"  ⊘ Qualification already exists for user {user_id} ({email}) -> tool {tool_id} ({tool_name}). Skipping.")
                logger.info(f"Qualification already exists (400): user {user_id} ({email}) -> tool {tool_id} ({tool_name})")
                return (True, True)  # Success but skipped (already exists)
            else:
                print(f"✗ Failed to create qualification: HTTP {response.status_code}")
                print(f"  Error response: {response.text[:200]}")
                logger.error(f"Failed to create qualification: HTTP {response.status_code} - {response.text[:200]}")
                return (False, False)
        else:
            print(f"✗ Failed to create qualification: HTTP {response.status_code}")
            print(f"  Error response: {response.text[:200]}")
            logger.error(f"Failed to create qualification: HTTP {response.status_code} - {response.text[:200]}")
            return (False, False)
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error creating qualification: {e}")
        logger.error(f"Network error creating qualification: {e}", exc_info=True)
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
                       default='SNSF-Data/SNC Qualified Users.xlsx',
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
    print("\n⚠  IMPORTANT: API LIMITATION")
    print("   The API marks 'qualified_on' as read-only.")
    print("   Historical dates from your spreadsheet CANNOT be set.")
    print("   All qualifications will be dated as today's date.")
    print("=" * 60)
    print("\nThis script will:")
    print("1. Read spreadsheet with tool names, user emails, and qualification dates")
    print("2. Download or load tools and create tool name -> tool ID mapping")
    print("3. Download or load users and create email -> user ID mapping")
    print("4. Match tool names to tool IDs and emails to user IDs")
    print("5. Create qualification records at /api/qualifications/ endpoint")
    print("   (qualified_on will be automatically set to today's date by the API)")
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
    
    if not test_api_connection(NEMO_QUALIFICATIONS_API_URL, "Qualifications"):
        print("Cannot proceed without valid qualifications API connection.")
        logger.error("Cannot proceed without valid qualifications API connection.")
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
    
    # Step 5: Create qualification records
    print("\n" + "=" * 60)
    print("Step 5: Creating qualification records...")
    print("=" * 60)
    logger.info("=" * 60)
    logger.info("Step 5: Creating qualification records")
    logger.info("=" * 60)
    
    success_count = 0
    failed_count = 0
    skipped_count = 0  # Already qualified
    account_not_found_count = 0  # Account doesn't exist
    
    for idx, assignment in enumerate(assignments, 1):
        tool_id = assignment['tool_id']
        tool_name = assignment['tool_name']
        user_id = assignment['user_id']
        email = assignment['email']
        qualified_on = assignment.get('qualified_on')
        
        date_display = f" (date: {qualified_on})" if qualified_on else " (date: null)"
        logger.info(f"[{idx}/{len(assignments)}] Processing qualification: tool {tool_id} '{tool_name}' → user {user_id} '{email}'{date_display}")
        
        print(f"  [{idx}/{len(assignments)}] Creating qualification: tool '{tool_name}' (ID: {tool_id}) → user '{email}' (ID: {user_id}){date_display}...")
        success, was_skipped = create_qualification_record(user_id, tool_id, email, tool_name, qualified_on)
        if success:
            if was_skipped:
                skipped_count += 1  # Already qualified
            else:
                success_count += 1
            print(f"    ✓ Success")
        else:
            if was_skipped:
                account_not_found_count += 1  # Account doesn't exist
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
        'account_not_found': account_not_found_count,
        'failed': failed_count
    }
    
    print(f"Tools loaded: {summary_data['tools_loaded']}")
    print(f"Users loaded: {summary_data['users_loaded']}")
    print(f"Spreadsheet rows: {summary_data['spreadsheet_rows']}")
    print(f"Valid assignments: {summary_data['valid_assignments']}")
    print(f"Successfully updated: {summary_data['successfully_updated']}")
    print(f"Skipped (already qualified): {summary_data['skipped']}")
    print(f"Skipped (account not found): {summary_data['account_not_found']}")
    print(f"Failed updates: {summary_data['failed']}")
    print("=" * 60)
    
    logger.info(f"Tools loaded: {summary_data['tools_loaded']}")
    logger.info(f"Users loaded: {summary_data['users_loaded']}")
    logger.info(f"Spreadsheet rows: {summary_data['spreadsheet_rows']}")
    logger.info(f"Valid assignments: {summary_data['valid_assignments']}")
    logger.info(f"Successfully updated: {summary_data['successfully_updated']}")
    logger.info(f"Skipped (already qualified): {summary_data['skipped']}")
    logger.info(f"Skipped (account not found): {summary_data['account_not_found']}")
    logger.info(f"Failed updates: {summary_data['failed']}")
    logger.info("=" * 60)
    logger.info("Script completed")

if __name__ == "__main__":
    main()

