#!/usr/bin/env python3
"""
Script to push interlocks from CSV/Excel file to NEMO API endpoint.
Reads interlock data (card reference, name, channel, unit_id, state) and creates Interlock objects.
"""

import pandas as pd
import requests
import json
import os
from typing import List, Dict, Any
import time
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging first (before token check so we can log errors)
log_filename = f"interlock_creation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()  # Also log to console
    ]
)
logger = logging.getLogger(__name__)

# NEMO API endpoint for interlocks
NEMO_API_URL = "https://nemo.stanford.edu/api/interlocks/"

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

def test_api_connection():
    """Test the API connection and authentication."""
    try:
        response = requests.get(NEMO_API_URL, headers=API_HEADERS)
        if response.status_code == 200:
            print("✓ API connection successful")
            logger.info("API connection test successful")
            return True
        elif response.status_code == 401:
            print("✗ Authentication failed: Check your NEMO_TOKEN")
            logger.error("API authentication failed: Check your NEMO_TOKEN")
            return False
        elif response.status_code == 403:
            print("✗ Permission denied: Check your API permissions")
            logger.error("API permission denied: Check your API permissions")
            return False
        else:
            print(f"✗ API connection failed: HTTP {response.status_code}")
            logger.error(f"API connection failed: HTTP {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error connecting to API: {e}")
        logger.error(f"Network error connecting to API: {e}")
        return False

def load_interlock_card_lookup(filename: str = "interlock_card_lookup.json") -> Dict[str, int]:
    """Load the interlock card lookup from the downloaded cards."""
    logger.info(f"Attempting to load interlock card lookup from {filename}")
    try:
        with open(filename, 'r') as f:
            lookup = json.load(f)
        print(f"✓ Loaded interlock card lookup with {len(lookup)} entries")
        logger.info(f"Successfully loaded interlock card lookup from {filename} with {len(lookup)} entries")
        return lookup
    except FileNotFoundError:
        print(f"✗ Interlock card lookup file {filename} not found!")
        print("Please run download_interlock_cards.py first to download interlock cards from NEMO.")
        logger.error(f"Interlock card lookup file {filename} not found!")
        return {}
    except json.JSONDecodeError as e:
        print(f"✗ Error parsing JSON in interlock card lookup file: {e}")
        logger.error(f"Error parsing JSON in interlock card lookup file {filename}: {e}")
        return {}
    except Exception as e:
        print(f"✗ Error loading interlock card lookup: {e}")
        logger.error(f"Error loading interlock card lookup from {filename}: {e}", exc_info=True)
        return {}

def get_protocol_from_hardware(hardware: Any) -> str:
    """Map hardware name to protocol name."""
    if not hardware:
        return None
    
    hardware_str = str(hardware).strip()
    
    # Hardware to protocol mapping
    hardware_to_protocol = {
        "NCD v1": "ProXr",
        "NCD v2": "ProXr",
        "WebSwitch Plus": "WebRelayHttp"
    }
    
    # Check for exact match first
    if hardware_str in hardware_to_protocol:
        return hardware_to_protocol[hardware_str]
    
    # Check for case-insensitive partial matches
    hardware_lower = hardware_str.lower()
    for key, protocol in hardware_to_protocol.items():
        if key.lower() in hardware_lower or hardware_lower in key.lower():
            return protocol
    
    return None

def get_port_from_protocol(protocol: str) -> int:
    """Map protocol name to port number."""
    protocol_to_port = {
        "ProXr": 2101,
        "WebRelayHttp": 80
    }
    
    return protocol_to_port.get(protocol) if protocol else None

def read_interlocks_from_csv(file_path: str, card_lookup: Dict[str, int]) -> List[Dict[str, Any]]:
    """Read interlock data from BadgerBoxes.csv file."""
    logger.info(f"Reading interlock data from CSV file: {file_path}")
    try:
        df = pd.read_csv(file_path)
        logger.info(f"CSV file loaded successfully. Total rows: {len(df)}")
        
        # Look for required columns in BadgerBoxes.csv
        ip_col = None
        badger_name_col = None
        hardware_col = None
        relay_col = None
        interlock_name_col = None
        
        for col in df.columns:
            col_lower = col.lower().strip()
            if col_lower == 'ip':
                ip_col = col
            elif 'badger name' in col_lower:
                badger_name_col = col
            elif col_lower == 'hardware':
                hardware_col = col
            elif 'relay' in col_lower and 'webswitch' in col_lower:
                relay_col = col
            elif col_lower == 'interlock':
                interlock_name_col = col
        
        if not ip_col:
            print(f"Warning: No IP column found in {file_path}")
            print(f"Available columns: {df.columns.tolist()}")
            return []
        
        if not hardware_col:
            print(f"Warning: No Hardware column found in {file_path}")
            print(f"Available columns: {df.columns.tolist()}")
            return []
        
        print(f"Using columns: IP={ip_col}, Badger Name={badger_name_col}, Hardware={hardware_col}, Relay #={relay_col}, Interlock={interlock_name_col}")
        logger.info(f"Column mapping: IP={ip_col}, Badger Name={badger_name_col}, Hardware={hardware_col}, Relay #={relay_col}, Interlock={interlock_name_col}")
        
        interlocks = []
        skipped_count = 0
        for idx, (_, row) in enumerate(df.iterrows(), start=2):  # Start at 2 because row 1 is header
            row_num = idx
            ip = row.get(ip_col, '')
            badger_name = row.get(badger_name_col, '') if badger_name_col else ''
            hardware = row.get(hardware_col, '')
            relay = row.get(relay_col, '') if relay_col else ''
            interlock_name = row.get(interlock_name_col, '') if interlock_name_col else None
            
            # Skip if IP is missing
            if pd.isna(ip) or not str(ip).strip():
                skipped_count += 1
                logger.debug(f"Row {row_num}: Skipping - missing IP")
                continue
            
            # Skip if hardware is empty (needed to determine protocol and port)
            if pd.isna(hardware) or not str(hardware).strip():
                skipped_count += 1
                logger.warning(f"Row {row_num}: Skipping - missing hardware (IP: {ip})")
                continue
            
            # Convert to string and clean
            ip_str = str(ip).strip()
            hardware_str = str(hardware).strip()
            badger_name_str = str(badger_name).strip() if pd.notna(badger_name) and badger_name_col else ''
            
            # Determine protocol from hardware
            protocol = get_protocol_from_hardware(hardware_str)
            if not protocol:
                skipped_count += 1
                print(f"⚠ Warning: Skipping row with unknown hardware type (IP: {ip_str}, hardware: {hardware_str})")
                logger.warning(f"Row {row_num}: Skipping - unknown hardware type (IP: {ip_str}, hardware: {hardware_str})")
                continue
            
            logger.debug(f"Row {row_num}: Hardware '{hardware_str}' mapped to protocol '{protocol}'")
            
            # Determine port from protocol
            port_int = get_port_from_protocol(protocol)
            if not port_int:
                skipped_count += 1
                print(f"⚠ Warning: Skipping row - no port mapping for protocol (IP: {ip_str}, protocol: {protocol})")
                logger.warning(f"Row {row_num}: Skipping - no port mapping for protocol (IP: {ip_str}, protocol: {protocol})")
                continue
            
            logger.debug(f"Row {row_num}: Protocol '{protocol}' mapped to port {port_int}")
            
            # Look up card ID using IP:port (from lookup table)
            # The lookup table uses "server:port" as keys
            card_id = None
            server_port_key = f"{ip_str}:{port_int}"
            
            logger.debug(f"Row {row_num}: Looking up card ID for IP:port '{server_port_key}'")
            
            # Try server:port first (primary lookup method)
            if server_port_key in card_lookup:
                card_id = card_lookup[server_port_key]
                logger.debug(f"Row {row_num}: Found card ID {card_id} using IP:port '{server_port_key}'")
            # Try Badger Name as fallback if available
            elif badger_name_str and badger_name_str in card_lookup:
                card_id = card_lookup[badger_name_str]
                logger.debug(f"Row {row_num}: Found card ID {card_id} using Badger Name '{badger_name_str}'")
            
            if not card_id:
                skipped_count += 1
                print(f"⚠ Warning: Could not find card ID for IP:port '{server_port_key}' or Badger Name '{badger_name_str}'")
                logger.warning(f"Row {row_num}: Could not find card ID for IP:port '{server_port_key}' or Badger Name '{badger_name_str}'")
                continue
            
            # Determine channel: Check Relay # column
            # ProXr channels should be 0, WebSwitch uses Relay # if present
            channel_int = 0  # Default to 0
            
            if protocol == "ProXr":
                # ProXr channels are always 0
                channel_int = 0
                logger.debug(f"Row {row_num}: ProXr protocol - channel set to 0")
            elif protocol == "WebRelayHttp":
                # WebSwitch: Use Relay # (WebSwitch Only) column if present
                try:
                    if pd.notna(relay) and str(relay).strip():
                        channel_int = int(float(relay))
                        logger.debug(f"Row {row_num}: WebSwitch protocol - channel set to {channel_int} from Relay # column")
                    else:
                        logger.debug(f"Row {row_num}: WebSwitch protocol - Relay # empty, channel set to 0")
                    # If Relay # is empty, keep channel as 0
                except (ValueError, TypeError):
                    print(f"⚠ Warning: Invalid Relay # value '{relay}' for IP: {ip_str}, defaulting to 0")
                    logger.warning(f"Row {row_num}: Invalid Relay # value '{relay}' for IP: {ip_str}, defaulting to 0")
                    channel_int = 0
            
            # Convert interlock name to None if empty
            name_str = None
            if interlock_name_col and pd.notna(interlock_name) and str(interlock_name).strip():
                name_str = str(interlock_name).strip()
                if name_str == '' or name_str == 'nan':
                    name_str = None
            
            interlock_data = {
                'card': card_id,
                'name': name_str,
                'channel': channel_int,
                'unit_id': 0,  # Default to 0
                'state': 1  # Default to 1
            }
            interlocks.append(interlock_data)
            logger.info(f"Row {row_num}: Prepared interlock - Card ID: {card_id}, Name: {name_str}, Channel: {channel_int}, IP: {ip_str}, Badger Name: {badger_name_str}")
        
        print(f"Found {len(interlocks)} interlock entries in {file_path}")
        logger.info(f"Successfully read {len(interlocks)} interlock entries from {file_path}")
        if skipped_count > 0:
            logger.info(f"Skipped {skipped_count} rows due to missing data or lookup failures")
        return interlocks
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        logger.error(f"Error reading {file_path}: {e}", exc_info=True)
        return []

def read_interlocks_from_excel(file_path: str, card_lookup: Dict[str, int]) -> List[Dict[str, Any]]:
    """Read interlock data from an Excel file."""
    try:
        df = pd.read_excel(file_path)
        
        # Look for card identifier column (can be card name, server:port, or card_id)
        card_col = None
        name_col = None
        channel_col = None
        unit_id_col = None
        state_col = None
        
        for col in df.columns:
            col_lower = col.lower().strip()
            if 'card' in col_lower and ('name' in col_lower or 'id' in col_lower or 'identifier' in col_lower or 'server' in col_lower):
                card_col = col
            elif col_lower == 'name':
                name_col = col
            elif col_lower == 'channel':
                channel_col = col
            elif 'unit' in col_lower and 'id' in col_lower:
                unit_id_col = col
            elif col_lower == 'state':
                state_col = col
        
        if not card_col:
            print(f"Warning: No card identifier column found in {file_path}")
            print(f"Available columns: {df.columns.tolist()}")
            print("Expected columns: card (name or server:port), name (optional), channel (optional), unit_id (optional), state (optional)")
            return []
        
        print(f"Using columns: Card={card_col}, Name={name_col}, Channel={channel_col}, Unit ID={unit_id_col}, State={state_col}")
        
        interlocks = []
        for _, row in df.iterrows():
            card_identifier = row.get(card_col, '')
            name = row.get(name_col, '') if name_col else None
            channel = row.get(channel_col, 0) if channel_col else 0
            unit_id = row.get(unit_id_col, 0) if unit_id_col else 0
            state = row.get(state_col, 1) if state_col else 1
            
            # Skip if card identifier is missing
            if pd.isna(card_identifier) or not str(card_identifier).strip():
                continue
            
            # Convert to string and clean
            card_identifier_str = str(card_identifier).strip()
            
            # Look up card ID from identifier (could be name, server:port, or already an ID)
            card_id = None
            
            # First try direct lookup
            if card_identifier_str in card_lookup:
                card_id = card_lookup[card_identifier_str]
            else:
                # Try to parse as integer (might already be a card ID)
                try:
                    card_id_int = int(card_identifier_str)
                    # Check if this ID exists in the lookup values
                    if card_id_int in card_lookup.values():
                        card_id = card_id_int
                except ValueError:
                    pass
            
            if not card_id:
                print(f"⚠ Warning: Could not find card ID for identifier '{card_identifier_str}'")
                logger.warning(f"Could not find card ID for identifier '{card_identifier_str}'")
                continue
            
            # Convert name to None if empty
            name_str = str(name).strip() if pd.notna(name) and name_col else None
            if name_str == '' or name_str == 'nan':
                name_str = None
            
            # Convert channel, unit_id, and state to integers
            try:
                channel_int = int(float(channel)) if pd.notna(channel) else 0
            except (ValueError, TypeError):
                channel_int = 0
            
            try:
                unit_id_int = int(float(unit_id)) if pd.notna(unit_id) else 0
            except (ValueError, TypeError):
                unit_id_int = 0
            
            try:
                state_int = int(float(state)) if pd.notna(state) else 1
            except (ValueError, TypeError):
                state_int = 1
            
            interlock_data = {
                'card': card_id,
                'name': name_str,
                'channel': channel_int,
                'unit_id': unit_id_int,
                'state': state_int
            }
            interlocks.append(interlock_data)
        
        print(f"Found {len(interlocks)} interlock entries in {file_path}")
        logger.info(f"Read {len(interlocks)} interlock entries from {file_path}")
        return interlocks
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        logger.error(f"Error reading {file_path}: {e}")
        return []

def create_interlock_payload(card_id: int, name: Any = None, channel: int = 0, unit_id: int = 0, state: int = 1) -> Dict[str, Any]:
    """Create an interlock payload with the given parameters."""
    payload = {
        "card": card_id,
        "channel": channel,
        "unit_id": unit_id,
        "state": state
    }
    
    # Only include name if it's not None
    if name is not None:
        payload["name"] = name
    
    return payload

def push_interlock_to_api(card_id: int, name: Any, channel: int, unit_id: int, state: int, api_url: str) -> bool:
    """Push a single interlock to the NEMO API."""
    logger.debug(f"Creating payload for interlock: card_id={card_id}, name='{name}', channel={channel}, unit_id={unit_id}, state={state}")
    try:
        payload = create_interlock_payload(card_id, name, channel, unit_id, state)
        logger.info(f"Attempting to create interlock: card_id={card_id}, name='{name}', channel={channel}, unit_id={unit_id}, state={state}")
        logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
    except ValueError as e:
        print(f"✗ Invalid data for interlock (card_id={card_id}): {e}")
        logger.error(f"Invalid data for interlock (card_id={card_id}): {e}")
        return False
    
    try:
        logger.debug(f"Sending POST request to {api_url}")
        response = requests.post(api_url, json=payload, headers=API_HEADERS)
        logger.debug(f"Response status code: {response.status_code}")
        
        if response.status_code == 201:  # Created
            print(f"✓ Successfully created interlock: Card ID {card_id}, Name: {name or '(none)'}, Channel: {channel}, Unit ID: {unit_id}, State: {state}")
            logger.info(f"SUCCESS: Created interlock - card_id={card_id}, name='{name}', channel={channel}, unit_id={unit_id}, state={state}")
            # Try to log the created interlock ID if available in response
            try:
                response_data = response.json()
                if 'id' in response_data:
                    logger.info(f"Created interlock ID: {response_data['id']}")
            except:
                pass
            return True
        elif response.status_code == 200:  # OK (some APIs return 200 for creation)
            print(f"✓ Successfully created interlock: Card ID {card_id}, Name: {name or '(none)'}, Channel: {channel}, Unit ID: {unit_id}, State: {state}")
            logger.info(f"SUCCESS: Created interlock - card_id={card_id}, name='{name}', channel={channel}, unit_id={unit_id}, state={state}")
            # Try to log the created interlock ID if available in response
            try:
                response_data = response.json()
                if 'id' in response_data:
                    logger.info(f"Created interlock ID: {response_data['id']}")
            except:
                pass
            return True
        elif response.status_code == 400:
            print(f"✗ Bad request for interlock (card_id={card_id}): {response.text}")
            logger.error(f"FAILED: Bad request for interlock (card_id={card_id}) - {response.text}")
            return False
        elif response.status_code == 401:
            print(f"✗ Authentication failed for interlock (card_id={card_id}): Check your NEMO_TOKEN")
            logger.error(f"FAILED: Authentication failed for interlock (card_id={card_id})")
            return False
        elif response.status_code == 403:
            print(f"✗ Permission denied for interlock (card_id={card_id}): Check your API permissions")
            logger.error(f"FAILED: Permission denied for interlock (card_id={card_id})")
            return False
        elif response.status_code == 409:
            print(f"⚠ Interlock (card_id={card_id}, channel={channel}, unit_id={unit_id}) already exists (conflict)")
            logger.warning(f"SKIPPED: Interlock (card_id={card_id}, channel={channel}, unit_id={unit_id}) already exists (conflict)")
            return False
        else:
            print(f"✗ Failed to create interlock (card_id={card_id}): HTTP {response.status_code} - {response.text}")
            logger.error(f"FAILED: Interlock (card_id={card_id}) - HTTP {response.status_code} - {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error creating interlock (card_id={card_id}): {e}")
        logger.error(f"FAILED: Network error creating interlock (card_id={card_id}): {e}")
        return False

def main():
    """Main function to read interlocks and push to API."""
    print("Starting interlock creation script...")
    print(f"API Endpoint: {NEMO_API_URL}")
    print(f"Log file: {log_filename}")
    print("-" * 50)
    logger.info("=" * 60)
    logger.info("Starting interlock creation script")
    logger.info(f"API Endpoint: {NEMO_API_URL}")
    logger.info(f"Log file: {log_filename}")
    logger.info("=" * 60)
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Load interlock card lookup (required for mapping card identifiers to IDs)
    print("Loading interlock card lookup...")
    card_lookup = load_interlock_card_lookup()
    
    if not card_lookup:
        print("✗ Error: Interlock card lookup is required for interlock creation.")
        print("Please run download_interlock_cards.py first to download interlock cards from NEMO.")
        return
    
    print("✓ Interlock card lookup loaded successfully")
    logger.info(f"Interlock card lookup loaded with {len(card_lookup)} entries")
    
    # File to process - BadgerBoxes.csv
    data_file = "SNSF-Data/BadgerBoxes.csv"
    
    all_interlocks = []
    
    # Read interlocks from file
    if not os.path.exists(data_file):
        print(f"✗ Error: File not found: {data_file}")
        print("Please ensure BadgerBoxes.csv exists in the SNSF-Data directory.")
        logger.error(f"Data file not found: {data_file}")
        return
    
    logger.info(f"Reading interlocks from file: {data_file}")
    
    if data_file.endswith('.csv'):
        interlocks = read_interlocks_from_csv(data_file, card_lookup)
    elif data_file.endswith(('.xlsx', '.xls')):
        interlocks = read_interlocks_from_excel(data_file, card_lookup)
    else:
        print(f"✗ Error: Unsupported file format. Please use CSV or Excel (.xlsx, .xls)")
        logger.error(f"Unsupported file format: {data_file}")
        return
    
    all_interlocks.extend(interlocks)
    
    print(f"\nTotal interlocks found: {len(all_interlocks)}")
    print("-" * 50)
    logger.info(f"Total interlocks found: {len(all_interlocks)}")
    
    if not all_interlocks:
        print("No interlocks found to create!")
        logger.warning("No interlocks found to create!")
        return
    
    print(f"\nReady to create {len(all_interlocks)} interlocks in NEMO API...")
    logger.info(f"Starting to create {len(all_interlocks)} interlocks in NEMO API")
    
    # Push interlocks to API
    successful_creates = 0
    failed_creates = 0
    
    for i, interlock in enumerate(all_interlocks, 1):
        card_id = interlock['card']
        name = interlock.get('name')
        channel = interlock.get('channel', 0)
        unit_id = interlock.get('unit_id', 0)
        state = interlock.get('state', 1)
        
        print(f"\n[{i}/{len(all_interlocks)}] Creating: Card ID {card_id}, Name: {name or '(none)'}, Channel: {channel}, Unit ID: {unit_id}, State: {state}")
        logger.info(f"Processing interlock {i}/{len(all_interlocks)}: Card ID {card_id}, Name: {name or '(none)'}, Channel: {channel}, Unit ID: {unit_id}, State: {state}")
        
        if push_interlock_to_api(card_id, name, channel, unit_id, state, NEMO_API_URL):
            successful_creates += 1
            logger.debug(f"Interlock {i}/{len(all_interlocks)} created successfully")
        else:
            failed_creates += 1
            logger.debug(f"Interlock {i}/{len(all_interlocks)} failed to create")
        
        # Add a small delay to avoid overwhelming the API
        time.sleep(0.5)
    
    # Summary
    print("\n" + "=" * 50)
    print("CREATION SUMMARY")
    print("=" * 50)
    print(f"Total interlocks processed: {len(all_interlocks)}")
    print(f"Successful creations: {successful_creates}")
    print(f"Failed creations: {failed_creates}")
    print(f"Success rate: {(successful_creates/len(all_interlocks)*100):.1f}%")
    
    logger.info("=" * 60)
    logger.info("CREATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total interlocks processed: {len(all_interlocks)}")
    logger.info(f"Successful creations: {successful_creates}")
    logger.info(f"Failed creations: {failed_creates}")
    logger.info(f"Success rate: {(successful_creates/len(all_interlocks)*100):.1f}%")
    
    if failed_creates > 0:
        print(f"\nNote: {failed_creates} interlocks failed to create.")
        print("These may need to be added manually or have their data corrected.")
        logger.warning(f"{failed_creates} interlocks failed to create - check log for details")
    
    logger.info("=" * 60)
    logger.info("Script completed")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()

