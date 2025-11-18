#!/usr/bin/env python3
"""
Script to push interlock cards from SNSF CSV file to NEMO API endpoint.
Reads IP (hostname), port, and name from BadgerBoxes.csv and pushes them to the API.
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

# NEMO API endpoint
NEMO_API_URL = "https://nemo.stanford.edu/api/interlock_cards/"

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

# Set up logging
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

def load_category_lookup(filename: str = "interlock_card_category_lookup.json") -> Dict[str, int]:
    """Load the interlock card category lookup from the downloaded categories."""
    try:
        with open(filename, 'r') as f:
            lookup = json.load(f)
        print(f"✓ Loaded category lookup with {len(lookup)} categories")
        logger.info(f"Loaded category lookup from {filename} with {len(lookup)} categories")
        return lookup
    except FileNotFoundError:
        print(f"✗ Category lookup file {filename} not found!")
        print("Please run download_interlock_card_categories.py first to download categories from NEMO.")
        logger.error(f"Category lookup file {filename} not found!")
        return {}
    except Exception as e:
        print(f"✗ Error loading category lookup: {e}")
        logger.error(f"Error loading category lookup: {e}")
        return {}

def read_interlocks_from_csv(file_path: str) -> List[Dict[str, Any]]:
    """Read hostname (IP), name, and hardware from a CSV file. Port is determined from hardware type."""
    try:
        df = pd.read_csv(file_path)
        
        # Look for IP, Badger Name, Instrument Name, and Hardware columns
        ip_col = None
        badger_name_col = None
        instrument_name_col = None
        hardware_col = None
        
        for col in df.columns:
            col_lower = col.lower().strip()
            if col_lower == 'ip':
                ip_col = col
            elif 'badger name' in col_lower:
                badger_name_col = col
            elif 'instrument name' in col_lower:
                instrument_name_col = col
            elif col_lower == 'hardware':
                hardware_col = col
        
        if not ip_col:
            print(f"Warning: No IP column found in {file_path}")
            print(f"Available columns: {df.columns.tolist()}")
            return []
        
        if not hardware_col:
            print(f"Warning: No Hardware column found in {file_path}")
            print(f"Available columns: {df.columns.tolist()}")
            return []
        
        print(f"Using columns: IP={ip_col}, Badger Name={badger_name_col}, Instrument Name={instrument_name_col}, Hardware={hardware_col}")
        
        interlocks = []
        for _, row in df.iterrows():
            ip = row.get(ip_col, '')
            badger_name = row.get(badger_name_col, '') if badger_name_col else ''
            instrument_name = row.get(instrument_name_col, '') if instrument_name_col else ''
            hardware = row.get(hardware_col, '')
            
            # Skip if IP is missing
            if pd.isna(ip):
                continue
            
            # Convert to string and clean
            ip_str = str(ip).strip() if pd.notna(ip) else ''
            badger_name_str = str(badger_name).strip() if pd.notna(badger_name) and badger_name_col else ''
            instrument_name_str = str(instrument_name).strip() if pd.notna(instrument_name) and instrument_name_col else ''
            hardware_str = str(hardware).strip() if pd.notna(hardware) else ''
            
            # Skip if IP is empty
            if not ip_str:
                continue
            
            # Skip if hardware is empty (needed to determine protocol and port)
            if not hardware_str:
                print(f"⚠ Warning: Skipping row with empty hardware (IP: {ip_str}, Badger Name: {badger_name_str or instrument_name_str})")
                logger.warning(f"Skipping row with empty hardware (IP: {ip_str})")
                continue
            
            # Determine protocol from hardware
            protocol = get_protocol_from_hardware(hardware_str)
            if not protocol:
                print(f"⚠ Warning: Skipping row with unknown hardware type (IP: {ip_str}, hardware: {hardware_str})")
                logger.warning(f"Skipping row with unknown hardware type (IP: {ip_str}, hardware: {hardware_str})")
                continue
            
            # Determine port from protocol
            port_int = get_port_from_protocol(protocol)
            if not port_int:
                print(f"⚠ Warning: Skipping row - no port mapping for protocol (IP: {ip_str}, protocol: {protocol})")
                logger.warning(f"Skipping row - no port mapping for protocol (IP: {ip_str}, protocol: {protocol})")
                continue
            
            # Use Badger Name if available, otherwise fall back to Instrument Name
            name_str = badger_name_str if badger_name_str else instrument_name_str
            
            # Skip if name is empty (both Badger Name and Instrument Name are empty)
            if not name_str:
                print(f"⚠ Warning: Skipping row with empty name (IP: {ip_str}, protocol: {protocol}, port: {port_int})")
                logger.warning(f"Skipping row with empty name (IP: {ip_str}, protocol: {protocol}, port: {port_int})")
                continue
            
            # Map hardware to hardware_id
            hardware_id = get_hardware_id(hardware_str)
            
            # Store interlock data with port determined from hardware/protocol
            interlock_data = {
                'name': name_str,
                'hostname': ip_str,
                'port': port_int,  # Port determined from hardware → protocol → port
                'hardware_id': hardware_id,
                'protocol': protocol  # Store protocol for later use
            }
            interlocks.append(interlock_data)
        
        print(f"Found {len(interlocks)} interlock entries in {file_path}")
        logger.info(f"Read {len(interlocks)} interlock entries from {file_path}")
        return interlocks
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        logger.error(f"Error reading {file_path}: {e}")
        return []

def read_interlocks_from_excel(file_path: str) -> List[Dict[str, Any]]:
    """Read hostname, port, and name from an Excel file."""
    try:
        df = pd.read_excel(file_path)
        
        # Look for hostname, port, and name columns (case-insensitive)
        hostname_col = None
        port_col = None
        name_col = None
        
        for col in df.columns:
            col_lower = col.lower()
            if 'hostname' in col_lower or 'host name' in col_lower or 'host' in col_lower:
                hostname_col = col
            elif col_lower == 'port':
                port_col = col
            elif col_lower == 'name':
                name_col = col
        
        if not hostname_col:
            print(f"Warning: No hostname column found in {file_path}")
            print(f"Available columns: {df.columns.tolist()}")
            return []
        
        if not port_col:
            print(f"Warning: No port column found in {file_path}")
            print(f"Available columns: {df.columns.tolist()}")
            return []
        
        if not name_col:
            print(f"Warning: No name column found in {file_path}")
            print(f"Available columns: {df.columns.tolist()}")
            return []
        
        print(f"Using columns: {name_col}, {hostname_col}, {port_col}")
        
        interlocks = []
        for _, row in df.iterrows():
            hostname = row.get(hostname_col, '')
            port = row.get(port_col, '')
            name = row.get(name_col, '')
            
            # Skip if both hostname and port are missing
            if pd.isna(hostname) and pd.isna(port):
                continue
            
            # Convert to string and clean
            hostname_str = str(hostname).strip() if pd.notna(hostname) else ''
            port_str = str(port).strip() if pd.notna(port) else ''
            name_str = str(name).strip() if pd.notna(name) else ''
            
            # Skip if hostname is empty (port alone is not enough)
            if not hostname_str:
                continue
            
            # Skip if name is empty
            if not name_str:
                print(f"⚠ Warning: Skipping row with empty name (hostname: {hostname_str}, port: {port_str})")
                continue
            
            # Convert port to integer if possible, otherwise keep as string
            try:
                port_int = int(float(port_str)) if port_str else None
            except (ValueError, TypeError):
                port_int = None
            
            interlock_data = {
                'name': name_str,
                'hostname': hostname_str,
                'port': port_int if port_int is not None else port_str
            }
            interlocks.append(interlock_data)
        
        print(f"Found {len(interlocks)} interlock entries in {file_path}")
        return interlocks
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []

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

def get_protocol_name_from_port(port: Any) -> str:
    """Get protocol name from port for display/logging purposes."""
    try:
        port_int = int(float(str(port))) if port else None
    except (ValueError, TypeError):
        return None
    
    port_to_protocol = {
        2101: "ProXr",
        80: "WebRelayHttp"
    }
    
    return port_to_protocol.get(port_int)

def get_hardware_id(hardware: Any) -> int:
    """Map hardware name to hardware ID."""
    # Handle None, empty string, or pandas NaN
    if hardware is None or (isinstance(hardware, str) and not hardware.strip()):
        return 2  # Default
    
    # Handle pandas NaN
    try:
        if pd.isna(hardware):
            return 2  # Default
    except (TypeError, ValueError):
        pass
    
    hardware_str = str(hardware).strip()
    
    # Hardware to ID mapping
    hardware_mapping = {
        "NCD v2": 3,
        "WebSwitch Plus": 2
    }
    
    # Check for exact match first
    if hardware_str in hardware_mapping:
        return hardware_mapping[hardware_str]
    
    # Check for case-insensitive partial matches
    hardware_lower = hardware_str.lower()
    for key, value in hardware_mapping.items():
        if key.lower() in hardware_lower or hardware_lower in key.lower():
            return value
    
    # Default to 2 if no match found
    return 2

def create_interlock_payload(name: str, hostname: str, port: Any, protocol: str, category_lookup: Dict[str, int], hardware_id: int = 2) -> Dict[str, Any]:
    """Create an interlock card payload with the given name, hostname, port, protocol, and hardware_id."""
    # Convert port to integer
    try:
        port_int = int(float(str(port))) if port else None
    except (ValueError, TypeError):
        port_int = None
    
    if port_int is None:
        raise ValueError(f"Invalid port value: {port}")
    
    # Map protocol to category ID
    if protocol not in category_lookup:
        raise ValueError(f"Protocol '{protocol}' not found in category lookup")
    
    category_id = category_lookup[protocol]
    
    # Create payload matching API format
    payload = {
        "name": name,
        "server": hostname,  # API uses "server" not "hostname"
        "port": port_int,
        "enabled": False,
        "category": category_id,
        "hardware": hardware_id,
        "number": None,
        "even_port": None,
        "odd_port": None,
        "username": None,
        "password": None,
        "extra_args": None
    }
    
    return payload

def push_interlock_to_api(name: str, hostname: str, port: Any, protocol: str, api_url: str, category_lookup: Dict[str, int], hardware_id: int = 2) -> bool:
    """Push a single interlock card to the NEMO API."""
    try:
        payload = create_interlock_payload(name, hostname, port, protocol, category_lookup, hardware_id)
        logger.info(f"Attempting to create interlock card: name='{name}', server='{hostname}', port={port}, protocol={protocol}, hardware_id={hardware_id}")
    except ValueError as e:
        print(f"✗ Invalid data for interlock '{name}' ({hostname}:{port}): {e}")
        logger.error(f"Invalid data for interlock '{name}' ({hostname}:{port}): {e}")
        return False
    
    try:
        response = requests.post(api_url, json=payload, headers=API_HEADERS)
        
        if response.status_code == 201:  # Created
            category_id = category_lookup.get(protocol, 'unknown')
            print(f"✓ Successfully pushed interlock: {name} ({hostname}:{port}) - Protocol: {protocol}, Category ID: {category_id}")
            logger.info(f"SUCCESS: Created interlock card - name='{name}', server='{hostname}', port={port}, protocol={protocol}, category_id={category_id}, hardware_id={hardware_id}")
            # Try to log the created interlock ID if available in response
            try:
                response_data = response.json()
                if 'id' in response_data:
                    logger.info(f"Created interlock card ID: {response_data['id']}")
            except:
                pass
            return True
        elif response.status_code == 200:  # OK (some APIs return 200 for creation)
            category_id = category_lookup.get(protocol, 'unknown')
            print(f"✓ Successfully pushed interlock: {name} ({hostname}:{port}) - Protocol: {protocol}, Category ID: {category_id}")
            logger.info(f"SUCCESS: Created interlock card - name='{name}', server='{hostname}', port={port}, protocol={protocol}, category_id={category_id}, hardware_id={hardware_id}")
            # Try to log the created interlock ID if available in response
            try:
                response_data = response.json()
                if 'id' in response_data:
                    logger.info(f"Created interlock card ID: {response_data['id']}")
            except:
                pass
            return True
        elif response.status_code == 400:
            print(f"✗ Bad request for interlock '{name}' ({hostname}:{port}): {response.text}")
            logger.error(f"FAILED: Bad request for interlock '{name}' ({hostname}:{port}) - {response.text}")
            return False
        elif response.status_code == 401:
            print(f"✗ Authentication failed for interlock '{name}' ({hostname}:{port}): Check your NEMO_TOKEN")
            logger.error(f"FAILED: Authentication failed for interlock '{name}' ({hostname}:{port})")
            return False
        elif response.status_code == 403:
            print(f"✗ Permission denied for interlock '{name}' ({hostname}:{port}): Check your API permissions")
            logger.error(f"FAILED: Permission denied for interlock '{name}' ({hostname}:{port})")
            return False
        elif response.status_code == 409:
            print(f"⚠ Interlock '{name}' ({hostname}:{port}) already exists (conflict)")
            logger.warning(f"SKIPPED: Interlock '{name}' ({hostname}:{port}) already exists (conflict)")
            return False
        else:
            print(f"✗ Failed to push interlock '{name}' ({hostname}:{port}): HTTP {response.status_code} - {response.text}")
            logger.error(f"FAILED: Interlock '{name}' ({hostname}:{port}) - HTTP {response.status_code} - {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error pushing interlock '{name}' ({hostname}:{port}): {e}")
        logger.error(f"FAILED: Network error pushing interlock '{name}' ({hostname}:{port}): {e}")
        return False

def main():
    """Main function to read interlocks and push to API."""
    print("Starting interlock card push to NEMO API...")
    print(f"API Endpoint: {NEMO_API_URL}")
    print(f"Log file: {log_filename}")
    print("-" * 50)
    logger.info("=" * 60)
    logger.info("Starting interlock card creation script")
    logger.info(f"API Endpoint: {NEMO_API_URL}")
    logger.info(f"Log file: {log_filename}")
    logger.info("=" * 60)
    
    # Test API connection first
    if not test_api_connection():
        print("Cannot proceed without valid API connection.")
        return
    
    # Load category lookup (required for port-to-protocol mapping)
    print("Loading interlock card category lookup...")
    category_lookup = load_category_lookup()
    
    if not category_lookup:
        print("✗ Error: Category lookup is required for interlock creation.")
        print("Please run download_interlock_card_categories.py first to download categories from NEMO.")
        return
    
    # Verify required protocols are in the lookup
    required_protocols = ["ProXr", "WebRelayHttp"]
    missing_protocols = [p for p in required_protocols if p not in category_lookup]
    if missing_protocols:
        print(f"✗ Error: Required protocols not found in category lookup: {missing_protocols}")
        print("Please ensure the category lookup includes ProXr and WebRelayHttp.")
        return
    
    print("✓ Category lookup loaded successfully")
    print(f"  Port 2101 → ProXr (Category ID: {category_lookup['ProXr']})")
    print(f"  Port 80 → WebRelayHttp (Category ID: {category_lookup['WebRelayHttp']})")
    logger.info(f"Category lookup loaded - ProXr: {category_lookup['ProXr']}, WebRelayHttp: {category_lookup['WebRelayHttp']}")
    
    # CSV file to process
    csv_file = "SNSF-Data/BadgerBoxes.csv"
    
    all_interlocks = []
    
    # Read interlocks (IP, port, and name) from CSV file
    if not os.path.exists(csv_file):
        print(f"✗ Error: File not found: {csv_file}")
        logger.error(f"CSV file not found: {csv_file}")
        return
    
    logger.info(f"Reading interlocks from CSV file: {csv_file}")
    interlocks = read_interlocks_from_csv(csv_file)
    all_interlocks.extend(interlocks)
    
    # Get unique interlocks by hostname:port combination
    seen_combinations = set()
    unique_interlocks = []
    for interlock in all_interlocks:
        combination_key = f"{interlock['hostname']}:{interlock['port']}"
        if combination_key not in seen_combinations:
            seen_combinations.add(combination_key)
            unique_interlocks.append(interlock)
        else:
            # If duplicate hostname:port, prefer the one with a name if existing doesn't have one
            existing_idx = next((i for i, ic in enumerate(unique_interlocks) if f"{ic['hostname']}:{ic['port']}" == combination_key), None)
            if existing_idx is not None and not unique_interlocks[existing_idx].get('name') and interlock.get('name'):
                unique_interlocks[existing_idx] = interlock
    
    print(f"\nTotal unique interlocks found: {len(unique_interlocks)}")
    print("-" * 50)
    logger.info(f"Total unique interlocks found: {len(unique_interlocks)}")
    
    if not unique_interlocks:
        print("No interlocks found to push!")
        logger.warning("No interlocks found to push!")
        return
    
    print(f"\nReady to push {len(unique_interlocks)} interlock cards to NEMO API...")
    logger.info(f"Starting to push {len(unique_interlocks)} interlock cards to NEMO API")
    
    # Push interlocks to API
    successful_pushes = 0
    failed_pushes = 0
    
    for i, interlock in enumerate(unique_interlocks, 1):
        name = interlock['name']
        hostname = interlock['hostname']
        port = interlock['port']
        protocol = interlock.get('protocol')  # Protocol determined from hardware
        hardware_id = interlock.get('hardware_id', 2)  # Default to 2 if not present
        
        if not protocol:
            print(f"\n[{i}/{len(unique_interlocks)}] Pushing: {name} ({hostname}:{port}) ⚠ (no protocol)")
            logger.warning(f"Skipping interlock '{name}' ({hostname}:{port}) - no protocol found")
            failed_pushes += 1
            continue
        
        print(f"\n[{i}/{len(unique_interlocks)}] Pushing: {name} ({hostname}:{port}) - {protocol}")
        
        if push_interlock_to_api(name, hostname, port, protocol, NEMO_API_URL, category_lookup, hardware_id):
            successful_pushes += 1
        else:
            failed_pushes += 1
        
        # Add a small delay to avoid overwhelming the API
        time.sleep(0.5)
    
    # Summary
    print("\n" + "=" * 50)
    print("PUSH SUMMARY")
    print("=" * 50)
    print(f"Total interlocks processed: {len(unique_interlocks)}")
    print(f"Successful pushes: {successful_pushes}")
    print(f"Failed pushes: {failed_pushes}")
    print(f"Success rate: {(successful_pushes/len(unique_interlocks)*100):.1f}%")
    
    logger.info("=" * 60)
    logger.info("PUSH SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total interlocks processed: {len(unique_interlocks)}")
    logger.info(f"Successful pushes: {successful_pushes}")
    logger.info(f"Failed pushes: {failed_pushes}")
    logger.info(f"Success rate: {(successful_pushes/len(unique_interlocks)*100):.1f}%")
    
    if failed_pushes > 0:
        print(f"\nNote: {failed_pushes} interlocks failed to push.")
        print("These may need to be added manually or have their data corrected.")
        logger.warning(f"{failed_pushes} interlocks failed to push - check log for details")
    
    logger.info("=" * 60)
    logger.info("Script completed")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()

