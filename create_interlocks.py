#!/usr/bin/env python3
"""
Script to push interlock cards from SNSF Excel files to NEMO API endpoint.
Reads hostname and port from SNC, SNL, and SMF Tools.xlsx files and pushes them to the API.
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

def load_category_lookup(filename: str = "interlock_card_category_lookup.json") -> Dict[str, int]:
    """Load the interlock card category lookup from the downloaded categories."""
    try:
        with open(filename, 'r') as f:
            lookup = json.load(f)
        print(f"✓ Loaded category lookup with {len(lookup)} categories")
        return lookup
    except FileNotFoundError:
        print(f"✗ Category lookup file {filename} not found!")
        print("Please run download_interlock_card_categories.py first to download categories from NEMO.")
        return {}
    except Exception as e:
        print(f"✗ Error loading category lookup: {e}")
        return {}

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

def get_protocol_from_port(port: Any) -> str:
    """Map port number to communication protocol name."""
    # Convert port to int for comparison
    try:
        port_int = int(float(str(port))) if port else None
    except (ValueError, TypeError):
        return None
    
    # Port to protocol mapping
    port_to_protocol = {
        2101: "ProXr",
        80: "WebRelayHttp"
    }
    
    return port_to_protocol.get(port_int)

def create_interlock_payload(name: str, hostname: str, port: Any, category_lookup: Dict[str, int]) -> Dict[str, Any]:
    """Create an interlock card payload with the given name, hostname and port."""
    # Convert port to integer
    try:
        port_int = int(float(str(port))) if port else None
    except (ValueError, TypeError):
        port_int = None
    
    if port_int is None:
        raise ValueError(f"Invalid port value: {port}")
    
    # Map port to protocol
    protocol_name = get_protocol_from_port(port_int)
    if not protocol_name:
        raise ValueError(f"No protocol mapping found for port {port_int}")
    
    # Look up category ID from protocol name
    if protocol_name not in category_lookup:
        raise ValueError(f"Protocol '{protocol_name}' not found in category lookup")
    
    category_id = category_lookup[protocol_name]
    
    # Create payload matching API format
    payload = {
        "name": name,
        "server": hostname,  # API uses "server" not "hostname"
        "port": port_int,
        "enabled": False,
        "category": category_id,
        "number": None,
        "even_port": None,
        "odd_port": None,
        "username": None,
        "password": None,
        "extra_args": None
    }
    
    return payload

def push_interlock_to_api(name: str, hostname: str, port: Any, api_url: str, category_lookup: Dict[str, int]) -> bool:
    """Push a single interlock card to the NEMO API."""
    try:
        payload = create_interlock_payload(name, hostname, port, category_lookup)
    except ValueError as e:
        print(f"✗ Invalid data for interlock '{name}' ({hostname}:{port}): {e}")
        return False
    
    try:
        response = requests.post(api_url, json=payload, headers=API_HEADERS)
        
        if response.status_code == 201:  # Created
            protocol = get_protocol_from_port(port)
            category_id = category_lookup.get(protocol, 'unknown')
            print(f"✓ Successfully pushed interlock: {name} ({hostname}:{port}) - Protocol: {protocol}, Category ID: {category_id}")
            return True
        elif response.status_code == 200:  # OK (some APIs return 200 for creation)
            protocol = get_protocol_from_port(port)
            category_id = category_lookup.get(protocol, 'unknown')
            print(f"✓ Successfully pushed interlock: {name} ({hostname}:{port}) - Protocol: {protocol}, Category ID: {category_id}")
            return True
        elif response.status_code == 400:
            print(f"✗ Bad request for interlock '{name}' ({hostname}:{port}): {response.text}")
            return False
        elif response.status_code == 401:
            print(f"✗ Authentication failed for interlock '{name}' ({hostname}:{port}): Check your NEMO_TOKEN")
            return False
        elif response.status_code == 403:
            print(f"✗ Permission denied for interlock '{name}' ({hostname}:{port}): Check your API permissions")
            return False
        elif response.status_code == 409:
            print(f"⚠ Interlock '{name}' ({hostname}:{port}) already exists (conflict)")
            return False
        else:
            print(f"✗ Failed to push interlock '{name}' ({hostname}:{port}): HTTP {response.status_code} - {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error pushing interlock '{name}' ({hostname}:{port}): {e}")
        return False

def main():
    """Main function to read interlocks and push to API."""
    print("Starting interlock card push to NEMO API...")
    print(f"API Endpoint: {NEMO_API_URL}")
    print("-" * 50)
    
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
    
    # Excel files to process
    excel_files = [
        "SNSF-Data/SNC Tools.xlsx",
        "SNSF-Data/SNL Tools.xlsx", 
        "SNSF-Data/SMF Tools.xlsx"
    ]
    
    all_interlocks = []
    
    # Read interlocks (hostname and port) from all Excel files
    for file_path in excel_files:
        if not os.path.exists(file_path):
            print(f"⚠ Warning: File not found: {file_path}")
            continue
        interlocks = read_interlocks_from_excel(file_path)
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
    
    if not unique_interlocks:
        print("No interlocks found to push!")
        return
    
    print(f"\nReady to push {len(unique_interlocks)} interlock cards to NEMO API...")
    
    # Push interlocks to API
    successful_pushes = 0
    failed_pushes = 0
    
    # Track unmapped ports for summary
    unmapped_ports = set()
    
    for i, interlock in enumerate(unique_interlocks, 1):
        name = interlock['name']
        hostname = interlock['hostname']
        port = interlock['port']
        
        # Check if port is mapped
        protocol = get_protocol_from_port(port)
        if not protocol:
            port_str = str(port)
            unmapped_ports.add(port_str)
            print(f"\n[{i}/{len(unique_interlocks)}] Pushing: {name} ({hostname}:{port}) ⚠ (unmapped port)")
        else:
            print(f"\n[{i}/{len(unique_interlocks)}] Pushing: {name} ({hostname}:{port}) - {protocol}")
        
        if push_interlock_to_api(name, hostname, port, NEMO_API_URL, category_lookup):
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
    
    if failed_pushes > 0:
        print(f"\nNote: {failed_pushes} interlocks failed to push.")
        print("These may need to be added manually or have their data corrected.")
    
    if unmapped_ports:
        print(f"\n⚠ Warning: Found {len(unmapped_ports)} unmapped port(s): {', '.join(sorted(unmapped_ports))}")
        print("Only ports 2101 (ProXr) and 80 (WebRelayHttp) are currently mapped.")
        print("Interlocks with unmapped ports were not uploaded.")

if __name__ == "__main__":
    main()

