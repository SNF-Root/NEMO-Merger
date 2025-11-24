#!/usr/bin/env python3
"""
Script to assign interlock IDs to tools based on name matching.
1. Downloads interlock cards (names and IDs)
2. Downloads interlocks and correlates with interlock cards
3. Downloads tools
4. Matches tool names with interlock card names
5. Assigns the respective interlock ID to matching tools
"""

import requests
import json
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional

# Load environment variables from .env file
load_dotenv()

# Set up logging first (before token check so we can log errors)
log_filename = f"logs/assign_interlocks_to_tools_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
NEMO_INTERLOCK_CARDS_API_URL = "https://nemo.stanford.edu/api/interlock_cards/"
NEMO_INTERLOCKS_API_URL = "https://nemo.stanford.edu/api/interlocks/"
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
    
    try:
        response = requests.get(api_url, headers=API_HEADERS)
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
            
            print(f"  Retrieved {len(items)} {item_name}")
            logger.info(f"Successfully downloaded {len(items)} {item_name}")
            return items
                
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

def create_interlock_card_lookup(cards: List[Dict[str, Any]]) -> Dict[str, int]:
    """Create a lookup mapping from interlock card names to card IDs."""
    logger.info(f"Creating interlock card lookup from {len(cards)} cards")
    lookup = {}
    duplicate_count = 0
    
    for card in cards:
        card_id = card.get('id')
        card_name = card.get('name', '').strip()
        
        if card_id and card_name:
            # Handle duplicate names - keep the first one
            if card_name not in lookup:
                lookup[card_name] = card_id
                logger.debug(f"Mapped interlock card '{card_name}' -> ID {card_id}")
            else:
                duplicate_count += 1
                print(f"⚠ Warning: Duplicate interlock card name '{card_name}' (IDs: {lookup[card_name]}, {card_id})")
                logger.warning(f"Duplicate interlock card name '{card_name}' (IDs: {lookup[card_name]}, {card_id})")
        else:
            logger.debug(f"Skipping card with missing ID or name: {card}")
    
    print(f"✓ Created interlock card lookup with {len(lookup)} entries")
    logger.info(f"Created interlock card lookup with {len(lookup)} entries ({duplicate_count} duplicates found)")
    return lookup

def create_card_to_interlock_mapping(interlocks: List[Dict[str, Any]]) -> Dict[int, int]:
    """
    Create a mapping from interlock card IDs to interlock IDs.
    If multiple interlocks exist for a card, use the first one found.
    """
    logger.info(f"Creating card-to-interlock mapping from {len(interlocks)} interlocks")
    mapping = {}
    duplicate_count = 0
    
    for interlock in interlocks:
        interlock_id = interlock.get('id')
        card_id = interlock.get('card')
        
        if interlock_id and card_id:
            # Handle multiple interlocks per card - keep the first one
            if card_id not in mapping:
                mapping[card_id] = interlock_id
                logger.debug(f"Mapped card {card_id} -> interlock {interlock_id}")
            else:
                duplicate_count += 1
                print(f"⚠ Warning: Multiple interlocks for card {card_id} (using interlock {mapping[card_id]}, skipping {interlock_id})")
                logger.warning(f"Multiple interlocks for card {card_id} (using interlock {mapping[card_id]}, skipping {interlock_id})")
        else:
            logger.debug(f"Skipping interlock with missing ID or card: {interlock}")
    
    print(f"✓ Created card-to-interlock mapping with {len(mapping)} entries")
    logger.info(f"Created card-to-interlock mapping with {len(mapping)} entries ({duplicate_count} cards with multiple interlocks)")
    return mapping

def create_name_to_interlock_mapping(
    card_lookup: Dict[str, int],
    card_to_interlock: Dict[int, int]
) -> Dict[str, int]:
    """
    Create a mapping from interlock card names to interlock IDs.
    This allows us to match tool names directly to interlock IDs.
    """
    logger.info(f"Creating name-to-interlock mapping from {len(card_lookup)} card names")
    name_to_interlock = {}
    missing_count = 0
    
    for card_name, card_id in card_lookup.items():
        if card_id in card_to_interlock:
            interlock_id = card_to_interlock[card_id]
            name_to_interlock[card_name] = interlock_id
            logger.debug(f"Mapped card name '{card_name}' (card {card_id}) -> interlock {interlock_id}")
        else:
            missing_count += 1
            print(f"⚠ Warning: No interlock found for card '{card_name}' (card ID: {card_id})")
            logger.warning(f"No interlock found for card '{card_name}' (card ID: {card_id})")
    
    print(f"✓ Created name-to-interlock mapping with {len(name_to_interlock)} entries")
    logger.info(f"Created name-to-interlock mapping with {len(name_to_interlock)} entries ({missing_count} cards without interlocks)")
    return name_to_interlock

def find_matching_tools(tools: List[Dict[str, Any]], name_to_interlock: Dict[str, int]) -> List[Dict[str, Any]]:
    """
    Find tools that match interlock card names and prepare them for update.
    Returns list of dicts with tool_id, tool_name, and interlock_id.
    """
    logger.info(f"Finding matching tools from {len(tools)} tools against {len(name_to_interlock)} interlock card names")
    matching_tools = []
    
    for tool in tools:
        tool_id = tool.get('id')
        tool_name = tool.get('name', '').strip()
        
        if tool_id and tool_name:
            # Try exact match first
            if tool_name in name_to_interlock:
                interlock_id = name_to_interlock[tool_name]
                matching_tools.append({
                    'tool_id': tool_id,
                    'tool_name': tool_name,
                    'interlock_id': interlock_id
                })
                logger.debug(f"Found match: tool '{tool_name}' (ID: {tool_id}) -> interlock {interlock_id}")
    
    print(f"✓ Found {len(matching_tools)} tools matching interlock card names")
    logger.info(f"Found {len(matching_tools)} tools matching interlock card names")
    return matching_tools

def update_tool_interlock(tool_id: int, interlock_id: int, tool_name: str = None) -> bool:
    """Update a tool's interlock field via the NEMO API."""
    update_url = f"{NEMO_TOOLS_API_URL}{tool_id}/"
    tool_display = f"'{tool_name}'" if tool_name else f"ID {tool_id}"
    logger.info(f"Updating tool {tool_id} {tool_display} with interlock {interlock_id}")
    
    try:
        payload = {
            '_interlock': interlock_id
        }
        logger.debug(f"Tool {tool_id} update payload: {json.dumps(payload)}")
        
        response = requests.patch(update_url, json=payload, headers=API_HEADERS)
        logger.debug(f"Tool {tool_id} update response status: {response.status_code}")
        
        if response.status_code == 200:
            logger.info(f"Successfully updated tool {tool_id} {tool_display} with interlock {interlock_id}")
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
    """Main function to download resources, correlate, and assign interlock IDs to tools."""
    logger.info("=" * 60)
    logger.info("Starting interlock assignment to tools script")
    logger.info(f"Log file: {log_filename}")
    logger.info("=" * 60)
    
    print("=" * 60)
    print("Assigning Interlock IDs to Tools")
    print("=" * 60)
    print("\nThis script will:")
    print("1. Download interlock cards (names and IDs)")
    print("2. Download interlocks and correlate with cards")
    print("3. Download tools")
    print("4. Match tool names with interlock card names")
    print("5. Assign interlock IDs to matching tools")
    print("-" * 60)
    
    # Test API connections
    print("\nTesting API connections...")
    logger.info("Testing API connections...")
    if not test_api_connection(NEMO_INTERLOCK_CARDS_API_URL, "Interlock Cards"):
        print("Cannot proceed without valid interlock cards API connection.")
        logger.error("Cannot proceed without valid interlock cards API connection.")
        return
    
    if not test_api_connection(NEMO_INTERLOCKS_API_URL, "Interlocks"):
        print("Cannot proceed without valid interlocks API connection.")
        logger.error("Cannot proceed without valid interlocks API connection.")
        return
    
    if not test_api_connection(NEMO_TOOLS_API_URL, "Tools"):
        print("Cannot proceed without valid tools API connection.")
        logger.error("Cannot proceed without valid tools API connection.")
        return
    
    # Step 1: Download interlock cards
    print("\n" + "=" * 60)
    print("Step 1: Downloading interlock cards...")
    print("=" * 60)
    logger.info("=" * 60)
    logger.info("Step 1: Downloading interlock cards")
    logger.info("=" * 60)
    interlock_cards = download_all_items(NEMO_INTERLOCK_CARDS_API_URL, "interlock cards")
    
    if not interlock_cards:
        print("No interlock cards downloaded. Cannot proceed.")
        logger.error("No interlock cards downloaded. Cannot proceed.")
        return
    
    # Create card name to card ID lookup
    card_lookup = create_interlock_card_lookup(interlock_cards)
    
    # Step 2: Download interlocks
    print("\n" + "=" * 60)
    print("Step 2: Downloading interlocks...")
    print("=" * 60)
    logger.info("=" * 60)
    logger.info("Step 2: Downloading interlocks")
    logger.info("=" * 60)
    interlocks = download_all_items(NEMO_INTERLOCKS_API_URL, "interlocks")
    
    if not interlocks:
        print("No interlocks downloaded. Cannot proceed.")
        logger.error("No interlocks downloaded. Cannot proceed.")
        return
    
    # Create card ID to interlock ID mapping
    card_to_interlock = create_card_to_interlock_mapping(interlocks)
    
    # Create name to interlock ID mapping
    name_to_interlock = create_name_to_interlock_mapping(card_lookup, card_to_interlock)
    
    if not name_to_interlock:
        print("No valid name-to-interlock mappings found. Cannot proceed.")
        logger.error("No valid name-to-interlock mappings found. Cannot proceed.")
        return
    
    # Step 3: Download tools
    print("\n" + "=" * 60)
    print("Step 3: Downloading tools...")
    print("=" * 60)
    logger.info("=" * 60)
    logger.info("Step 3: Downloading tools")
    logger.info("=" * 60)
    tools = download_all_items(NEMO_TOOLS_API_URL, "tools")
    
    if not tools:
        print("No tools downloaded. Cannot proceed.")
        logger.error("No tools downloaded. Cannot proceed.")
        return
    
    # Step 4: Find matching tools
    print("\n" + "=" * 60)
    print("Step 4: Finding tools matching interlock card names...")
    print("=" * 60)
    logger.info("=" * 60)
    logger.info("Step 4: Finding tools matching interlock card names")
    logger.info("=" * 60)
    matching_tools = find_matching_tools(tools, name_to_interlock)
    
    if not matching_tools:
        print("No tools found matching interlock card names.")
        logger.warning("No tools found matching interlock card names.")
        print("\nSample interlock card names:")
        logger.info("Sample interlock card names:")
        sample_count = 0
        for card_name in list(name_to_interlock.keys())[:10]:
            print(f"  - {card_name}")
            logger.info(f"  - {card_name}")
            sample_count += 1
        if len(name_to_interlock) > 10:
            print(f"  ... and {len(name_to_interlock) - 10} more")
            logger.info(f"  ... and {len(name_to_interlock) - 10} more")
        return
    
    # Show preview of matches
    print(f"\nFound {len(matching_tools)} matching tools:")
    logger.info(f"Found {len(matching_tools)} matching tools:")
    for match in matching_tools[:10]:
        print(f"  - Tool '{match['tool_name']}' (ID: {match['tool_id']}) → Interlock ID: {match['interlock_id']}")
        logger.info(f"  - Tool '{match['tool_name']}' (ID: {match['tool_id']}) → Interlock ID: {match['interlock_id']}")
    if len(matching_tools) > 10:
        print(f"  ... and {len(matching_tools) - 10} more")
        logger.info(f"  ... and {len(matching_tools) - 10} more")
    
    # Step 5: Update tools
    print("\n" + "=" * 60)
    print("Step 5: Assigning interlock IDs to tools...")
    print("=" * 60)
    logger.info("=" * 60)
    logger.info("Step 5: Assigning interlock IDs to tools")
    logger.info("=" * 60)
    
    success_count = 0
    failed_count = 0
    skipped_count = 0
    
    for idx, match in enumerate(matching_tools, 1):
        tool_id = match['tool_id']
        tool_name = match['tool_name']
        interlock_id = match['interlock_id']
        
        logger.info(f"[{idx}/{len(matching_tools)}] Processing tool {tool_id} '{tool_name}'")
        
        # Check if tool already has this interlock assigned
        tool = next((t for t in tools if t.get('id') == tool_id), None)
        if tool:
            current_interlock = tool.get('_interlock')
            if current_interlock == interlock_id:
                print(f"  ⊘ Tool {tool_id} '{tool_name}' already has interlock {interlock_id} assigned. Skipping.")
                logger.info(f"Tool {tool_id} '{tool_name}' already has interlock {interlock_id} assigned. Skipping.")
                skipped_count += 1
                continue
        
        print(f"  Updating tool {tool_id} '{tool_name}' with interlock {interlock_id}...")
        if update_tool_interlock(tool_id, interlock_id, tool_name):
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
        'interlock_cards_downloaded': len(interlock_cards),
        'interlocks_downloaded': len(interlocks),
        'tools_downloaded': len(tools),
        'tools_matching': len(matching_tools),
        'successfully_updated': success_count,
        'skipped': skipped_count,
        'failed': failed_count
    }
    
    print(f"Interlock cards downloaded: {summary_data['interlock_cards_downloaded']}")
    print(f"Interlocks downloaded: {summary_data['interlocks_downloaded']}")
    print(f"Tools downloaded: {summary_data['tools_downloaded']}")
    print(f"Tools matching interlock card names: {summary_data['tools_matching']}")
    print(f"Successfully updated: {summary_data['successfully_updated']}")
    print(f"Skipped (already assigned): {summary_data['skipped']}")
    print(f"Failed updates: {summary_data['failed']}")
    print("=" * 60)
    
    logger.info(f"Interlock cards downloaded: {summary_data['interlock_cards_downloaded']}")
    logger.info(f"Interlocks downloaded: {summary_data['interlocks_downloaded']}")
    logger.info(f"Tools downloaded: {summary_data['tools_downloaded']}")
    logger.info(f"Tools matching interlock card names: {summary_data['tools_matching']}")
    logger.info(f"Successfully updated: {summary_data['successfully_updated']}")
    logger.info(f"Skipped (already assigned): {summary_data['skipped']}")
    logger.info(f"Failed updates: {summary_data['failed']}")
    logger.info("=" * 60)
    logger.info("Script completed")

if __name__ == "__main__":
    main()

