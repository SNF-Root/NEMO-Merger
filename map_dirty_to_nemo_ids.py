#!/usr/bin/env python3
"""
Script to map dirty SNSF accounts to NEMO account IDs.
Creates a new CSV with dirty account names and their corresponding NEMO IDs,
sorted by ID in ascending order.
"""

import csv
from pathlib import Path

def read_dirty_accounts(file_path):
    """Read dirty accounts from CSV file."""
    dirty_accounts = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        # Skip header
        next(reader, None)
        # Skip empty line
        next(reader, None)
        
        for row in reader:
            # Handle different row formats:
            # - Two columns: ['Last', ' First'] -> join as "Last, First"
            # - One column: ['Company Name'] -> use as is
            if len(row) >= 2 and row[1].strip():
                # Two columns: join with comma
                account_name = f"{row[0].strip()}, {row[1].strip()}"
            elif len(row) >= 1 and row[0].strip():
                # Single column
                account_name = row[0].strip()
            else:
                continue
            
            if account_name:
                dirty_accounts.append(account_name)
    
    return dirty_accounts

def read_nemo_accounts(file_path):
    """Read NEMO accounts and create a lookup dictionary."""
    nemo_lookup = {}
    nemo_accounts = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            account_name = row['name'].strip()
            account_id = row['id'].strip()
            nemo_accounts.append((account_name, account_id))
            # Store exact match (case-sensitive)
            nemo_lookup[account_name] = account_id
            # Store case-insensitive match
            nemo_lookup[account_name.lower()] = account_id
    
    return nemo_lookup, nemo_accounts

def map_dirty_to_nemo(dirty_accounts, nemo_lookup, nemo_accounts):
    """Map dirty accounts to NEMO IDs."""
    mapped_results = []
    
    for dirty_account in dirty_accounts:
        nemo_id = None
        
        # Try exact match first (case-sensitive)
        nemo_id = nemo_lookup.get(dirty_account)
        
        # If no exact match, try case-insensitive
        if nemo_id is None:
            nemo_id = nemo_lookup.get(dirty_account.lower())
        
        # If still no match, try fuzzy matching (normalize whitespace)
        if nemo_id is None:
            dirty_normalized = ' '.join(dirty_account.split())
            for nemo_name, nid in nemo_accounts:
                nemo_normalized = ' '.join(nemo_name.split())
                if dirty_normalized.lower() == nemo_normalized.lower():
                    nemo_id = nid
                    break
        
        mapped_results.append({
            'dirty_account': dirty_account,
            'nemo_id': nemo_id if nemo_id else 'NOT_FOUND'
        })
    
    return mapped_results

def write_mapped_csv(results, output_path):
    """Write mapped results to CSV, sorted by NEMO ID."""
    # Filter out NOT_FOUND entries and convert IDs to integers for sorting
    valid_results = []
    not_found_results = []
    
    for result in results:
        if result['nemo_id'] == 'NOT_FOUND':
            not_found_results.append(result)
        else:
            try:
                result['nemo_id_int'] = int(result['nemo_id'])
                valid_results.append(result)
            except ValueError:
                not_found_results.append(result)
    
    # Sort valid results by ID
    valid_results.sort(key=lambda x: x['nemo_id_int'])
    
    # Combine: valid results first, then not found
    all_results = valid_results + not_found_results
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['dirty_account', 'nemo_id'])
        writer.writeheader()
        
        for result in all_results:
            writer.writerow({
                'dirty_account': result['dirty_account'],
                'nemo_id': result['nemo_id']
            })

def main():
    # File paths
    base_dir = Path(__file__).parent
    dirty_file = base_dir / 'dirty-snsf-accounts.csv'
    nemo_file = base_dir / 'nemo_accounts.csv'
    output_file = base_dir / 'dirty_to_nemo_mapping.csv'
    
    print("Reading dirty accounts...")
    dirty_accounts = read_dirty_accounts(dirty_file)
    print(f"Found {len(dirty_accounts)} dirty accounts")
    
    print("Reading NEMO accounts...")
    nemo_lookup, nemo_accounts = read_nemo_accounts(nemo_file)
    print(f"Found {len(nemo_accounts)} NEMO accounts")
    
    print("Mapping dirty accounts to NEMO IDs...")
    mapped_results = map_dirty_to_nemo(dirty_accounts, nemo_lookup, nemo_accounts)
    
    # Count matches
    found_count = sum(1 for r in mapped_results if r['nemo_id'] != 'NOT_FOUND')
    not_found_count = len(mapped_results) - found_count
    
    print(f"Matched: {found_count}")
    print(f"Not found: {not_found_count}")
    
    print(f"Writing results to {output_file}...")
    write_mapped_csv(mapped_results, output_file)
    
    print("Done!")
    
    # Print some examples of not found accounts
    if not_found_count > 0:
        print("\nFirst 10 accounts that were not found:")
        not_found = [r for r in mapped_results if r['nemo_id'] == 'NOT_FOUND']
        for i, result in enumerate(not_found[:10], 1):
            print(f"  {i}. {result['dirty_account']}")

if __name__ == '__main__':
    main()

