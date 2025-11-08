import csv
from pathlib import Path

def map_removed_accounts_to_ids(removed_file, nemo_file, output_file):
    """
    Map removed accounts to their IDs from nemo_accounts.csv and sort by ID.
    
    Args:
        removed_file: Path to the removed accounts CSV file
        nemo_file: Path to the nemo_accounts.csv file
        output_file: Path to output CSV file with IDs added
    """
    # Read nemo_accounts.csv and create a mapping from name to id
    name_to_id = {}
    with open(nemo_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row['name'].strip()
            account_id = row['id'].strip()
            # Store mapping (handle duplicates by keeping first occurrence)
            if name and name not in name_to_id:
                name_to_id[name] = account_id
    
    # Read removed accounts
    removed_accounts = []
    with open(removed_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for row in reader:
            if row and row[0].strip():
                removed_accounts.append(row[0].strip())
    
    # Map removed accounts to IDs
    mapped_accounts = []
    for account in removed_accounts:
        account_id = name_to_id.get(account, '')
        mapped_accounts.append({
            'id': account_id,
            'name': account
        })
    
    # Sort by ID (treat empty IDs as highest value, so they appear at the end)
    def sort_key(item):
        if item['id']:
            try:
                return (0, int(item['id']))  # Valid ID: sort by number
            except ValueError:
                return (1, item['id'])  # Non-numeric ID: sort alphabetically
        else:
            return (2, item['name'])  # No ID: sort by name at the end
    
    mapped_accounts.sort(key=sort_key)
    
    # Write to output file
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['ID', 'Removed Account'])  # Header
        for item in mapped_accounts:
            writer.writerow([item['id'], item['name']])
    
    # Count matches
    matched_count = sum(1 for item in mapped_accounts if item['id'])
    unmatched_count = len(mapped_accounts) - matched_count
    
    print(f"Mapped {matched_count} accounts to IDs")
    print(f"Unmatched accounts: {unmatched_count}")
    print(f"Results written to: {output_file}")
    
    return mapped_accounts

if __name__ == "__main__":
    # Set file paths
    removed_file = Path("removed_accounts.csv")
    nemo_file = Path("nemo_accounts.csv")
    output_file = Path("removed_accounts.csv")
    
    # Run mapping
    mapped = map_removed_accounts_to_ids(removed_file, nemo_file, output_file)

