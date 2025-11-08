import csv
from pathlib import Path

def compare_accounts(dirty_file, clean_file, output_file):
    """
    Compare two CSV files and find accounts that are in dirty_file but not in clean_file.
    
    Args:
        dirty_file: Path to the dirty accounts CSV file
        clean_file: Path to the clean accounts CSV file
        output_file: Path to output CSV file with removed accounts
    """
    # Read dirty accounts
    dirty_accounts = set()
    with open(dirty_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for row in reader:
            if row and row[0].strip():  # Skip empty rows
                dirty_accounts.add(row[0].strip())
    
    # Read clean accounts
    clean_accounts = set()
    with open(clean_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for row in reader:
            if row and row[0].strip():  # Skip empty rows
                clean_accounts.add(row[0].strip())
    
    # Find accounts in dirty but not in clean (removed accounts)
    removed_accounts = sorted(dirty_accounts - clean_accounts)
    
    # Write removed accounts to output file
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Removed Account'])  # Header
        for account in removed_accounts:
            writer.writerow([account])
    
    print(f"Found {len(removed_accounts)} removed accounts")
    print(f"Results written to: {output_file}")
    
    return removed_accounts

if __name__ == "__main__":
    # Set file paths
    dirty_file = Path("dirty-snsf-accounts.csv")
    clean_file = Path("snsf-clean-accounts.csv")
    output_file = Path("removed_accounts.csv")
    
    # Run comparison
    removed = compare_accounts(dirty_file, clean_file, output_file)
    
    # Print summary - need to recalculate counts
    with open(dirty_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        dirty_count = len(set(row[0].strip() for row in reader if row and row[0].strip()))
    
    with open(clean_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        clean_count = len(set(row[0].strip() for row in reader if row and row[0].strip()))
    
    print(f"\nSummary:")
    print(f"  Total unique accounts in dirty file: {dirty_count}")
    print(f"  Total unique accounts in clean file: {clean_count}")
    print(f"  Removed accounts: {len(removed)}")

