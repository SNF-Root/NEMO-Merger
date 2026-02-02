#!/usr/bin/env python3
"""
Script to create a pie chart showing bluem tool usage by account.
"""

import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import os
import glob

# Tool name
TOOL_NAME = "bluem"

def find_latest_report(tool_name: str) -> str:
    """Find the most recent usage report CSV for the tool."""
    pattern = f"{tool_name}_usage_report_*.csv"
    files = glob.glob(pattern)
    if not files:
        return None
    # Sort by modification time and return the most recent
    latest = max(files, key=os.path.getmtime)
    return latest

def load_data(csv_path: str) -> pd.DataFrame:
    """Load the usage report CSV."""
    print(f"Loading data from {csv_path}...")
    df = pd.read_csv(csv_path, low_memory=False)
    print(f"✓ Loaded {len(df)} rows")
    return df

def aggregate_by_account(df: pd.DataFrame) -> pd.DataFrame:
    """Group by account and sum hours used."""
    print("\nAggregating usage by account...")
    
    # Group by account and sum unit_quantity (hours)
    account_usage = df.groupby('account').agg({
        'unit_quantity': 'sum',  # Total hours
        'amount': 'sum',  # Total amount billed
        'item_id': 'count'  # Number of usage events
    }).reset_index()
    
    # Rename columns for clarity
    account_usage.columns = ['account', 'total_hours', 'total_amount', 'usage_count']
    
    # Sort by total hours (descending)
    account_usage = account_usage.sort_values('total_hours', ascending=False)
    
    print(f"✓ Found {len(account_usage)} unique accounts")
    return account_usage

def create_pie_chart(account_usage: pd.DataFrame, tool_name: str, output_filename: str):
    """Create a pie chart showing usage by account."""
    print(f"\nCreating pie chart...")
    
    # Prepare data
    accounts = account_usage['account'].tolist()
    hours = account_usage['total_hours'].tolist()
    
    # For readability, we might want to combine smaller accounts into "Others"
    # Let's show top 10 accounts individually, and group the rest
    top_n = 10
    if len(account_usage) > top_n:
        top_accounts = account_usage.head(top_n)
        other_hours = account_usage.tail(len(account_usage) - top_n)['total_hours'].sum()
        other_count = account_usage.tail(len(account_usage) - top_n)['usage_count'].sum()
        
        # Combine data
        plot_accounts = top_accounts['account'].tolist() + ['Others']
        plot_hours = top_accounts['total_hours'].tolist() + [other_hours]
        
        # Create labels with hours and percentage
        labels = []
        for i, account in enumerate(plot_accounts):
            hours_val = plot_hours[i]
            pct = (hours_val / sum(plot_hours)) * 100
            if account == 'Others':
                labels.append(f'{account}\n({hours_val:.1f}h, {pct:.1f}%)')
            else:
                labels.append(f'{account}\n({hours_val:.1f}h)')
    else:
        plot_accounts = accounts
        plot_hours = hours
        labels = [f'{acc}\n({hrs:.1f}h)' for acc, hrs in zip(plot_accounts, plot_hours)]
    
    # Create the pie chart
    fig, ax = plt.subplots(figsize=(14, 10))
    
    # Create pie chart with custom colors
    colors = plt.cm.Set3(range(len(plot_hours)))
    wedges, texts, autotexts = ax.pie(
        plot_hours,
        labels=labels,
        autopct='%1.1f%%',
        startangle=90,
        colors=colors,
        textprops={'fontsize': 9}
    )
    
    # Improve text appearance
    for autotext in autotexts:
        autotext.set_color('black')
        autotext.set_fontweight('bold')
        autotext.set_fontsize(8)
    
    # Add title
    total_hours = sum(plot_hours)
    ax.set_title(
        f'{tool_name} Tool Usage by Account\nTotal: {total_hours:.1f} hours across {len(account_usage)} accounts',
        fontsize=16,
        fontweight='bold',
        pad=20
    )
    
    # Ensure pie is circular
    ax.axis('equal')
    
    # Save the chart
    plt.tight_layout()
    plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    print(f"✓ Saved pie chart to {output_filename}")
    
    plt.close()

def print_top_accounts(account_usage: pd.DataFrame, n: int = 10):
    """Print top N accounts by usage."""
    print(f"\nTop {n} accounts by hours used:")
    print("-" * 80)
    top_n = account_usage.head(n)
    for idx, row in top_n.iterrows():
        print(f"{row['account']:40s} {row['total_hours']:8.2f} hours  ({row['usage_count']:4d} sessions)  ${row['total_amount']:8.2f}")

def main():
    """Main function."""
    print("=" * 60)
    print("BLUEM USAGE PIE CHART GENERATOR")
    print("=" * 60)
    
    # Find the latest usage report
    input_csv = find_latest_report(TOOL_NAME)
    if not input_csv:
        print(f"✗ Error: No usage report found for '{TOOL_NAME}'.")
        print(f"   Please run analyze_bluem_usage.py first to generate the report.")
        return
    
    print(f"Using report: {input_csv}")
    
    # Load data
    df = load_data(input_csv)
    
    # Aggregate by account
    account_usage = aggregate_by_account(df)
    
    # Print top accounts
    print_top_accounts(account_usage)
    
    # Create pie chart
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_filename = f"{TOOL_NAME}_usage_pie_chart_{timestamp}.png"
    create_pie_chart(account_usage, TOOL_NAME, output_filename)
    
    print("\n" + "=" * 60)
    print(f"✓ Chart generation complete! Saved to: {output_filename}")
    print("=" * 60)

if __name__ == "__main__":
    main()
