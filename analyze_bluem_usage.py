#!/usr/bin/env python3
"""
Script to analyze bluem tool usage from billing data.
Filters rows for bluem, sums total hours, and creates a detailed CSV report.
"""

import pandas as pd
from datetime import datetime
from typing import List, Dict, Any

# Input CSV file
INPUT_CSV = "/Users/adenton/Desktop/NEMO-Merger/billing_data_master_master.csv"
TOOL_NAME = "bluem"

def load_and_filter_data(csv_path: str, tool_name: str) -> pd.DataFrame:
    """Load CSV and filter for specific tool name."""
    print(f"Loading data from {csv_path}...")
    df = pd.read_csv(csv_path)
    print(f"✓ Loaded {len(df)} total rows")
    
    # Filter for the specific tool
    filtered_df = df[df['name'] == tool_name].copy()
    print(f"✓ Found {len(filtered_df)} rows for tool '{tool_name}'")
    
    return filtered_df

def calculate_total_hours(df: pd.DataFrame) -> float:
    """Calculate total hours used from unit_quantity column."""
    # unit_quantity contains hours for hourly billing
    total_hours = df['unit_quantity'].sum()
    return total_hours

def create_usage_report(df: pd.DataFrame, tool_name: str) -> pd.DataFrame:
    """Create a detailed usage report with useful columns."""
    # Select relevant columns for the report
    report_columns = [
        'item_id',
        'account',
        'account_id',
        'project',
        'project_id',
        'user',
        'user_fullname',
        'proxy_user',
        'proxy_user_fullname',
        'start',
        'end',
        'unit_quantity',  # Hours used
        'quantity',      # Duration in minutes
        'rate',
        'rate_category',
        'unit_rate',
        'amount',
        'discount_amount',
        'department',
        'application',
        'core_facility'
    ]
    
    # Only include columns that exist in the dataframe
    available_columns = [col for col in report_columns if col in df.columns]
    report_df = df[available_columns].copy()
    
    # Sort by start date (most recent first)
    if 'start' in report_df.columns:
        report_df['start'] = pd.to_datetime(report_df['start'], errors='coerce')
        report_df = report_df.sort_values('start', ascending=False)
    
    return report_df

def save_report(report_df: pd.DataFrame, tool_name: str) -> str:
    """Save the report to CSV with timestamp."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{tool_name}_usage_report_{timestamp}.csv"
    
    report_df.to_csv(filename, index=False)
    print(f"✓ Saved usage report to {filename}")
    return filename

def print_summary(df: pd.DataFrame, total_hours: float, tool_name: str):
    """Print summary statistics."""
    print("\n" + "=" * 60)
    print(f"USAGE SUMMARY FOR '{tool_name}'")
    print("=" * 60)
    print(f"Total usage events: {len(df)}")
    print(f"Total hours used: {total_hours:.2f} hours")
    print(f"Total hours used: {total_hours * 60:.2f} minutes")
    
    if 'amount' in df.columns:
        total_amount = df['amount'].sum()
        print(f"Total amount billed: ${total_amount:.2f}")
    
    # Unique users
    if 'user_fullname' in df.columns:
        unique_users = df['user_fullname'].nunique()
        print(f"Unique users: {unique_users}")
    
    # Unique accounts
    if 'account' in df.columns:
        unique_accounts = df['account'].nunique()
        print(f"Unique accounts: {unique_accounts}")
    
    # Unique projects
    if 'project_id' in df.columns:
        unique_projects = df['project_id'].nunique()
        print(f"Unique projects: {unique_projects}")
    
    print("=" * 60)

def main():
    """Main function."""
    print("=" * 60)
    print("BLUEM TOOL USAGE ANALYSIS")
    print("=" * 60)
    print()
    
    # Load and filter data
    filtered_df = load_and_filter_data(INPUT_CSV, TOOL_NAME)
    
    if len(filtered_df) == 0:
        print(f"✗ No data found for tool '{TOOL_NAME}'. Exiting.")
        return
    
    # Calculate total hours
    total_hours = calculate_total_hours(filtered_df)
    
    # Create detailed report
    print("\nCreating usage report...")
    report_df = create_usage_report(filtered_df, TOOL_NAME)
    
    # Save report
    report_filename = save_report(report_df, TOOL_NAME)
    
    # Print summary
    print_summary(filtered_df, total_hours, TOOL_NAME)
    
    print(f"\n✓ Analysis complete! Report saved to: {report_filename}")

if __name__ == "__main__":
    main()
