#!/usr/bin/env python3
"""
Script to compare two qualification spreadsheets and identify new qualifications.
Compares 'Final SNL Qualified Users.xlsx' with 'SNL Qualified Users.xlsx'
to find qualifications that exist in the new file but not in the old file.
"""

import pandas as pd
import os
from typing import Dict, List, Set, Tuple, Optional, Any
from datetime import datetime

def find_column(df_columns: List[str], possible_names: List[str]) -> Optional[str]:
    """Find a column by checking multiple possible names (case-insensitive)."""
    df_columns_lower = [col.lower().strip() for col in df_columns]
    for name in possible_names:
        name_lower = name.lower().strip()
        if name_lower in df_columns_lower:
            idx = df_columns_lower.index(name_lower)
            return df_columns[idx]
    return None

def read_spreadsheet(filename: str) -> Tuple[pd.DataFrame, Optional[str], Optional[str]]:
    """Read spreadsheet and return DataFrame along with identified column names."""
    print(f"\nReading spreadsheet: {filename}")
    
    if not os.path.exists(filename):
        print(f"✗ File not found: {filename}")
        return None, None, None
    
    try:
        df = pd.read_excel(filename, parse_dates=False)
        print(f"✓ Read {len(df)} rows")
        print(f"  Columns: {', '.join(df.columns.tolist())}")
        
        # Find key columns
        equipment_col = find_column(df.columns.tolist(), ['equipment', 'tool', 'tool name', 'tool_name'])
        member_col = find_column(df.columns.tolist(), ['member', 'email', 'user_email', 'member_email'])
        
        if equipment_col:
            print(f"  Using '{equipment_col}' for tool names")
        else:
            print(f"  ⚠ Warning: Could not find equipment/tool column")
        
        if member_col:
            print(f"  Using '{member_col}' for user emails")
        else:
            print(f"  ⚠ Warning: Could not find member/email column")
        
        return df, equipment_col, member_col
        
    except Exception as e:
        print(f"✗ Error reading spreadsheet: {e}")
        return None, None, None

def normalize_value(value: Any) -> str:
    """Normalize a value for comparison (strip whitespace, convert to lowercase)."""
    if value is None or pd.isna(value):
        return ""
    return str(value).strip().lower()

def create_qualification_set(df: pd.DataFrame, equipment_col: str, member_col: str) -> Set[Tuple[str, str]]:
    """Create a set of (tool_name, email) tuples from the dataframe."""
    qualifications = set()
    
    if not equipment_col or not member_col:
        return qualifications
    
    for idx, row in df.iterrows():
        tool_name = normalize_value(row.get(equipment_col))
        email = normalize_value(row.get(member_col))
        
        # Skip rows with missing data
        if not tool_name or not email or '@' not in email:
            continue
        
        qualifications.add((tool_name, email))
    
    return qualifications

def compare_qualifications(old_file: str, new_file: str, output_file: Optional[str] = None):
    """Compare two qualification files and identify new qualifications."""
    print("=" * 70)
    print("Comparing Qualification Files")
    print("=" * 70)
    print(f"\nOld file: {old_file}")
    print(f"New file: {new_file}")
    print("=" * 70)
    
    # Read both files
    old_df, old_equipment_col, old_member_col = read_spreadsheet(old_file)
    new_df, new_equipment_col, new_member_col = read_spreadsheet(new_file)
    
    if old_df is None or new_df is None:
        print("\n✗ Cannot proceed - failed to read one or both files")
        return
    
    if not old_equipment_col or not old_member_col:
        print("\n✗ Cannot proceed - missing required columns in old file")
        return
    
    if not new_equipment_col or not new_member_col:
        print("\n✗ Cannot proceed - missing required columns in new file")
        return
    
    # Create sets of qualifications
    print("\n" + "-" * 70)
    print("Creating qualification sets...")
    print("-" * 70)
    
    old_qualifications = create_qualification_set(old_df, old_equipment_col, old_member_col)
    new_qualifications = create_qualification_set(new_df, new_equipment_col, new_member_col)
    
    print(f"Old file: {len(old_qualifications)} unique qualifications")
    print(f"New file: {len(new_qualifications)} unique qualifications")
    
    # Find new qualifications
    new_only = new_qualifications - old_qualifications
    removed = old_qualifications - new_qualifications
    
    print("\n" + "=" * 70)
    print("COMPARISON RESULTS")
    print("=" * 70)
    print(f"\nTotal in old file: {len(old_qualifications)}")
    print(f"Total in new file: {len(new_qualifications)}")
    print(f"\n✨ NEW qualifications: {len(new_only)}")
    print(f"❌ REMOVED qualifications: {len(removed)}")
    print(f"📊 Net change: +{len(new_only) - len(removed)}")
    
    # Show new qualifications
    if new_only:
        print("\n" + "-" * 70)
        print("NEW QUALIFICATIONS:")
        print("-" * 70)
        
        # Create a list of new qualification rows from the dataframe
        new_rows = []
        for idx, row in new_df.iterrows():
            tool_name = normalize_value(row.get(new_equipment_col))
            email = normalize_value(row.get(new_member_col))
            
            if (tool_name, email) in new_only:
                new_rows.append(row)
        
        # Show first 20 new qualifications
        print(f"\nShowing first 20 of {len(new_rows)} new qualifications:\n")
        for i, row in enumerate(new_rows[:20], 1):
            tool_name = str(row.get(new_equipment_col, '')).strip()
            email = str(row.get(new_member_col, '')).strip()
            print(f"  {i:3d}. Tool: '{tool_name}' → User: '{email}'")
        
        if len(new_rows) > 20:
            print(f"\n  ... and {len(new_rows) - 20} more")
        
        # Save new qualifications to file if requested
        if output_file:
            new_df_filtered = pd.DataFrame(new_rows)
            new_df_filtered.to_excel(output_file, index=False)
            print(f"\n✓ Saved {len(new_rows)} new qualifications to: {output_file}")
    
    # Show removed qualifications if any
    if removed:
        print("\n" + "-" * 70)
        print("REMOVED QUALIFICATIONS:")
        print("-" * 70)
        
        removed_list = list(removed)
        print(f"\nShowing first 20 of {len(removed_list)} removed qualifications:\n")
        for i, (tool_name, email) in enumerate(removed_list[:20], 1):
            print(f"  {i:3d}. Tool: '{tool_name}' → User: '{email}'")
        
        if len(removed_list) > 20:
            print(f"\n  ... and {len(removed_list) - 20} more")
    
    print("\n" + "=" * 70)

def main():
    """Main function."""
    old_file = "/Users/adenton/Desktop/NEMO-Merger/SNSF-Data/SNC Qualified Users.xlsx"
    new_file = "/Users/adenton/Desktop/NEMO-Merger/SNSF-Data/Final SNC Qualifed Users.xlsx"
    
    # Optional: save new qualifications to a file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"SNC_new_qualifications_{timestamp}.xlsx"
    
    compare_qualifications(old_file, new_file, output_file)

if __name__ == "__main__":
    main()

