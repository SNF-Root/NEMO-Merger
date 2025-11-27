#!/usr/bin/env python3
"""
Script to compare PTAs from the SNSF Excel file with duplicate PTAs from the CSV.
Shows which PTAs overlap between the two sources.
"""

import pandas as pd
import csv
import os
from typing import Set, Dict, List, Any
from datetime import datetime

def read_ptas_from_excel(excel_path: str) -> Dict[str, List[Dict[str, Any]]]:
    """Read PTAs from the Excel file and return a dictionary mapping PTA -> row info."""
    try:
        print(f"Reading Excel file: {excel_path}...")
        df = pd.read_excel(excel_path)
        
        print(f"Found {len(df)} rows")
        print(f"Columns: {df.columns.tolist()}")
        
        # Find PTA column
        pta_col = None
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if col_lower == 'pta' or (col_lower.startswith('pta') and 'other' not in col_lower):
                pta_col = col
                break
        
        if not pta_col:
            print("✗ Error: Could not find PTA column in Excel file")
            return {}
        
        print(f"Using PTA column: {pta_col}")
        
        # Extract PTAs
        pta_to_rows = {}
        for idx, row in df.iterrows():
            if pd.notna(row[pta_col]):
                pta = str(row[pta_col]).strip().upper()
                if pta and pta not in ['NAN', 'NONE', '']:
                    if pta not in pta_to_rows:
                        pta_to_rows[pta] = []
                    
                    # Store row information
                    row_info = {
                        'row_number': idx + 2,  # +2 because Excel is 1-indexed and has header
                        'pta': pta,
                        'pta_name': str(row.get('PTA Name', 'N/A')).strip() if 'PTA Name' in df.columns else 'N/A',
                        'account': str(row.get('Account', 'N/A')).strip() if 'Account' in df.columns else 'N/A',
                        'project_type': str(row.get('project_type', 'N/A')).strip() if 'project_type' in df.columns else 'N/A',
                    }
                    pta_to_rows[pta].append(row_info)
        
        print(f"✓ Found {len(pta_to_rows)} unique PTAs in Excel file")
        return pta_to_rows
        
    except Exception as e:
        print(f"✗ Error reading Excel file: {e}")
        import traceback
        traceback.print_exc()
        return {}

def read_ptas_from_csv(csv_path: str) -> Dict[str, Dict[str, Any]]:
    """Read PTAs from the duplicate PTAs CSV file."""
    try:
        print(f"\nReading CSV file: {csv_path}...")
        
        pta_to_info = {}
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                pta = row.get('PTA', '').strip()
                if pta and pta.upper() not in ['NAN', 'NONE', '']:
                    pta_upper = pta.upper()
                    num_projects = row.get('Number of Projects', 'N/A')
                    mapped_id = row.get('Mapped Project ID (pta_lookup.json)', 'N/A')
                    
                    # Extract all project IDs
                    project_ids = []
                    project_names = []
                    i = 1
                    while True:
                        proj_id_col = f'Project ID {i}'
                        proj_name_col = f'Project Name {i}'
                        
                        if proj_id_col not in row or not row[proj_id_col] or row[proj_id_col] == '':
                            break
                        
                        project_ids.append(row[proj_id_col])
                        project_names.append(row.get(proj_name_col, 'N/A'))
                        i += 1
                    
                    pta_to_info[pta_upper] = {
                        'pta': pta_upper,
                        'num_projects': num_projects,
                        'mapped_project_id': mapped_id,
                        'project_ids': project_ids,
                        'project_names': project_names
                    }
        
        print(f"✓ Found {len(pta_to_info)} PTAs in CSV file")
        return pta_to_info
        
    except Exception as e:
        print(f"✗ Error reading CSV file: {e}")
        import traceback
        traceback.print_exc()
        return {}

def find_overlaps(
    excel_ptas: Dict[str, List[Dict[str, Any]]],
    csv_ptas: Dict[str, Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    """Find PTAs that appear in both sources."""
    overlaps = {}
    
    excel_pta_set = set(excel_ptas.keys())
    csv_pta_set = set(csv_ptas.keys())
    
    overlapping_ptas = excel_pta_set.intersection(csv_pta_set)
    
    for pta in overlapping_ptas:
        overlaps[pta] = {
            'excel_info': excel_ptas[pta],
            'csv_info': csv_ptas[pta]
        }
    
    return overlaps

def export_overlaps_to_csv(overlaps: Dict[str, Dict[str, Any]], output_path: str = None) -> str:
    """Export overlapping PTAs to a CSV file."""
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"pta_overlaps_{timestamp}.csv"
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'PTA',
            'In Excel File',
            'Excel Row Numbers',
            'Excel PTA Names',
            'Excel Accounts',
            'In Duplicate CSV',
            'Number of Duplicate Projects',
            'Mapped Project ID',
            'Duplicate Project IDs',
            'Duplicate Project Names'
        ]
        
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for pta in sorted(overlaps.keys()):
            excel_info_list = overlaps[pta]['excel_info']
            csv_info = overlaps[pta]['csv_info']
            
            # Combine Excel info
            excel_rows = [str(info['row_number']) for info in excel_info_list]
            excel_names = [info['pta_name'] for info in excel_info_list]
            excel_accounts = [info['account'] for info in excel_info_list]
            
            row = {
                'PTA': pta,
                'In Excel File': 'Yes',
                'Excel Row Numbers': '; '.join(excel_rows),
                'Excel PTA Names': '; '.join(excel_names),
                'Excel Accounts': '; '.join(excel_accounts),
                'In Duplicate CSV': 'Yes',
                'Number of Duplicate Projects': csv_info['num_projects'],
                'Mapped Project ID': csv_info['mapped_project_id'],
                'Duplicate Project IDs': '; '.join(csv_info['project_ids']),
                'Duplicate Project Names': '; '.join(csv_info['project_names'])
            }
            
            writer.writerow(row)
    
    return output_path

def main():
    """Main function to compare PTAs."""
    print("=" * 60)
    print("COMPARING PTAs FROM EXCEL AND DUPLICATE CSV")
    print("=" * 60)
    print()
    
    # File paths
    excel_path = "/Users/adenton/Desktop/NEMO-Merger/SNSF-Data/Copy of SNSF PTAs for Alex Denton.xlsx"
    csv_path = "/Users/adenton/Desktop/NEMO-Merger/duplicate_ptas_20251126_084800.csv"
    
    # Check if files exist
    if not os.path.exists(excel_path):
        print(f"✗ Error: Excel file not found: {excel_path}")
        return
    
    if not os.path.exists(csv_path):
        print(f"✗ Error: CSV file not found: {csv_path}")
        return
    
    # Read PTAs from both sources
    excel_ptas = read_ptas_from_excel(excel_path)
    csv_ptas = read_ptas_from_csv(csv_path)
    
    if not excel_ptas:
        print("✗ No PTAs found in Excel file")
        return
    
    if not csv_ptas:
        print("✗ No PTAs found in CSV file")
        return
    
    # Find overlaps
    print("\n" + "=" * 60)
    print("FINDING OVERLAPS")
    print("=" * 60)
    
    overlaps = find_overlaps(excel_ptas, csv_ptas)
    
    # Statistics
    excel_only = set(excel_ptas.keys()) - set(csv_ptas.keys())
    csv_only = set(csv_ptas.keys()) - set(excel_ptas.keys())
    
    print(f"\nStatistics:")
    print(f"  PTAs in Excel file: {len(excel_ptas)}")
    print(f"  PTAs in CSV file: {len(csv_ptas)}")
    print(f"  Overlapping PTAs: {len(overlaps)}")
    print(f"  PTAs only in Excel: {len(excel_only)}")
    print(f"  PTAs only in CSV: {len(csv_only)}")
    
    if overlaps:
        print(f"\n⚠ WARNING: Found {len(overlaps)} PTA(s) that appear in BOTH sources!")
        print("These PTAs are in your Excel file AND have duplicate projects in NEMO.")
        print("\nOverlapping PTAs:")
        for pta in sorted(overlaps.keys()):
            excel_info = overlaps[pta]['excel_info']
            csv_info = overlaps[pta]['csv_info']
            print(f"\n  {pta}:")
            print(f"    Excel: Appears in {len(excel_info)} row(s) - {', '.join([str(info['row_number']) for info in excel_info])}")
            print(f"    CSV: Has {csv_info['num_projects']} duplicate project(s)")
            print(f"    Mapped Project ID: {csv_info['mapped_project_id']}")
            print(f"    Duplicate Project IDs: {', '.join(csv_info['project_ids'])}")
        
        # Export to CSV
        print("\n" + "=" * 60)
        print("EXPORTING OVERLAPS TO CSV")
        print("=" * 60)
        output_file = export_overlaps_to_csv(overlaps)
        print(f"✓ Exported overlapping PTAs to: {output_file}")
    else:
        print("\n✓ No overlapping PTAs found.")
        print("All PTAs in the Excel file are unique in NEMO (no duplicates).")
    
    # Show some examples of Excel-only and CSV-only PTAs
    if excel_only:
        print(f"\nSample PTAs only in Excel (showing first 10):")
        for pta in sorted(list(excel_only))[:10]:
            print(f"  - {pta}")
        if len(excel_only) > 10:
            print(f"  ... and {len(excel_only) - 10} more")
    
    if csv_only:
        print(f"\nSample PTAs only in CSV (showing first 10):")
        for pta in sorted(list(csv_only))[:10]:
            print(f"  - {pta}")
        if len(csv_only) > 10:
            print(f"  ... and {len(csv_only) - 10} more")
    
    print("\n" + "=" * 60)
    print("ANALYSIS COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()

