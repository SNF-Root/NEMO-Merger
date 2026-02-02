#!/usr/bin/env python3
"""
Script to find duplicate or similar-sounding department names in nemo_departments.json
"""

import json
from typing import List, Dict, Any, Tuple
from difflib import SequenceMatcher

def load_departments(filename: str = "nemo_departments.json") -> List[Dict[str, Any]]:
    """Load departments from JSON file."""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            departments = json.load(f)
        print(f"✓ Loaded {len(departments)} departments from {filename}")
        return departments
    except Exception as e:
        print(f"✗ Error loading departments: {e}")
        return []

def normalize_name(name: str) -> str:
    """Normalize a department name for comparison."""
    # Remove "Department" suffix
    normalized = name.replace(" Department", "").strip()
    # Normalize ampersands
    normalized = normalized.replace(" & ", " and ").replace("&", " and ")
    # Convert to lowercase for comparison
    return normalized.lower()

def find_similar_departments(departments: List[Dict[str, Any]], similarity_threshold: float = 0.85) -> List[Tuple[Dict[str, Any], Dict[str, Any], float]]:
    """Find pairs of departments with similar names."""
    similar_pairs = []
    
    for i, dept1 in enumerate(departments):
        name1 = dept1.get('name', '')
        normalized1 = normalize_name(name1)
        
        for j, dept2 in enumerate(departments[i+1:], start=i+1):
            name2 = dept2.get('name', '')
            normalized2 = normalize_name(name2)
            
            # Check if normalized names are identical (exact duplicates)
            if normalized1 == normalized2:
                similar_pairs.append((dept1, dept2, 1.0))
            else:
                # Calculate similarity ratio
                similarity = SequenceMatcher(None, normalized1, normalized2).ratio()
                if similarity >= similarity_threshold:
                    similar_pairs.append((dept1, dept2, similarity))
    
    return similar_pairs

def main():
    """Main function to find and display duplicate departments."""
    print("Finding duplicate or similar-sounding departments...")
    print("-" * 80)
    
    # Load departments
    departments = load_departments()
    
    if not departments:
        print("No departments loaded. Cannot proceed.")
        return
    
    # Find similar departments
    print("\nAnalyzing department names for duplicates...")
    similar_pairs = find_similar_departments(departments, similarity_threshold=0.85)
    
    if not similar_pairs:
        print("\n✓ No duplicate or similar departments found!")
        return
    
    # Group by exact matches vs similar matches
    exact_matches = [pair for pair in similar_pairs if pair[2] == 1.0]
    similar_matches = [pair for pair in similar_pairs if pair[2] < 1.0]
    
    print(f"\n{'='*80}")
    print("DUPLICATE DEPARTMENT ANALYSIS")
    print(f"{'='*80}")
    
    if exact_matches:
        print(f"\n📋 EXACT DUPLICATES (Normalized names are identical): {len(exact_matches)} pairs")
        print("-" * 80)
        for dept1, dept2, similarity in exact_matches:
            name1 = dept1.get('name', '')
            name2 = dept2.get('name', '')
            id1 = dept1.get('id', 'N/A')
            id2 = dept2.get('id', 'N/A')
            order1 = dept1.get('display_order', 'N/A')
            order2 = dept2.get('display_order', 'N/A')
            
            print(f"\n  Pair:")
            print(f"    ID {id1:3d} (order {order1:3d}): \"{name1}\"")
            print(f"    ID {id2:3d} (order {order2:3d}): \"{name2}\"")
            print(f"    → Normalized: \"{normalize_name(name1)}\"")
    
    if similar_matches:
        print(f"\n🔍 SIMILAR DEPARTMENTS (Similarity >= 85%): {len(similar_matches)} pairs")
        print("-" * 80)
        # Sort by similarity (highest first)
        similar_matches.sort(key=lambda x: x[2], reverse=True)
        
        for dept1, dept2, similarity in similar_matches:
            name1 = dept1.get('name', '')
            name2 = dept2.get('name', '')
            id1 = dept1.get('id', 'N/A')
            id2 = dept2.get('id', 'N/A')
            order1 = dept1.get('display_order', 'N/A')
            order2 = dept2.get('display_order', 'N/A')
            
            print(f"\n  Pair (similarity: {similarity:.1%}):")
            print(f"    ID {id1:3d} (order {order1:3d}): \"{name1}\"")
            print(f"    ID {id2:3d} (order {order2:3d}): \"{name2}\"")
            print(f"    → Normalized: \"{normalize_name(name1)}\" vs \"{normalize_name(name2)}\"")
    
    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"Total departments: {len(departments)}")
    print(f"Exact duplicates: {len(exact_matches)} pairs")
    print(f"Similar departments: {len(similar_matches)} pairs")
    print(f"Total similar pairs: {len(similar_pairs)}")
    
    # Create a list of departments to potentially delete (keep the one with lower ID, delete higher ID)
    if exact_matches or similar_matches:
        print(f"\n{'='*80}")
        print("RECOMMENDED DELETIONS (keeping lower ID, deleting higher ID):")
        print(f"{'='*80}")
        
        departments_to_delete = []
        for dept1, dept2, similarity in exact_matches + similar_matches:
            id1 = dept1.get('id', 999999)
            id2 = dept2.get('id', 999999)
            
            # Keep the one with lower ID (usually the original)
            if id1 < id2:
                keep_dept = dept1
                delete_dept = dept2
            else:
                keep_dept = dept2
                delete_dept = dept1
            
            departments_to_delete.append({
                'keep': keep_dept,
                'delete': delete_dept,
                'similarity': similarity
            })
        
        # Sort by ID to delete
        departments_to_delete.sort(key=lambda x: x['delete'].get('id', 999999))
        
        for item in departments_to_delete:
            delete_dept = item['delete']
            keep_dept = item['keep']
            similarity = item['similarity']
            
            delete_id = delete_dept.get('id', 'N/A')
            delete_name = delete_dept.get('name', 'N/A')
            keep_id = keep_dept.get('id', 'N/A')
            keep_name = keep_dept.get('name', 'N/A')
            
            match_type = "EXACT" if similarity == 1.0 else f"{similarity:.1%}"
            print(f"  DELETE ID {delete_id:3d}: \"{delete_name}\" (keep ID {keep_id:3d}: \"{keep_name}\") [{match_type}]")
        
        print(f"\nTotal departments to delete: {len(departments_to_delete)}")

if __name__ == "__main__":
    main()
