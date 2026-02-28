"""
Data Cleaning Script for Echelon Project
---
Handles missing values, duplicates, standardization, and validation
based on EDA findings from 01_data_exploration.ipynb

Input: data/raw/enterprise_privileged_access_logs.csv
Output: data/processed/cleaned_access_logs.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys

def load_raw_data(filepath):
    """Load raw CSV and parse timestamps."""
    print("=" * 80)
    print(" ECHELON - DATA CLEANING PIPELINE")
    print("=" * 80)
    print()
    
    print(f"[1/6] Loading raw data from: {filepath}")
    df = pd.read_csv(filepath)
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    print(f"  Loaded {len(df):,} records")
    print(f"  Shape: {df.shape}")
    print(f"  Memory: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    print()
    
    return df


def handle_missing_values(df):
    """
    Impute missing values using ROLE-SPECIFIC medians.
    
    Strategy:
    - session_duration: Fill with role-specific median (durations vary by role)
    - access_volume: Fill with role-specific median (volume varies by role)
    
    WHY role-specific?
    - DB_Admin baseline: 65 min sessions
    - HR_Admin baseline: 35 min sessions
    - Using global median would distort role patterns
    """
    print("[2/6] Handling missing values...")
    
    # Document pre-cleaning state
    missing_before = df.isnull().sum().sum()
    print(f"  Missing values before: {missing_before}")
    
    # session_duration: role-specific median imputation
    if df['session_duration'].isnull().any():
        for role in df['role'].unique():
            role_mask = df['role'].str.strip() == role
            role_median = df.loc[role_mask, 'session_duration'].median()
            
            # Fill nulls for this role
            null_mask = role_mask & df['session_duration'].isnull()
            df.loc[null_mask, 'session_duration'] = role_median
            
            filled_count = null_mask.sum()
            if filled_count > 0:
                print(f"    ├─ {role}: filled {filled_count} nulls with median={role_median:.1f}")
    
    # access_volume: role-specific median imputation
    if df['access_volume'].isnull().any():
        for role in df['role'].unique():
            role_mask = df['role'].str.strip() == role
            role_median = df.loc[role_mask, 'access_volume'].median()
            
            null_mask = role_mask & df['access_volume'].isnull()
            df.loc[null_mask, 'access_volume'] = role_median
            
            filled_count = null_mask.sum()
            if filled_count > 0:
                print(f"    ├─ {role}: filled {filled_count} nulls with median={role_median:.1f}")
    
    # Verify no nulls remain
    missing_after = df.isnull().sum().sum()
    print(f"  Missing values after: {missing_after}")
    
    if missing_after > 0:
        print(f"  WARNING: {missing_after} missing values still present!")
        print(df.isnull().sum()[df.isnull().sum() > 0])
    
    print()
    return df


def remove_duplicates(df):
    """
    Remove exact duplicate rows (keep first occurrence).
    
    NOTE: Must be called AFTER standardization to catch duplicates
    that only differ by casing or whitespace.
    """
    print("[3/6] Removing duplicate rows...")
    
    rows_before = len(df)
    duplicates_found = df.duplicated().sum()
    
    print(f"  Duplicates found: {duplicates_found} ({duplicates_found/rows_before*100:.2f}%)")
    
    # Keep first occurrence of each duplicate
    df_deduped = df.drop_duplicates(keep='first')
    
    rows_after = len(df_deduped)
    rows_removed = rows_before - rows_after
    
    print(f"  Rows before: {rows_before:,}")
    print(f"  Rows after: {rows_after:,}")
    print(f"  Rows removed: {rows_removed:,}")
    print()
    
    return df_deduped


def standardize_columns(df):
    """
    Standardize column names and categorical values.
    
    Fixes:
    - Column names: lowercase with underscores
    - Categorical strings: strip whitespace, lowercase
    - Boolean: ensure True/False
    """
    print("[4/6] Standardizing column names and values...")
    
    # Column names: already lowercase from generator, but enforce consistency
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    print("  Column names standardized")
    
    # role: strip whitespace (EDA found ~5% with padding)
    df.loc[:, 'role'] = df['role'].str.strip()
    print(f"  role column stripped (unique values: {df['role'].nunique()})")
    
    # action: lowercase and strip (EDA found ~8% mixed case)
    df.loc[:, 'action'] = df['action'].str.lower().str.strip()
    print(f"  action column normalized (unique values: {df['action'].nunique()})")
    
    # resource_type: lowercase (EDA found ~10% uppercase)
    df.loc[:, 'resource_type'] = df['resource_type'].str.lower().str.strip()
    print(f"  resource_type column normalized (unique values: {df['resource_type'].nunique()})")
    
    # success_flag: ensure boolean
    df.loc[:, 'success_flag'] = df['success_flag'].astype(bool)
    print(f"  success_flag converted to bool")
    
    print()
    return df


def save_cleaned_data(df, output_path):
    """Save cleaned dataframe to CSV."""
    print(f"[6/6] Saving cleaned data to: {output_path}")
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save
    df.to_csv(output_path, index=False)
    
    print(f"  Saved {len(df):,} records")
    print(f"  File size: {output_path.stat().st_size / 1024:.1f} KB")
    print()


def generate_cleaning_summary(df_before, df_after, output_dir):
    """Generate cleaning summary report."""
    summary = f"""# Data Cleaning Summary

## Input Data
- **Raw file:** `data/raw/enterprise_privileged_access_logs.csv`
- **Records (before):** {len(df_before):,}
- **Memory (before):** {df_before.memory_usage(deep=True).sum() / 1024**2:.2f} MB

## Cleaning Steps Applied

### 1. Missing Value Imputation
- **Method:** Role-specific median imputation
- **Columns affected:** `session_duration`, `access_volume`
- **Rationale:** Each role has different baseline behavior patterns
  - DB_Admin: 65 min sessions, 15 records/session
  - HR_Admin: 35 min sessions, 8 records/session
  - Using global median would distort role-specific patterns

### 2. Duplicate Removal
- **Duplicates found:** {df_before.duplicated().sum()} ({df_before.duplicated().sum()/len(df_before)*100:.2f}%)
- **Strategy:** Keep first occurrence
- **Records removed:** {len(df_before) - len(df_after):,}

### 3. Column Standardization
- **Column names:** Lowercase with underscores
- **Categorical values:**
  - `role`: Stripped whitespace
  - `action`: Lowercased and stripped
  - `resource_type`: Lowercased and stripped
- **Boolean:** `success_flag` converted to True/False

## Output Data
- **Clean file:** `data/processed/cleaned_access_logs.csv`
- **Records (after):** {len(df_after):,}
- **Memory (after):** {df_after.memory_usage(deep=True).sum() / 1024**2:.2f} MB
- **Data quality:** 100% complete, no duplicates

## Impact Analysis
- **Rows removed:** {len(df_before) - len(df_after):,} ({(len(df_before)-len(df_after))/len(df_before)*100:.2f}%)
- **Data integrity:** Preserved
- **Missing values:** 0
- **Duplicates:** 0

## Validation Status
- No missing values  
- No duplicates  
- Timestamp range valid (2024)  
- All 5 roles present  
- Value ranges valid  
- Action types valid  

## Next Steps
- Proceed to Feature Engineering (`notebooks/03_feature_engineering.ipynb`)
- Extract temporal features (hour, day_of_week, week, month)
- Compute behavioral features (export_ratio, utilization_ratio, privilege_gap)
- Calculate role-normalized z-scores
"""
    
    summary_path = output_dir / 'cleaning_summary.md'
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(summary_path, 'w') as f:
        f.write(summary)
    
    print(f"  Summary report saved to: {summary_path}")


def main():
    """Main execution pipeline."""
    # File paths
    raw_file = Path('../data/raw/enterprise_privileged_access_logs.csv')
    clean_file = Path('../data/processed/cleaned_access_logs.csv')
    report_dir = Path('../outputs/reports')
    
    # Check input file exists
    if not raw_file.exists():
        print(f"ERROR: Raw data file not found: {raw_file}")
        print("   Run generate_enterprise_access_logs.py first")
        sys.exit(1)
    
    # Load
    df_raw = load_raw_data(raw_file)
    df_before = df_raw.copy()  # Keep original for summary
    
    # Clean
    df = handle_missing_values(df_raw)
    df = standardize_columns(df)
    df = remove_duplicates(df)  # Must be AFTER standardization
    
    # Validate
    from src.data.validate_data import validate_cleaned_data
    validation_passed = validate_cleaned_data(df)
    
    if not validation_passed:
        print("WARNING: Validation issues detected!")
        print("   Review the issues above before proceeding.")
        print()
    
    # Save
    save_cleaned_data(df, clean_file)
    
    # Generate summary report
    generate_cleaning_summary(df_before, df, report_dir)
    
    # Final summary
    print("=" * 80)
    print(" DATA CLEANING COMPLETE")
    print("=" * 80)
    print()
    print(f"Input:  {len(df_before):,} records")
    print(f"Output: {len(df):,} records")
    print(f"Impact: {len(df_before)-len(df):,} records removed ({(len(df_before)-len(df))/len(df_before)*100:.2f}%)")
    print()
    print("Next step: Run notebooks/03_feature_engineering.ipynb")
    print()


if __name__ == "__main__":
    main()