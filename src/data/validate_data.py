def validate_cleaned_data(df):
    """
    Validation checks to ensure cleaning was successful.
    
    Checks:
    1. No missing values
    2. No duplicates
    3. Timestamp range valid (2024 only)
    4. All expected roles present
    5. Value ranges sensible
    """
    print("[5/6] Validating cleaned data...")
    
    issues = []
    
    # Check 1: No missing values
    missing_count = df.isnull().sum().sum()
    if missing_count == 0:
        print("  No missing values")
    else:
        issues.append(f"Missing values: {missing_count}")
        print(f"  Missing values found: {missing_count}")
    
    # Check 2: No duplicates
    dupe_count = df.duplicated().sum()
    if dupe_count == 0:
        print("  No duplicate rows")
    else:
        issues.append(f"Duplicates: {dupe_count}")
        print(f"  Duplicates found: {dupe_count}")
    
    # Check 3: Timestamp range
    if df['timestamp'].min().year == 2024 and df['timestamp'].max().year == 2024:
        print("  Timestamp range valid (2024)")
    else:
        issues.append("Timestamp range invalid")
        print(f"  Timestamp range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    # Check 4: All roles present
    expected_roles = {'DB_Admin', 'HR_Admin', 'Cloud_Admin', 'Security_Admin', 'DevOps_Engineer'}
    actual_roles = set(df['role'].unique())
    if expected_roles == actual_roles:
        print(f"  All 5 roles present")
    else:
        issues.append(f"Missing roles: {expected_roles - actual_roles}")
        print(f"  Roles mismatch: {actual_roles}")
    
    # Check 5: Value ranges
    valid_ranges = True
    
    if df['session_duration'].min() < 5 or df['session_duration'].max() > 180:
        issues.append("session_duration out of range [5, 180]")
        valid_ranges = False
    
    if df['access_volume'].min() < 1 or df['access_volume'].max() > 50:
        issues.append("access_volume out of range [1, 50]")
        valid_ranges = False
    
    if valid_ranges:
        print("  Value ranges valid")
    else:
        print("  Value range violations detected")
    
    # Check 6: All actions valid
    valid_actions = {'read', 'write', 'delete', 'export'}
    actual_actions = set(df['action'].unique())
    if actual_actions.issubset(valid_actions):
        print(f"  All action types valid")
    else:
        issues.append(f"Invalid actions: {actual_actions - valid_actions}")
        print(f"  Invalid actions: {actual_actions - valid_actions}")
    
    print()
    
    if issues:
        print("  VALIDATION ISSUES:")
        for issue in issues:
            print(f"    - {issue}")
        return False
    else:
        print("  All validation checks passed!")
        return True