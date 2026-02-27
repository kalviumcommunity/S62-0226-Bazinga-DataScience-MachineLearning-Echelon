"""
Data Validation Script
---
Validates enterprise_privileged_access_logs.csv against requirements

VALIDATION CHECKS:
1. Volume checks (12,000-15,000 records, 100 users)
2. Role distribution (5 roles, balanced)
3. Temporal coverage (full year 2024)
4. Data quality issues present (as expected)
5. Behavioral patterns detectable (exports, night access, etc.)
6. Value ranges (session duration 5-240, access volume 1-50)
7. Column schema (including assigned_resource_count, actively_used_resource_count)
8. Privilege-usage misalignment detectable

Run after: python generate_access_logs.py
"""

import pandas as pd
import numpy as np
from datetime import datetime

def validate_data():
    print("=" * 80)
    print(" ECHELON - DATA VALIDATION")
    print("=" * 80)
    print()

    # Load data
    print("Loading data...")
    try:
        df = pd.read_csv('../data/raw/enterprise_privileged_access_logs.csv')
        print(f"   Loaded {len(df):,} records")
        print()
    except FileNotFoundError:
        print("   ERROR: File not found!")
        print("   Run: python generate_access_logs.py first")
        return

    # Parse timestamp
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    print("VALIDATION CHECKS:")
    print()

    # Check 1: Volume
    print("1. VOLUME CHECKS:")
    total_records = len(df)
    if 12000 <= total_records <= 15000:
        print(f"   Total records: {total_records:,} (within 12,000-15,000)")
    else:
        print(f"   Total records: {total_records:,} (expected 12,000-15,000)")

    unique_users = df['user_id'].nunique()
    if unique_users == 100:
        print(f"   Unique users: {unique_users}")
    else:
        print(f"   Unique users: {unique_users} (expected 100)")

    avg_logs_per_user = total_records / unique_users
    if 100 <= avg_logs_per_user <= 180:
        print(f"   Avg logs per user: {avg_logs_per_user:.1f}")
    else:
        print(f"   Avg logs per user: {avg_logs_per_user:.1f} (expected 100-180)")
    print()

    # Check 2: Role distribution
    print("2. ROLE DISTRIBUTION:")
    role_counts = df['role'].str.strip().value_counts()
    expected_roles = ['DB_Admin', 'HR_Admin', 'Cloud_Admin', 'Security_Admin', 'DevOps_Engineer']

    for role in expected_roles:
        if role in role_counts.index:
            count = role_counts[role]
            if 1800 <= count <= 3600:
                print(f"   {role}: {count:,} records")
            else:
                print(f"   {role}: {count:,} records (expected 1,800-3,600)")
        else:
            print(f"   {role}: MISSING")

    users_per_role = df.groupby(df['role'].str.strip())['user_id'].nunique()
    print()
    print("   Users per role:")
    for role in expected_roles:
        if role in users_per_role.index:
            count = users_per_role[role]
            if count == 20:
                print(f"    {role}: {count} users")
            else:
                print(f"     {role}: {count} users (expected 20)")
    print()

    # Check 3: Temporal coverage
    print("3. TEMPORAL COVERAGE:")
    min_date = df['timestamp'].min()
    max_date = df['timestamp'].max()

    if min_date.year == 2024:
        print(f"   Start date: {min_date.date()} (2024)")
    else:
        print(f"   Start date: {min_date.date()} (expected 2024)")

    if max_date.year == 2024:
        print(f"   End date: {max_date.date()} (2024)")
    else:
        print(f"   End date: {max_date.date()} (expected 2024)")

    date_range_days = (max_date - min_date).days
    if date_range_days >= 350:
        print(f"   Date range: {date_range_days} days (covers full year)")
    else:
        print(f"   Date range: {date_range_days} days (expected ~365)")
    print()

    # Check 4: Data quality issues present
    print("4. DATA QUALITY ISSUES (Expected):")

    missing_total = df.isnull().sum().sum()
    if missing_total > 0:
        print(f"   Missing values present: {missing_total} cells")
        for col in df.columns:
            missing = df[col].isnull().sum()
            if missing > 0:
                print(f"     ├─ {col}: {missing} ({missing/len(df)*100:.2f}%)")
    else:
        print(f"   Missing values: {missing_total} (expected ~3%)")

    duplicates = df.duplicated().sum()
    if duplicates > 0:
        print(f"   Duplicate rows present: {duplicates} ({duplicates/len(df)*100:.2f}%)")
    else:
        print(f"   Duplicate rows: {duplicates} (expected ~2%)")

    uppercase_resources = df['resource_type'].str.isupper().sum()
    if uppercase_resources > 0:
        print(f"   Uppercase resources: {uppercase_resources} ({uppercase_resources/len(df)*100:.2f}%)")
    else:
        print(f"   Uppercase resources: {uppercase_resources} (expected ~10%)")

    whitespace_roles = df['role'].str.contains(r'^\s+|\s+$', regex=True, na=False).sum()
    if whitespace_roles > 0:
        print(f"   Role whitespace: {whitespace_roles} ({whitespace_roles/len(df)*100:.2f}%)")
    else:
        print(f"   Role whitespace: {whitespace_roles} (expected ~5%)")
    print()

    # Check 5: Behavioral patterns
    print("5. BEHAVIORAL PATTERNS:")

    # Export ratio - overall expected 3-10% given mix of normal + export_heavy users
    export_count = (df['action'].str.lower() == 'export').sum()
    export_pct = (export_count / len(df)) * 100
    if 3.0 <= export_pct <= 10.0:
        print(f"   Overall export ratio: {export_pct:.2f}% (expected 3-10%)")
    else:
        print(f"   Overall export ratio: {export_pct:.2f}% (expected 3-10%)")

    # export_heavy profile: 5 users with 18-28% exports
    user_export_ratios = []
    for user in df['user_id'].unique():
        user_df = df[df['user_id'] == user]
        user_exports = (user_df['action'].str.lower() == 'export').sum()
        user_export_ratio = (user_exports / len(user_df)) * 100
        user_export_ratios.append(user_export_ratio)

    high_export_users = sum(1 for r in user_export_ratios if r > 15.0)
    if high_export_users >= 3:
        print(f"   High-export users (>15%): {high_export_users} (expected 3-7)")
    else:
        print(f"   High-export users (>15%): {high_export_users} (expected 3-7)")

    # Night access - Security and DevOps have legitimate night access (~20% chance)
    df['hour'] = df['timestamp'].dt.hour
    df['is_night'] = (df['hour'] >= 22) | (df['hour'] <= 6)
    night_pct = (df['is_night'].sum() / len(df)) * 100
    if 8.0 <= night_pct <= 30.0:
        print(f"   Night access: {night_pct:.2f}% (expected 8-30%)")
    else:
        print(f"   Night access: {night_pct:.2f}% (expected 8-30%)")

    # Weekend access - DevOps (25%) and Security (20%) have weekend patterns
    df['is_weekend'] = df['timestamp'].dt.dayofweek >= 5
    weekend_pct = (df['is_weekend'].sum() / len(df)) * 100
    if 12.0 <= weekend_pct <= 30.0:
        print(f"   Weekend access: {weekend_pct:.2f}% (expected 12-30%)")
    else:
        print(f"   Weekend access: {weekend_pct:.2f}% (expected 12-30%)")

    # Resource diversity - 5 roles x 12 resources = 60 unique resources total
    unique_resources = df['resource_type'].str.lower().nunique()
    if 50 <= unique_resources <= 65:
        print(f"   Unique resources: {unique_resources} (expected 50-65)")
    else:
        print(f"   Unique resources: {unique_resources} (expected 50-65)")
    print()

    # Check 6: Value ranges
    print("6. VALUE RANGES:")

    # Session duration: min 5, max 240 (volatile users can go up to 240)
    session_min = df['session_duration'].min()
    session_max = df['session_duration'].max()
    if session_min >= 5 and session_max <= 240:
        print(f"   Session duration: {session_min:.0f}-{session_max:.0f} min (expected 5-240)")
    else:
        print(f"   Session duration: {session_min:.0f}-{session_max:.0f} min (expected 5-240)")

    # Access volume: min 1, max 50
    volume_min = df['access_volume'].min()
    volume_max = df['access_volume'].max()
    if volume_min >= 1 and volume_max <= 50:
        print(f"   Access volume: {volume_min:.0f}-{volume_max:.0f} (expected 1-50)")
    else:
        print(f"   Access volume: {volume_min:.0f}-{volume_max:.0f} (expected 1-50)")

    # Success rate: 95% target
    success_rate = (df['success_flag'].sum() / len(df)) * 100
    if 92.0 <= success_rate <= 98.0:
        print(f"   Success rate: {success_rate:.2f}% (expected 92-98%)")
    else:
        print(f"   Success rate: {success_rate:.2f}% (expected 92-98%)")

    # Assigned resource count: 5-12 per user
    assigned_min = df['assigned_resource_count'].min()
    assigned_max = df['assigned_resource_count'].max()
    if assigned_min >= 5 and assigned_max <= 12:
        print(f"   Assigned resource count: {assigned_min:.0f}-{assigned_max:.0f} (expected 5-12)")
    else:
        print(f"   Assigned resource count: {assigned_min:.0f}-{assigned_max:.0f} (expected 5-12)")

    # Actively used count must always be <= assigned count
    invalid_usage = (df['actively_used_resource_count'] > df['assigned_resource_count']).sum()
    if invalid_usage == 0:
        print(f"   Actively used <= assigned: always valid (no violations)")
    else:
        print(f"   Actively used > assigned: {invalid_usage} violations found")
    print()

    # Check 7: Column schema
    print("7. COLUMN SCHEMA:")
    expected_columns = [
        'user_id', 'role', 'resource_type', 'action',
        'timestamp', 'session_duration', 'access_volume', 'success_flag',
        'assigned_resource_count', 'actively_used_resource_count'
    ]

    for col in expected_columns:
        if col in df.columns:
            print(f"   {col}")
        else:
            print(f"   {col} MISSING")

    computed_columns = {'is_night', 'is_weekend', 'hour', 'week'}
    extra_columns = set(df.columns) - set(expected_columns) - computed_columns
    if extra_columns:
        print(f"   Extra columns found: {extra_columns}")
    print()

    # Check 8: Privilege-usage misalignment (new - aligned with generator)
    print("8. PRIVILEGE-USAGE MISALIGNMENT:")

    user_misalignment = df.groupby('user_id').agg(
        assigned=('assigned_resource_count', 'first'),
        used=('actively_used_resource_count', 'first')
    )
    user_misalignment['gap'] = user_misalignment['assigned'] - user_misalignment['used']
    user_misalignment['usage_ratio'] = user_misalignment['used'] / user_misalignment['assigned']

    avg_gap = user_misalignment['gap'].mean()
    print(f"   Avg assigned resources per user: {user_misalignment['assigned'].mean():.1f}")
    print(f"   Avg actively used per user:      {user_misalignment['used'].mean():.1f}")
    print(f"   Avg unused privilege gap:        {avg_gap:.1f} resources")

    # Over-provisioned users (usage ratio < 40%) - expect ~15 users
    low_usage_users = (user_misalignment['usage_ratio'] < 0.40).sum()
    if low_usage_users >= 10:
        print(f"   Low usage ratio users (<40%): {low_usage_users} (expected ~15)")
    else:
        print(f"   Low usage ratio users (<40%): {low_usage_users} (expected ~15)")
    print()

    # Check 9: Governance risk indicators detectable
    print("9. GOVERNANCE RISK INDICATORS (Detectable):")

    # Low resource diversity - over-provisioned users use few resources
    user_resource_diversity = df.groupby('user_id')['resource_type'].nunique()
    low_diversity_users = (user_resource_diversity <= 3).sum()
    print(f"   Low resource diversity users (<= 3 resources): {low_diversity_users}")
    print(f"     (Potential over-provisioned: accessing few resources despite privileges)")

    # High weekly variance - volatile users
    df['week'] = df['timestamp'].dt.isocalendar().week
    weekly_volume = df.groupby(['user_id', 'week'])['access_volume'].sum().reset_index()
    user_weekly_variance = weekly_volume.groupby('user_id')['access_volume'].std()
    high_variance_users = (user_weekly_variance > 20).sum()
    print(f"   High weekly variance users (std > 20): {high_variance_users}")
    print(f"     (Potential volatile behavior: unstable access patterns)")

    # Behavioral drift - gradually_drifting users show 30%+ increase
    early_weeks = df[df['week'] <= 12].groupby('user_id')['access_volume'].mean()
    late_weeks = df[df['week'] >= 40].groupby('user_id')['access_volume'].mean()
    drift_comparison = late_weeks / early_weeks
    drifting_users = (drift_comparison > 1.3).sum()
    print(f"   Potential drift users (>30% increase late vs early): {drifting_users}")
    print(f"     (Behavioral drift: increasing activity over time)")
    print()

    # Final summary
    print("=" * 80)
    print(" VALIDATION SUMMARY")
    print("=" * 80)
    print()
    print("Data generation successful!")
    print(f"   • {total_records:,} access records generated")
    print(f"   • {unique_users} users across 5 roles")
    print(f"   • Full year coverage (2024)")
    print(f"   • Data quality issues present for cleaning practice")
    print(f"   • Privilege-usage misalignment detectable")
    print(f"   • Governance risk patterns detectable")
    print()


if __name__ == "__main__":
    validate_data()