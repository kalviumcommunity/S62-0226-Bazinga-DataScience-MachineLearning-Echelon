"""
Echelon - Privileged Access Log Data Generator
================================================
Generates realistic enterprise access logs with:
- 100 users across 5 roles (20 per role)
- ~15,000 access records (exact count varies slightly due to weekly drift)
- 12-month time span (January - December 2024)
- ALL 9 FEATURES FULLY SUPPORTED:
   - Average daily access volume
   - Export action ratio
   - Unique resources accessed
   - Average session duration
   - Night access percentage
   - Weekend activity ratio (WITH PATTERNS)
   - Access time variance
   - Week-over-week change (WITH GRADUAL DRIFT)
   - Access spike indicator (WITH SPIKE EVENTS)
- 15% high-risk users with suspicious patterns (hidden from final CSV)
- Intentional data quality issues for cleaning practice

DESIGN PRINCIPLE:
  risk_profile is used ONLY internally to simulate realistic behavior.
  It is NOT saved in the final CSV.
  The model must discover risk independently from behavioral features.
  This keeps the system unsupervised and governance-focused.

HOW TO RUN:
  Navigate to the scripts/ folder and run:
    python generate_data.py
  Output saved to: ../data/raw/privileged_access_logs.csv
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# CONFIGURATION 
np.random.seed(42)
random.seed(42)

NUM_USERS = 100
TOTAL_LOGS = 15000
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 12, 31)
NUM_WEEKS = 52

# Role distribution: 20 users per role
ROLES = {
    'DB_Admin': 20,
    'HR_Admin': 20,
    'Cloud_Admin': 20,
    'Security_Admin': 20,
    'DevOps_Engineer': 20
}

# Resources by role (6 resources each)
RESOURCES = {
    'DB_Admin': [
        'prod_database', 'analytics_db', 'data_warehouse',
        'backup_db', 'replication_service', 'query_tool'
    ],
    'HR_Admin': [
        'hr_portal', 'payroll_system', 'employee_records',
        'benefits_portal', 'performance_system', 'recruitment_portal'
    ],
    'Cloud_Admin': [
        'aws_console', 'azure_portal', 'vm_management',
        'storage_buckets', 'api_gateway', 'load_balancer'
    ],
    'Security_Admin': [
        'siem_console', 'firewall_portal', 'audit_logs',
        'vulnerability_scanner', 'threat_intelligence', 'incident_response'
    ],
    'DevOps_Engineer': [
        'jenkins_pipeline', 'docker_registry', 'kubernetes_cluster',
        'terraform_console', 'ansible_tower', 'monitoring_dashboard'
    ]
}

ACTIONS = ['read', 'write', 'delete', 'export']


# USER GENERATION 
def generate_users():
    """
    Generate 100 users with roles and hidden risk profiles.
    NOTE: risk_profile is used internally only - it will NOT be saved to CSV.
    """
    users = []
    user_id = 1

    for role, count in ROLES.items():
        for i in range(count):
            # 15% of users have high-risk behavior patterns
            # This is a simulation tool only - not a label for the model
            is_high_risk = random.random() < 0.15

            users.append({
                'user_id': f'USER_{user_id:03d}',
                'role': role,
                'risk_profile': 'high' if is_high_risk else 'normal'  # INTERNAL USE ONLY
            })
            user_id += 1

    return pd.DataFrame(users)


# TIMESTAMP GENERATION
def generate_timestamp(start, end, role, is_high_risk):
    """Generate realistic timestamp with role-specific night and weekend patterns."""
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    timestamp = start + timedelta(seconds=random_seconds)

    # NIGHT ACCESS PATTERNS
    # DevOps and Security have legitimate night work
    if role in ['DevOps_Engineer', 'Security_Admin']:
        if random.random() < 0.25:
            timestamp = timestamp.replace(hour=random.choice([22, 23, 0, 1, 2, 3]))

    # High-risk users have EXCESSIVE night access (suspicious)
    if is_high_risk and random.random() < 0.40:
        timestamp = timestamp.replace(hour=random.choice([22, 23, 0, 1, 2, 3, 4, 5]))

    # WEEKEND ACCESS PATTERNS
    # DevOps has legitimate weekend deployments
    if role == 'DevOps_Engineer' and random.random() < 0.30:
        days_to_weekend = (5 - timestamp.weekday()) % 7
        if days_to_weekend == 0:
            days_to_weekend = 1
        timestamp = timestamp + timedelta(days=days_to_weekend)

    # Security has weekend monitoring
    if role == 'Security_Admin' and random.random() < 0.25:
        days_to_weekend = (5 - timestamp.weekday()) % 7
        if days_to_weekend == 0:
            days_to_weekend = 1
        timestamp = timestamp + timedelta(days=days_to_weekend)

    # High-risk users have EXCESSIVE weekend activity (suspicious)
    if is_high_risk and random.random() < 0.35:
        days_to_weekend = (5 - timestamp.weekday()) % 7
        if days_to_weekend == 0:
            days_to_weekend = 1
        timestamp = timestamp + timedelta(days=days_to_weekend)

    # HR_Admin has MINIMAL weekend activity (business hours role)
    if role == 'HR_Admin' and timestamp.weekday() >= 5:
        if random.random() < 0.95:
            days_to_subtract = timestamp.weekday() - 4
            timestamp = timestamp - timedelta(days=days_to_subtract)

    return timestamp


# ACCESS RECORD GENERATION
def generate_access_record(user, timestamp):
    """
    Generate single access record with role-specific behavior.
    Uses internal risk_profile to shape behavior but does NOT include it in output.
    """
    role = user['role']
    is_high_risk = user['risk_profile'] == 'high'

    # 1. SELECT RESOURCE
    resource = random.choice(RESOURCES[role])

    # 2. SELECT ACTION with role-specific weights
    # High-risk users have more exports and writes (suspicious patterns)
    action_weights_map = {
        'Security_Admin': [30, 25, 5, 40] if is_high_risk else [45, 30, 5, 20],
        'DevOps_Engineer': [25, 40, 25, 10] if is_high_risk else [35, 40, 15, 10],
        'DB_Admin':        [35, 30, 5, 30] if is_high_risk else [55, 30, 5, 10],
        'HR_Admin':        [40, 25, 3, 32] if is_high_risk else [60, 30, 2, 8],
        'Cloud_Admin':     [30, 35, 20, 15] if is_high_risk else [45, 35, 10, 10],
    }
    action = random.choices(ACTIONS, weights=action_weights_map[role], k=1)[0]

    # 3. SESSION DURATION (role-specific baseline)
    base_duration = {
        'DB_Admin': 65,
        'HR_Admin': 35,
        'Cloud_Admin': 45,
        'Security_Admin': 55,
        'DevOps_Engineer': 40
    }[role]

    # High-risk users have longer sessions
    if is_high_risk:
        session_duration = int(np.random.normal(base_duration + 35, 25))
    else:
        session_duration = int(np.random.normal(base_duration, 15))
    session_duration = max(5, min(180, session_duration))

    # 4. ACCESS VOLUME (high-risk users access more, with spikes)
    if is_high_risk:
        access_volume = int(np.random.normal(28, 9))
        # 10% chance of spike event for high-risk users
        if random.random() < 0.10:
            spike_multiplier = random.uniform(1.8, 2.5)
            access_volume = int(access_volume * spike_multiplier)
    else:
        access_volume = int(np.random.normal(12, 5))
        # 3% rare spikes for normal users
        if random.random() < 0.03:
            spike_multiplier = random.uniform(1.5, 2.0)
            access_volume = int(access_volume * spike_multiplier)
    access_volume = max(1, min(50, access_volume))

    # 5. SUCCESS FLAG (95% success rate)
    success = random.random() < 0.95
    # risk_profile is intentionally excluded here
    return {
        'user_id': user['user_id'],
        'role': user['role'],
        'resource_type': resource,
        'action': action,
        'timestamp': timestamp,
        'session_duration': session_duration,
        'access_volume': access_volume,
        'success_flag': success
    }


# WEEKLY LOG GENERATION WITH GRADUAL DRIFT
def generate_logs_for_user(user, num_logs, start_date, end_date):
    """
    Generate logs with weekly consistency and behavioral drift.
    High-risk users show gradual increase in activity over time (Feature 8).
    """
    records = []
    logs_per_week_base = num_logs / NUM_WEEKS

    for week_num in range(NUM_WEEKS):
        week_start = start_date + timedelta(weeks=week_num)
        week_end = week_start + timedelta(days=7)

        # GRADUAL DRIFT: High-risk users increase activity 1% per week
        if user['risk_profile'] == 'high':
            drift_factor = 1.0 + (week_num * 0.01)  # ~50% increase by week 52

            # 15% of weeks have volatile spikes
            if random.random() < 0.15:
                spike_multiplier = random.uniform(1.8, 2.5)
                logs_this_week = int(logs_per_week_base * drift_factor * spike_multiplier)
            else:
                logs_this_week = int(logs_per_week_base * drift_factor)
        else:
            # Normal users: stable with minor variance
            logs_this_week = int(np.random.normal(logs_per_week_base, logs_per_week_base * 0.2))

        logs_this_week = max(1, logs_this_week)

        for _ in range(logs_this_week):
            timestamp = generate_timestamp(
                week_start,
                min(week_end, end_date),
                user['role'],
                user['risk_profile'] == 'high'
            )
            record = generate_access_record(user, timestamp)
            records.append(record)

    return records


# DATA QUALITY ISSUES
def introduce_data_quality_issues(df):
    """
    Add intentional data quality problems for cleaning practice.
    These will be fixed in notebook 02_data_cleaning.ipynb.
    """
    df = df.copy()

    # 1. Missing values (~2% of records)
    missing_count = int(len(df) * 0.02)
    missing_indices_duration = np.random.choice(df.index, size=missing_count, replace=False)
    df.loc[missing_indices_duration, 'session_duration'] = np.nan

    missing_indices_volume = np.random.choice(df.index, size=missing_count, replace=False)
    df.loc[missing_indices_volume, 'access_volume'] = np.nan

    # 2. Duplicate records (~1%)
    duplicate_count = int(len(df) * 0.01)
    duplicate_indices = np.random.choice(df.index, size=duplicate_count, replace=False)
    duplicates = df.loc[duplicate_indices].copy()
    df = pd.concat([df, duplicates], ignore_index=True)

    # 3. Inconsistent formatting (uppercase resource_type in 10% of records)
    uppercase_count = int(len(df) * 0.10)
    uppercase_indices = np.random.choice(df.index, size=uppercase_count, replace=False)
    df.loc[uppercase_indices, 'resource_type'] = df.loc[uppercase_indices, 'resource_type'].str.upper()

    # 4. Extra whitespace in role column (5% of records)
    whitespace_count = int(len(df) * 0.05)
    whitespace_indices = np.random.choice(df.index, size=whitespace_count, replace=False)
    df.loc[whitespace_indices, 'role'] = ' ' + df.loc[whitespace_indices, 'role'] + ' '

    # 5. Mixed case in action column (8% of records)
    mixcase_count = int(len(df) * 0.08)
    mixcase_indices = np.random.choice(df.index, size=mixcase_count, replace=False)
    df.loc[mixcase_indices, 'action'] = df.loc[mixcase_indices, 'action'].str.capitalize()

    return df


# MAIN
def main():
    print("=" * 70)
    print(" ECHELON - Privileged Access Log Data Generator")
    print("=" * 70)
    print()

    # Generate users
    print(" Generating users...")
    users_df = generate_users()

    high_risk_count = len(users_df[users_df['risk_profile'] == 'high'])
    print(f" Created {len(users_df)} users")
    for role in ROLES:
        print(f"   ├─ {role}: {len(users_df[users_df['role'] == role])}")
    print(f"   └─ High-risk profiles (internal): {high_risk_count} (~15%)")
    print()

    # Generate access logs 
    print(f" Generating ~{TOTAL_LOGS:,} access logs...")
    print(f"   Time span: {START_DATE.date()} to {END_DATE.date()} (12 months)")
    print()

    logs_per_user = TOTAL_LOGS // NUM_USERS
    extra_logs = TOTAL_LOGS % NUM_USERS
    records = []

    for idx, (_, user) in enumerate(users_df.iterrows()):
        num_logs = logs_per_user + (1 if idx < extra_logs else 0)
        user_records = generate_logs_for_user(user, num_logs, START_DATE, END_DATE)
        records.extend(user_records)

        if (idx + 1) % 20 == 0:
            print(f"   ✓ Generated logs for {idx + 1}/{NUM_USERS} users...")

    df = pd.DataFrame(records)
    print(f" Generated {len(df):,} access records")
    print(f"   └─ Average logs per user: {len(df) / NUM_USERS:.1f}")
    print()

    # Add data quality issues 
    print(" Adding data quality issues...")
    original_count = len(df)
    df = introduce_data_quality_issues(df)
    print(f"  Data quality issues added")
    print(f"   ├─ Original records:  {original_count:,}")
    print(f"   ├─ After duplicates:  {len(df):,}")
    print(f"   ├─ Missing values:    ~{int(original_count * 0.02 * 2):,} cells")
    print(f"   └─ Formatting issues: ~{int(original_count * 0.23):,} records")
    print()

    # Remove risk_profile before saving 
    # CRITICAL: risk_profile must NOT appear in the final dataset.
    # The governance model must discover risk independently from behavior.
    # Keeping this column would make the system supervised, not unsupervised.
    df = df.drop(columns=['risk_profile'], errors='ignore')
    print(" Removed internal 'risk_profile' column from final dataset.")
    print("   (Risk must be discovered by the model, not pre-labeled)")
    print()

    # Sort and save 
    print(" Step 5: Saving to CSV...")
    df = df.sort_values('timestamp').reset_index(drop=True)

    # Path assumes script is run from inside the scripts/ folder
    output_path = '../data/raw/privileged_access_logs.csv'

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_csv(output_path, index=False)
    print(f" Saved to: {output_path}")
    print()

    # Summary 
    print("=" * 70)
    print(" FINAL DATASET SUMMARY")
    print("=" * 70)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    print(f"Date range:      {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"Total records:   {len(df):,}")
    print(f"Unique users:    {df['user_id'].nunique()}")
    print(f"Columns:         {list(df.columns)}")
    print(f"Missing values:  {df.isnull().sum().sum()}")
    print(f"Duplicate rows:  {df.duplicated().sum()}")
    print()

    df['is_weekend'] = df['timestamp'].dt.dayofweek >= 5
    df['hour'] = df['timestamp'].dt.hour
    df['is_night'] = (df['hour'] >= 22) | (df['hour'] <= 6)
    weekend_pct = (df['is_weekend'].sum() / len(df)) * 100
    night_pct = (df['is_night'].sum() / len(df)) * 100
    print(f"  TEMPORAL PATTERNS:")
    print(f"   Weekend records: {df['is_weekend'].sum():,} ({weekend_pct:.1f}%)")
    print(f"   Night records:   {df['is_night'].sum():,} ({night_pct:.1f}%)")
    print()

    print("=" * 70)
    print(" COLUMNS IN FINAL CSV (no risk labels):")
    for col in df.drop(columns=['is_weekend', 'hour', 'is_night']).columns:
        print(f"   • {col}")
    print()
    print(" DATA GENERATION COMPLETE!")
    print("=" * 70)


if __name__ == "__main__":
    main()