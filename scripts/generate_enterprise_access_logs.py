"""
Enterprise Privileged Access Log Generator
---
Aligned with: Role-Aware Privileged Access Risk Scoring System

PROBLEM STATEMENT ALIGNMENT:
- Generates privilege-usage misalignment (over-provisioned users)
- Creates peer deviation patterns (users acting unlike role peers)
- Implements multiple behavioral drift types (gradual, sudden, volatile)
- Produces realistic export ratios (2-5% normal, 15-28% governance risk)
- Tracks temporal stability/instability patterns

KEY FEATURES:
- 100 users across 5 roles
- 6 behavioral profiles (not binary high/low risk)
- 12 resources per role (sufficient variance for analysis)
- Privilege assignment vs. actual usage tracking
- ~12,000-15,000 access records
- 12-month timespan (January - December 2024)
- Intentional data quality issues

BEHAVIORAL PROFILES:
1. well_aligned_stable (55 users) - Normal, uses privileges appropriately
2. over_provisioned (15 users) - Has many privileges, uses few
3. gradually_drifting (12 users) - Stable → increasing activity over time
4. peer_deviant (8 users) - Behaves unlike same-role peers
5. export_heavy (5 users) - High export ratio (governance risk, not malicious)
6. volatile (5 users) - High week-to-week variance

OUTPUT: data/raw/enterprise_privileged_access_logs.csv
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
TARGET_LOGS = random.randint(12000, 15000)  # Variable total
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 12, 31)
NUM_WEEKS = 52

# Role distribution (20 users each)
ROLES = {
    'DB_Admin': 20,
    'HR_Admin': 20,
    'Cloud_Admin': 20,
    'Security_Admin': 20,
    'DevOps_Engineer': 20
}

# Resources per role (12 each - sufficient for misalignment analysis)
RESOURCES = {
    'DB_Admin': [
        'prod_database', 'analytics_db', 'data_warehouse', 'backup_db',
        'replication_service', 'query_tool', 'dev_database', 'staging_db',
        'archive_db', 'data_lake', 'etl_pipeline', 'schema_registry'
    ],
    'HR_Admin': [
        'hr_portal', 'payroll_system', 'employee_records', 'benefits_portal',
        'performance_system', 'recruitment_portal', 'onboarding_system',
        'time_tracking', 'compensation_db', 'training_portal',
        'exit_management', 'employee_surveys'
    ],
    'Cloud_Admin': [
        'aws_console', 'azure_portal', 'gcp_console', 'vm_management',
        'storage_buckets', 'api_gateway', 'load_balancer', 'cdn_manager',
        'security_groups', 'iam_console', 'cost_explorer', 'cloudwatch'
    ],
    'Security_Admin': [
        'siem_console', 'firewall_portal', 'audit_logs', 'vulnerability_scanner',
        'threat_intelligence', 'incident_response', 'ids_ips_console',
        'dlp_system', 'encryption_manager', 'access_review', 
        'security_dashboard', 'compliance_portal'
    ],
    'DevOps_Engineer': [
        'jenkins_pipeline', 'docker_registry', 'kubernetes_cluster',
        'terraform_console', 'ansible_tower', 'monitoring_dashboard',
        'grafana', 'prometheus', 'git_repository', 'artifact_registry',
        'ci_cd_console', 'deployment_manager'
    ]
}

ACTIONS = ['read', 'write', 'delete', 'export']

# BEHAVIORAL PROFILES (Core of Governance Risk Problem)

BEHAVIORAL_PROFILES = {
    'well_aligned_stable': {
        'count': 55,
        'privilege_usage_ratio': (0.70, 0.95),  # Uses 70-95% of assigned privileges
        'drift_type': 'stable',
        'peer_conformity': 'normal',
        'export_ratio_range': (0.02, 0.05),  # 2-5% exports (normal)
        'weekly_variance_factor': 0.15,  # Low variance
    },
    'over_provisioned': {
        'count': 15,
        'privilege_usage_ratio': (0.20, 0.40),  # Uses only 20-40% (MISALIGNMENT)
        'drift_type': 'stable',
        'peer_conformity': 'normal',
        'export_ratio_range': (0.02, 0.05),
        'weekly_variance_factor': 0.15,
    },
    'gradually_drifting': {
        'count': 12,
        'privilege_usage_ratio': (0.60, 0.85),
        'drift_type': 'gradual_escalation',  # Increases over time
        'peer_conformity': 'normal',
        'export_ratio_range': (0.03, 0.08),  # Slight increase
        'weekly_variance_factor': 0.20,
    },
    'peer_deviant': {
        'count': 8,
        'privilege_usage_ratio': (0.50, 0.80),
        'drift_type': 'stable',
        'peer_conformity': 'deviant',  # Acts like different role
        'export_ratio_range': (0.02, 0.06),
        'weekly_variance_factor': 0.15,
    },
    'export_heavy': {
        'count': 5,
        'privilege_usage_ratio': (0.60, 0.85),
        'drift_type': 'stable',
        'peer_conformity': 'normal',
        'export_ratio_range': (0.18, 0.28),  # 18-28% exports (GOVERNANCE RISK)
        'weekly_variance_factor': 0.15,
    },
    'volatile': {
        'count': 5,
        'privilege_usage_ratio': (0.50, 0.80),
        'drift_type': 'volatile_random',  # High week-to-week variance
        'peer_conformity': 'normal',
        'export_ratio_range': (0.03, 0.07),
        'weekly_variance_factor': 0.50,  # High variance
    },
}

# Normal action weights per role (what peers typically do)
ROLE_ACTION_WEIGHTS = {
    'DB_Admin':        [60, 32, 6, 2],   # 60% read, 32% write, 6% delete, 2% export
    'HR_Admin':        [65, 28, 4, 3],   # 65% read, 28% write, 4% delete, 3% export
    'Cloud_Admin':     [50, 38, 9, 3],   # 50% read, 38% write, 9% delete, 3% export
    'Security_Admin':  [60, 30, 7, 3],   # 60% read, 30% write, 7% delete, 3% export
    'DevOps_Engineer': [40, 45, 11, 4],  # 40% read, 45% write, 11% delete, 4% export
}

# Peer-deviant users act like a DIFFERENT role
PEER_DEVIANT_MAPPINGS = {
    'DB_Admin': 'Security_Admin',        # DB_Admin acts like Security (read-heavy)
    'HR_Admin': 'DevOps_Engineer',       # HR_Admin acts like DevOps (write-heavy)
    'Cloud_Admin': 'DB_Admin',           # Cloud acts like DB (read-heavy)
    'Security_Admin': 'Cloud_Admin',     # Security acts like Cloud (delete-heavy)
    'DevOps_Engineer': 'Security_Admin', # DevOps acts like Security (read-heavy)
}

# Role-specific session duration baselines
ROLE_SESSION_BASELINES = {
    'DB_Admin': 65,
    'HR_Admin': 35,
    'Cloud_Admin': 50,
    'Security_Admin': 55,
    'DevOps_Engineer': 40
}

# DRIFT PATTERNS (Temporal Behavior Evolution)

def drift_stable(week):
    """No drift - stable baseline"""
    return 1.0

def drift_gradual_escalation(week):
    """Gradual increase over time (governance risk pattern)"""
    if week < 12:
        return 1.0  # Baseline weeks 1-12
    elif week < 24:
        return 1.0 + ((week - 12) / 12) * 0.15  # Gradual 15% increase
    elif week < 36:
        return 1.15 + 0.30  # Sudden 30% spike
    else:
        return 1.25  # New elevated baseline

def drift_volatile_random(week):
    """High variance week-to-week (temporal instability)"""
    return np.random.uniform(0.70, 1.40)

def drift_sudden_spike_sustained(week):
    """Sudden spike then partial return"""
    if week < 20:
        return 1.0
    elif week < 35:
        return 1.50  # Sudden spike
    else:
        return 1.20  # Partial return

def drift_cyclical(week):
    """Seasonal pattern (legitimate for some roles)"""
    return 1.0 + 0.25 * np.sin(week * np.pi / 26)

DRIFT_FUNCTIONS = {
    'stable': drift_stable,
    'gradual_escalation': drift_gradual_escalation,
    'volatile_random': drift_volatile_random,
    'sudden_spike_sustained': drift_sudden_spike_sustained,
    'cyclical': drift_cyclical,
}

# USER GENERATION

def assign_behavioral_profile():
    """Assign profile based on distribution"""
    profiles = []
    for profile_name, config in BEHAVIORAL_PROFILES.items():
        profiles.extend([profile_name] * config['count'])
    
    random.shuffle(profiles)
    return profiles

def generate_users():
    """
    Generate 100 users with:
    - Assigned privileges (what they CAN access)
    - Used privileges (what they ACTUALLY access)
    - Behavioral profile (internal only - not saved to CSV)
    """
    users = []
    user_id = 1
    profiles = assign_behavioral_profile()
    profile_idx = 0
    
    for role, count in ROLES.items():
        for i in range(count):
            profile_name = profiles[profile_idx]
            profile_config = BEHAVIORAL_PROFILES[profile_name]
            
            # Privilege assignment (what user CAN access)
            total_resources = len(RESOURCES[role])
            assigned_count = random.randint(5, total_resources)  # Assign 5-12 resources
            assigned_resources = random.sample(RESOURCES[role], assigned_count)
            
            # Actual usage (what user DOES access regularly)
            usage_ratio = random.uniform(*profile_config['privilege_usage_ratio'])
            actively_used_count = max(2, int(assigned_count * usage_ratio))
            actively_used_resources = random.sample(assigned_resources, actively_used_count)
            
            # Export behavior target
            export_ratio_target = random.uniform(*profile_config['export_ratio_range'])
            
            users.append({
                'user_id': f'USER_{user_id:03d}',
                'role': role,
                'profile': profile_name,  # INTERNAL ONLY
                'assigned_resources': assigned_resources,
                'actively_used_resources': actively_used_resources,
                'assigned_resource_count': assigned_count,        # saved to CSV for misalignment modeling
                'actively_used_resource_count': actively_used_count,  # saved to CSV for misalignment modeling
                'export_ratio_target': export_ratio_target,
                'drift_type': profile_config['drift_type'],
                'peer_conformity': profile_config['peer_conformity'],
                'weekly_variance_factor': profile_config['weekly_variance_factor'],
            })
            user_id += 1
            profile_idx += 1
    
    return pd.DataFrame(users)

# TIMESTAMP GENERATION

def generate_timestamp(start, end, role, profile):
    """Generate realistic timestamp with role and profile-specific patterns"""
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    timestamp = start + timedelta(seconds=random_seconds)
    
    # NIGHT ACCESS PATTERNS
    # DevOps and Security have legitimate night work (24/7 roles)
    if role in ['DevOps_Engineer', 'Security_Admin']:
        if random.random() < 0.12:
            timestamp = timestamp.replace(hour=random.choice([22, 23, 0, 1, 2, 3]))
    
    # Volatile users have erratic hours (high time variance)
    if profile == 'volatile':
        if random.random() < 0.25:
            timestamp = timestamp.replace(hour=random.randint(0, 23))
    
    # WEEKEND ACCESS PATTERNS
    # DevOps has legitimate weekend deployments
    if role == 'DevOps_Engineer' and random.random() < 0.18:
        days_to_weekend = (5 - timestamp.weekday()) % 7
        if days_to_weekend == 0:
            days_to_weekend = 1
        timestamp = timestamp + timedelta(days=days_to_weekend)
    
    # Security has weekend monitoring
    if role == 'Security_Admin' and random.random() < 0.15:
        days_to_weekend = (5 - timestamp.weekday()) % 7
        if days_to_weekend == 0:
            days_to_weekend = 1
        timestamp = timestamp + timedelta(days=days_to_weekend)
    
    # HR_Admin has MINIMAL weekend activity (business hours role)
    if role == 'HR_Admin' and timestamp.weekday() >= 5:
        if random.random() < 0.95:
            days_to_subtract = timestamp.weekday() - 4
            timestamp = timestamp - timedelta(days=days_to_subtract)
    
    # Business hours bias for most roles (9 AM - 5 PM)
    if role in ['DB_Admin', 'HR_Admin'] and random.random() < 0.70:
        timestamp = timestamp.replace(hour=random.randint(9, 17))

    # HARD BOUNDARY CHECK 
    if timestamp > end:
        timestamp = end - timedelta(seconds=random.randint(0, 86400))

    if timestamp < start:
        timestamp = start + timedelta(seconds=random.randint(0, 86400))
    
    return timestamp


# ACCESS RECORD GENERATION


def generate_access_record(user, timestamp):
    """
    Generate single access record with:
    - Privilege-usage alignment/misalignment
    - Peer-conforming or peer-deviant behavior
    - Export ratio matching user's governance risk profile
    """
    role = user['role']
    profile = user['profile']
    
    # 1. SELECT RESOURCE (from actively_used set, not all assigned)
    # Over-provisioned users favor 1-2 "core" resources heavily
    if profile == 'over_provisioned':
        core_resources = user['actively_used_resources'][:2]
        weights = [0.35 if r in core_resources else 0.05 for r in user['actively_used_resources']]
        resource = random.choices(user['actively_used_resources'], weights=weights)[0]
    else:
        resource = random.choice(user['actively_used_resources'])
    
    # 2. SELECT ACTION (with peer-conforming or peer-deviant weights)
    if user['peer_conformity'] == 'deviant':
        # Use action weights from a DIFFERENT role
        deviant_role = PEER_DEVIANT_MAPPINGS[role]
        action_weights = ROLE_ACTION_WEIGHTS[deviant_role]
    else:
        # Normal role-specific weights
        action_weights = ROLE_ACTION_WEIGHTS[role].copy()
    
    # Adjust export weight based on user's export_ratio_target
    # Export-heavy users should have much higher export weight
    if profile == 'export_heavy':
        # Recalculate weights to achieve 18-28% exports
        action_weights = [40, 33, 4, 23]  # ~23% exports (aligned with 18-28% range)
    
    action = random.choices(ACTIONS, weights=action_weights)[0]
    
    # 3. SESSION DURATION (role + profile-aware variability)
    base_duration = ROLE_SESSION_BASELINES[role]

    if profile == 'volatile':
        duration_std = 25
    elif profile == 'gradually_drifting':
        duration_std = 20
    else:
        duration_std = 12

    session_duration = int(np.random.normal(base_duration, duration_std))
    session_duration = max(5, min(240, session_duration))
    
    # 4. ACCESS VOLUME (role-aware base, with profile adjustments)
    role_base_volume = {
        'DB_Admin': 14,
        'HR_Admin': 8,
        'Cloud_Admin': 20,
        'Security_Admin': 18,
        'DevOps_Engineer': 25,
    }
    base_volume = role_base_volume[role]

    # Gradual drifters and volatile users have higher base
    if profile in ['gradually_drifting', 'volatile']:
        base_volume = int(base_volume * 1.4)
    
    if role in ['DB_Admin', 'Cloud_Admin']:
        volume_std = 6
    elif role == 'Security_Admin':
        volume_std = 7
    else:
        volume_std = 4

    if profile == 'volatile':
        volume_std *= 1.5

    access_volume = int(np.random.normal(base_volume, volume_std))
    
    # SPIKE EVENTS (rare but present)
    # 5% chance for gradually_drifting users
    if profile == 'gradually_drifting' and random.random() < 0.05:
        access_volume = int(access_volume * random.uniform(2.0, 3.0))
    
    # 1% chance for normal users
    if profile == 'well_aligned_stable' and random.random() < 0.01:
        access_volume = int(access_volume * random.uniform(1.5, 2.0))
    
    access_volume = max(1, min(50, access_volume))
    
    # 5. SUCCESS FLAG (95% success rate)
    # Kept for realism. Not a primary governance feature.
    # success_flag represents authentication/permission results.
    # It is NOT used as a governance risk signal
    success = random.random() < 0.95
    
    return {
        'user_id': user['user_id'],
        'role': user['role'],
        'resource_type': resource,
        'action': action,
        'timestamp': timestamp,
        'session_duration': session_duration,
        'access_volume': access_volume,
        'success_flag': success,
        'assigned_resource_count': user['assigned_resource_count'],       # how many resources user CAN access
        'actively_used_resource_count': user['actively_used_resource_count'],  # how many they ACTUALLY use
    }

# WEEKLY LOG GENERATION WITH DRIFT

def generate_logs_for_user(user, num_logs, start_date, end_date):
    """
    Generate logs with:
    - Weekly consistency
    - Behavioral drift based on profile
    - Week-to-week variance based on profile
    """
    records = []
    logs_per_week_base = num_logs / NUM_WEEKS
    drift_function = DRIFT_FUNCTIONS[user['drift_type']]
    
    current_start = start_date
    while current_start <= end_date:
        week_start = current_start
        week_end = min(current_start + timedelta(days=7), end_date)
        week_num = (current_start - start_date).days // 7
        
        # Apply drift factor
        drift_factor = drift_function(week_num)
        
        # Apply weekly variance
        variance = user['weekly_variance_factor']
        variance_multiplier = np.random.normal(1.0, variance)
        
        logs_this_week = int(logs_per_week_base * drift_factor * variance_multiplier)
        logs_this_week = max(1, logs_this_week)
        
        for _ in range(logs_this_week):
            timestamp = generate_timestamp(
                week_start,
                week_end,
                user['role'],
                user['profile']
            )
            record = generate_access_record(user, timestamp)
            records.append(record)

        current_start += timedelta(days=7)
    
    return records

# DATA QUALITY ISSUES

def introduce_data_quality_issues(df):
    """Add intentional data quality problems for cleaning practice"""
    df = df.copy()
    
    # 1. Missing values (~3% of records)
    missing_count = int(len(df) * 0.03)
    missing_indices_duration = np.random.choice(df.index, size=missing_count, replace=False)
    df.loc[missing_indices_duration, 'session_duration'] = np.nan
    
    missing_indices_volume = np.random.choice(df.index, size=missing_count, replace=False)
    df.loc[missing_indices_volume, 'access_volume'] = np.nan
    
    # 2. Duplicate records (~2%)
    duplicate_count = int(len(df) * 0.02)
    duplicate_indices = np.random.choice(df.index, size=duplicate_count, replace=False)
    duplicates = df.loc[duplicate_indices].copy()
    df = pd.concat([df, duplicates], ignore_index=True)
    
    # 3. Inconsistent formatting (uppercase resource_type in 10%)
    uppercase_count = int(len(df) * 0.10)
    uppercase_indices = np.random.choice(df.index, size=uppercase_count, replace=False)
    df.loc[uppercase_indices, 'resource_type'] = df.loc[uppercase_indices, 'resource_type'].str.upper()
    
    # 4. Extra whitespace in role column (5%)
    whitespace_count = int(len(df) * 0.05)
    whitespace_indices = np.random.choice(df.index, size=whitespace_count, replace=False)
    df.loc[whitespace_indices, 'role'] = ' ' + df.loc[whitespace_indices, 'role'] + ' '
    
    # 5. Mixed case in action column (8%)
    mixcase_count = int(len(df) * 0.08)
    mixcase_indices = np.random.choice(df.index, size=mixcase_count, replace=False)
    df.loc[mixcase_indices, 'action'] = df.loc[mixcase_indices, 'action'].str.capitalize()
    
    return df

# MAIN EXECUTION

def main():
    print("=" * 80)
    print(" ECHELON - Enterprise Privileged Access Log Generator")
    print(" Aligned with: Role-Aware Privileged Access Risk Scoring System")
    print("=" * 80)
    print()
    
    # Step 1: Generate users
    print(" Step 1: Generating users with behavioral profiles...")
    users_df = generate_users()
    
    print(f"   Created {len(users_df)} users across {len(ROLES)} roles")
    for role in ROLES:
        print(f"     ├─ {role}: {len(users_df[users_df['role'] == role])} users")
    print()
    
    print("   Behavioral Profile Distribution:")
    for profile_name, config in BEHAVIORAL_PROFILES.items():
        count = len(users_df[users_df['profile'] == profile_name])
        print(f"     ├─ {profile_name}: {count} users")
    print()
    
    # Step 2: Generate access logs
    print(f" Step 2: Generating ~{TARGET_LOGS:,} access logs...")
    print(f"   Time span: {START_DATE.date()} to {END_DATE.date()} (12 months)")
    print()
    
    logs_per_user = TARGET_LOGS // NUM_USERS
    extra_logs = TARGET_LOGS % NUM_USERS
    records = []
    
    for idx, (_, user) in enumerate(users_df.iterrows()):
        num_logs = logs_per_user + (1 if idx < extra_logs else 0)
        user_records = generate_logs_for_user(user, num_logs, START_DATE, END_DATE)
        records.extend(user_records)
        
        if (idx + 1) % 20 == 0:
            print(f"   Generated logs for {idx + 1}/{NUM_USERS} users...")
    
    df = pd.DataFrame(records)
    print(f"\n   Generated {len(df):,} access records")
    print(f"     └─ Average logs per user: {len(df) / NUM_USERS:.1f}")
    print()
    
    # Step 3: Add data quality issues
    print(" Step 3: Adding data quality issues...")
    original_count = len(df)
    df = introduce_data_quality_issues(df)
    print(f"   Data quality issues added:")
    print(f"     ├─ Original records:  {original_count:,}")
    print(f"     ├─ After duplicates:  {len(df):,}")
    print(f"     ├─ Missing values:    ~{int(original_count * 0.06):,} cells")
    print(f"     └─ Formatting issues: ~{int(original_count * 0.23):,} records")
    print()
    
    # Step 4: Remove internal tracking columns, keep misalignment columns
    print(" Step 4: Removing internal tracking columns...")
    df = df[['user_id', 'role', 'resource_type', 'action', 'timestamp',
             'session_duration', 'access_volume', 'success_flag',
             'assigned_resource_count', 'actively_used_resource_count']]
    print("   Removed: profile, assigned_resources, actively_used_resources, etc.")
    print("   Final columns: user_id, role, resource_type, action, timestamp,")
    print("                    session_duration, access_volume, success_flag,")
    print("                    assigned_resource_count, actively_used_resource_count")
    print()
    
    # Step 5: Save to CSV
    print(" Step 5: Saving to CSV...")
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    output_path = '../data/raw/enterprise_privileged_access_logs.csv'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    
    print(f"   Saved to: {output_path}")
    print()
    
    # Step 6: Summary statistics
    print("=" * 80)
    print(" FINAL DATASET SUMMARY")
    print("=" * 80)
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    print(f" Date range:      {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f" Total records:   {len(df):,}")
    print(f" Unique users:    {df['user_id'].nunique()}")
    print(f" Roles:           {df['role'].nunique()}")
    print(f" Total resources: {df['resource_type'].nunique()}")
    print(f"  Missing values:  {df.isnull().sum().sum()}")
    print(f" Duplicate rows:  {df.duplicated().sum()}")
    print()

    # Privilege-usage misalignment summary
    user_misalignment = df.groupby('user_id').agg(
        assigned=('assigned_resource_count', 'first'),
        used=('actively_used_resource_count', 'first')
    )
    user_misalignment['gap'] = user_misalignment['assigned'] - user_misalignment['used']
    print(" PRIVILEGE-USAGE MISALIGNMENT SUMMARY:")
    print(f"   ├─ Avg assigned resources per user: {user_misalignment['assigned'].mean():.1f}")
    print(f"   ├─ Avg actively used per user:      {user_misalignment['used'].mean():.1f}")
    print(f"   └─ Avg unused privilege gap:        {user_misalignment['gap'].mean():.1f} resources")
    print()
    
    # Temporal patterns
    df['is_weekend'] = df['timestamp'].dt.dayofweek >= 5
    df['hour'] = df['timestamp'].dt.hour
    df['is_night'] = (df['hour'] >= 22) | (df['hour'] <= 6)
    
    weekend_pct = (df['is_weekend'].sum() / len(df)) * 100
    night_pct = (df['is_night'].sum() / len(df)) * 100
    
    print(" TEMPORAL PATTERNS:")
    print(f"   ├─ Weekend records: {df['is_weekend'].sum():,} ({weekend_pct:.1f}%)")
    print(f"   └─ Night records:   {df['is_night'].sum():,} ({night_pct:.1f}%)")
    print()
    
    # Action distribution
    print(" ACTION DISTRIBUTION:")
    for action in ACTIONS:
        count = (df['action'].str.lower() == action).sum()
        pct = (count / len(df)) * 100
        print(f"   ├─ {action}: {count:,} ({pct:.1f}%)")
    print()
    
    # Role distribution
    print("  ROLE DISTRIBUTION:")
    for role in ROLES:
        count = len(df[df['role'].str.strip() == role])
        print(f"   ├─ {role}: {count:,} records")
    print()
    
    print("=" * 80)
    print(" DATA GENERATION COMPLETE!")
    print("=" * 80)
    print()
    print(" WHAT THIS DATASET CONTAINS:")
    print("   • Privilege-usage misalignment (over-provisioned users)")
    print("   • assigned_resource_count vs actively_used_resource_count gap")
    print("   • Peer deviation patterns (users acting unlike role peers)")
    print("   • Multiple behavioral drift types (gradual, sudden, volatile)")
    print("   • Realistic export ratios (2-5% normal, 18-28% high-risk)")
    print("   • Temporal stability/instability patterns")
    print("   • Intentional data quality issues for cleaning practice")
    print()

if __name__ == "__main__":
    main()