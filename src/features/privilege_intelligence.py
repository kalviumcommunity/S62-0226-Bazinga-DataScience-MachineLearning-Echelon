"""
Privilege-Usage Intelligence Features
---
🎯 THIS IS THE CORE DIFFERENTIATOR OF YOUR PROJECT

Measures the alignment between:
- What privileges users are ASSIGNED (can access)
- What privileges users ACTUALLY USE (do access)

Traditional systems only check if access is permitted.
This module checks if permitted access is actually needed.
"""

import pandas as pd
import numpy as np


def calculate_privilege_usage_gap(df):
    """
    Feature 10: Privilege-Usage Gap
    
    🎯 CORE FEATURE - THIS DEFINES YOUR PROJECT
    
    Definition: Difference between assigned and actively used resource count
    
    Business Logic:
    - Over-provisioned users: Large gap (assigned >> used)
      Example: Assigned 10 resources, uses only 3 → Gap = 7
    - Well-aligned users: Small gap (assigned ≈ used)
      Example: Assigned 8 resources, uses 7 → Gap = 1
    - This is THE indicator of privilege creep and over-privileging
    
    Why This Matters:
    - Traditional RBAC: "Does user have permission?" ✓
    - Your approach: "Does user NEED the permission they have?" ← NEW
    
    Args:
        df: DataFrame with 'assigned_resource_count' and 'actively_used_resource_count'
    
    Returns:
        DataFrame with 'privilege_usage_gap' column added
    """
    print("  [1/3] Calculating privilege_usage_gap...")
    print("      🎯 CORE FEATURE: Privilege-Usage Intelligence")
    
    # Get per-user privilege assignment vs usage
    user_privilege = df.groupby('user_id').agg({
        'assigned_resource_count': 'first',
        'actively_used_resource_count': 'first'
    })
    
    # Calculate gap (unused privileges)
    user_privilege['privilege_usage_gap'] = (
        user_privilege['assigned_resource_count'] - 
        user_privilege['actively_used_resource_count']
    )
    
    # Map back to main dataframe
    df['privilege_usage_gap'] = df['user_id'].map(user_privilege['privilege_usage_gap'])
    
    # Statistics
    print(f"      Gap Range: [{df['privilege_usage_gap'].min():.0f}, {df['privilege_usage_gap'].max():.0f}] unused resources")
    print(f"      Mean Gap: {df['privilege_usage_gap'].mean():.2f} unused resources per user")
    
    # Identify severely over-provisioned users
    over_provisioned_count = (user_privilege['privilege_usage_gap'] >= 5).sum()
    print(f"      Severely over-provisioned (gap ≥5): {over_provisioned_count} users ({over_provisioned_count/len(user_privilege)*100:.1f}%)")
    
    return df


def calculate_privilege_usage_ratio(df):
    """
    Feature 11: Privilege-Usage Ratio
    
    Definition: Percentage of assigned privileges that are actively used
    
    Business Logic:
    - Well-aligned users: 70-95% usage ratio
    - Over-provisioned users: <50% usage ratio (GOVERNANCE RISK)
    - Perfect alignment: 100% (uses everything assigned)
    
    Formula: (actively_used / assigned) × 100
    
    Interpretation:
    - 90%: User uses 9 out of 10 assigned resources (good alignment)
    - 30%: User uses 3 out of 10 assigned resources (severe over-provisioning)
    
    Args:
        df: DataFrame with privilege columns
    
    Returns:
        DataFrame with 'privilege_usage_ratio' column added
    """
    print("  [2/3] Calculating privilege_usage_ratio...")
    
    user_privilege = df.groupby('user_id').agg({
        'assigned_resource_count': 'first',
        'actively_used_resource_count': 'first'
    })
    
    # Calculate usage ratio (avoid division by zero)
    user_privilege['privilege_usage_ratio'] = (
        user_privilege['actively_used_resource_count'] / 
        user_privilege['assigned_resource_count'].replace(0, 1)  # Avoid div by zero
    ) * 100
    
    df['privilege_usage_ratio'] = df['user_id'].map(user_privilege['privilege_usage_ratio'])
    
    print(f"      Ratio Range: [{df['privilege_usage_ratio'].min():.1f}%, {df['privilege_usage_ratio'].max():.1f}%]")
    print(f"      Mean Ratio: {df['privilege_usage_ratio'].mean():.1f}%")
    
    # Identify alignment categories
    well_aligned = (user_privilege['privilege_usage_ratio'] >= 70).sum()
    over_provisioned = (user_privilege['privilege_usage_ratio'] < 50).sum()
    
    print(f"      Well-aligned (≥70%): {well_aligned} users ({well_aligned/len(user_privilege)*100:.1f}%)")
    print(f"      Over-provisioned (<50%): {over_provisioned} users ({over_provisioned/len(user_privilege)*100:.1f}%)")
    
    return df


def calculate_resource_access_concentration(df):
    """
    Feature 12: Resource Access Concentration
    
    Definition: Coefficient of variation in resource access frequency
    
    Business Logic:
    - Measures whether user spreads access evenly or concentrates on few resources
    - High concentration (high CV): Uses 1-2 resources heavily, ignores others
    - Low concentration (low CV): Uses all resources somewhat equally
    
    Calculation: CV = std(access_frequency) / mean(access_frequency)
    
    Interpretation:
    - CV = 0.5: Relatively even distribution (all resources used similarly)
    - CV = 2.0: Highly concentrated (1-2 resources dominate usage)
    
    Why This Matters:
    - Over-privileged users often have HIGH concentration
      (Use 2 resources heavily, have access to 10 unnecessarily)
    - Well-aligned users have MODERATE concentration
      (Use most of what they're assigned)
    
    Args:
        df: DataFrame with user_id and resource_type
    
    Returns:
        DataFrame with 'resource_access_concentration' column added
    """
    print("  [3/3] Calculating resource_access_concentration...")
    
    # Calculate access frequency per user per resource
    user_resource_freq = df.groupby(['user_id', 'resource_type']).size().reset_index(name='access_count')
    
    # Calculate coefficient of variation (CV) per user
    # CV = std / mean (measures relative variability)
    user_cv = user_resource_freq.groupby('user_id')['access_count'].agg(
        lambda x: x.std() / x.mean() if x.mean() > 0 else 0
    )
    
    df['resource_access_concentration'] = df['user_id'].map(user_cv)
    
    print(f"      Range: [{df['resource_access_concentration'].min():.2f}, {df['resource_access_concentration'].max():.2f}]")
    print(f"      Mean: {df['resource_access_concentration'].mean():.2f}")
    
    return df


def build_all_privilege_intelligence_features(df):
    """
    Build all 3 privilege-usage intelligence features.
    
    🎯 THIS IS WHAT MAKES YOUR PROJECT UNIQUE
    
    Traditional monitoring: "Did user access system X?" (yes/no)
    Your approach: "Should user even have access to system X?" (intelligence)
    
    Args:
        df: DataFrame with privilege assignment columns
    
    Returns:
        DataFrame with all 3 privilege intelligence features
    """
    print("\n[SECTION 5] Building Privilege-Usage Intelligence Features...")
    print("  🎯 THIS IS THE CORE DIFFERENTIATOR OF YOUR PROJECT")
    print("  Traditional systems check IF access is permitted")
    print("  Your system checks if permitted access is NEEDED")
    print()
    
    df = calculate_privilege_usage_gap(df)
    df = calculate_privilege_usage_ratio(df)
    df = calculate_resource_access_concentration(df)
    
    print("  ✓ All 3 privilege intelligence features created\n")
    
    return df