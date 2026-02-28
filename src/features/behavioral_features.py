"""
Behavioral Feature Engineering
---
Core access behavior metrics that measure privilege scope and usage patterns
"""

import pandas as pd
import numpy as np


def calculate_avg_daily_access(df):
    """
    Feature 1: Average Daily Access Volume
    
    Definition: Average number of resources accessed per day by each user
    
    Business Logic:
    - Higher values indicate more active/privileged users
    - Compared against role peers to identify over-access
    - Persistent high values may indicate over-privileging
    
    Args:
        df: DataFrame with columns ['user_id', 'date', 'access_volume']
    
    Returns:
        DataFrame with 'avg_daily_access' column added
    """
    print("  [1/4] Calculating avg_daily_access...")
    
    # Step 1: Calculate total access per user per day
    daily_access = df.groupby(['user_id', 'date'])['access_volume'].sum().reset_index()
    
    # Step 2: Calculate average per user
    avg_daily_access = daily_access.groupby('user_id')['access_volume'].mean()
    
    # Step 3: Merge back to main dataframe
    df['avg_daily_access'] = df['user_id'].map(avg_daily_access)
    
    print(f"      Range: [{df['avg_daily_access'].min():.2f}, {df['avg_daily_access'].max():.2f}]")
    print(f"      Mean: {df['avg_daily_access'].mean():.2f}, Std: {df['avg_daily_access'].std():.2f}")
    
    return df


def calculate_export_ratio(df):
    """
    Feature 2: Export Action Ratio
    
    Definition: Percentage of user's actions that are 'export'
    
    Business Logic:
    - Exports are security-sensitive (data exfiltration risk)
    - Normal users have <5% export ratio
    - High export ratio (>15%) is a governance red flag
    
    Args:
        df: DataFrame with columns ['user_id', 'action']
    
    Returns:
        DataFrame with 'export_ratio' column added
    """
    print("  [2/4] Calculating export_ratio...")
    
    # Calculate percentage of 'export' actions per user
    user_actions = df.groupby('user_id')['action'].value_counts(normalize=True).unstack(fill_value=0)
    
    # Get export ratio (default to 0 if no exports)
    export_ratio = user_actions.get('export', pd.Series(0, index=user_actions.index)) * 100
    
    df['export_ratio'] = df['user_id'].map(export_ratio)
    
    print(f"      Range: [{df['export_ratio'].min():.2f}%, {df['export_ratio'].max():.2f}%]")
    print(f"      Mean: {df['export_ratio'].mean():.2f}%, Std: {df['export_ratio'].std():.2f}%")
    
    return df


def calculate_unique_resources(df):
    """
    Feature 3: Unique Resources Accessed
    
    Definition: Count of distinct resource_type values accessed by user
    
    Business Logic:
    - Measures breadth of access
    - Over-privileged users access many diverse resources
    - Principle of least privilege suggests narrow access is better
    
    Args:
        df: DataFrame with columns ['user_id', 'resource_type']
    
    Returns:
        DataFrame with 'unique_resources' column added
    """
    print("  [3/4] Calculating unique_resources...")
    
    unique_resources = df.groupby('user_id')['resource_type'].nunique()
    df['unique_resources'] = df['user_id'].map(unique_resources)
    
    print(f"      Range: [{df['unique_resources'].min()}, {df['unique_resources'].max()}]")
    print(f"      Mean: {df['unique_resources'].mean():.2f}, Std: {df['unique_resources'].std():.2f}")
    
    return df


def calculate_avg_session_duration(df):
    """
    Feature 4: Average Session Duration
    
    Definition: Mean session length (minutes) per user
    
    Business Logic:
    - Varies by role (DBAs have longer sessions than HR)
    - Unusually short/long sessions vs peers indicate anomaly
    
    Args:
        df: DataFrame with columns ['user_id', 'session_duration']
    
    Returns:
        DataFrame with 'avg_session_duration' column added
    """
    print("  [4/4] Calculating avg_session_duration...")
    
    avg_session = df.groupby('user_id')['session_duration'].mean()
    df['avg_session_duration'] = df['user_id'].map(avg_session)
    
    print(f"      Range: [{df['avg_session_duration'].min():.2f}, {df['avg_session_duration'].max():.2f}] minutes")
    print(f"      Mean: {df['avg_session_duration'].mean():.2f}, Std: {df['avg_session_duration'].std():.2f}")
    
    return df


def build_all_behavioral_features(df):
    """
    Build all 4 behavioral features in sequence.
    
    Args:
        df: DataFrame with cleaned access logs
    
    Returns:
        DataFrame with all behavioral features added
    """
    print("\n[SECTION 2] Building Behavioral Features...")
    
    df = calculate_avg_daily_access(df)
    df = calculate_export_ratio(df)
    df = calculate_unique_resources(df)
    df = calculate_avg_session_duration(df)
    
    print("  ✓ All 4 behavioral features created\n")
    
    return df