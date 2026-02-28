"""
Stability/Drift Feature Engineering
---
Behavioral consistency metrics that detect anomalies and drift over time
"""

import pandas as pd
import numpy as np


def calculate_weekly_access_change(df):
    """
    Feature 8: Weekly Access Change (Behavioral Stability)
    
    Definition: Standard deviation of week-over-week access volume changes
    
    Business Logic:
    - Stable users → low week-to-week variation
    - Drifting users → increasing trend over time
    - Volatile users → large unpredictable swings
    
    Args:
        df: DataFrame with columns ['user_id', 'week', 'access_volume']
    
    Returns:
        DataFrame with 'weekly_access_change' column added
    """
    print("  [1/2] Calculating weekly_access_change...")
    
    # Step 1: Aggregate weekly access per user
    weekly_usage = df.groupby(['user_id', 'week'])['access_volume'].sum().reset_index()
    
    # Step 2: Calculate week-over-week difference
    weekly_usage['weekly_diff'] = weekly_usage.groupby('user_id')['access_volume'].diff()
    
    # Step 3: Calculate std of weekly changes per user
    weekly_instability = weekly_usage.groupby('user_id')['weekly_diff'].std().fillna(0)
    
    df['weekly_access_change'] = df['user_id'].map(weekly_instability)
    
    print(f"      Range: [{df['weekly_access_change'].min():.2f}, {df['weekly_access_change'].max():.2f}]")
    print(f"      Mean: {df['weekly_access_change'].mean():.2f}, Std: {df['weekly_access_change'].std():.2f}")
    
    return df


def calculate_access_spike_score(df):
    """
    Feature 9: Access Spike Score
    
    Definition: Percentage of events where access_volume exceeds user's personal baseline +2σ
    
    Business Logic:
    - Captures sudden abnormal bursts
    - Spikes may indicate automation, data harvesting, or compromised account
    - Occasional spikes normal; frequent spikes are governance risk
    
    Args:
        df: DataFrame with 'access_volume' column
    
    Returns:
        DataFrame with 'access_spike_score' column added
    """
    print("  [2/2] Calculating access_spike_score...")
    
    # Step 1: Calculate each user's baseline (mean + 2*std)
    user_stats = df.groupby('user_id')['access_volume'].agg(['mean', 'std']).reset_index()
    user_stats['spike_threshold'] = user_stats['mean'] + (2 * user_stats['std'])
    
    # Step 2: Merge threshold back
    df = df.merge(user_stats[['user_id', 'spike_threshold']], on='user_id', how='left')
    
    # Step 3: Flag spike events
    df['is_spike'] = (df['access_volume'] > df['spike_threshold']).astype(int)
    
    # Step 4: Calculate spike ratio per user
    spike_ratio = df.groupby('user_id')['is_spike'].mean() * 100
    df['access_spike_score'] = df['user_id'].map(spike_ratio)
    
    # Cleanup temporary columns
    df = df.drop(columns=['spike_threshold', 'is_spike'])
    
    print(f"      Range: [{df['access_spike_score'].min():.2f}%, {df['access_spike_score'].max():.2f}%]")
    print(f"      Mean: {df['access_spike_score'].mean():.2f}%, Std: {df['access_spike_score'].std():.2f}%")
    
    return df


def build_all_stability_features(df):
    """
    Build all 2 stability features in sequence.
    
    Args:
        df: DataFrame with temporal and access data
    
    Returns:
        DataFrame with all stability features added
    """
    print("\n[SECTION 4] Building Stability/Drift Features...")
    
    df = calculate_weekly_access_change(df)
    df = calculate_access_spike_score(df)
    
    print("  ✓ All 2 stability features created\n")
    
    return df