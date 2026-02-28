"""
Temporal Feature Engineering
---
Time-based patterns that reveal unusual access timing
"""

import pandas as pd
import numpy as np


def calculate_night_access_pct(df):
    """
    Feature 5: Night Access Percentage
    
    Definition: Percentage of access occurring between 10 PM - 6 AM
    
    Business Logic:
    - Most roles work 9-5; night access is unusual
    - Legitimate for Security_Admin, Cloud_Admin (on-call)
    - High night access for DB_Admin or HR_Admin is anomalous
    
    Args:
        df: DataFrame with 'hour' column (0-23)
    
    Returns:
        DataFrame with 'night_access_pct' column added
    """
    print("  [1/3] Calculating night_access_pct...")
    
    # Flag night access (10 PM = 22:00, 6 AM = 06:00)
    df['is_night'] = ((df['hour'] >= 22) | (df['hour'] <= 6)).astype(int)
    
    # Calculate percentage per user
    night_pct = df.groupby('user_id')['is_night'].mean() * 100
    df['night_access_pct'] = df['user_id'].map(night_pct)
    
    print(f"      Range: [{df['night_access_pct'].min():.2f}%, {df['night_access_pct'].max():.2f}%]")
    print(f"      Mean: {df['night_access_pct'].mean():.2f}%, Std: {df['night_access_pct'].std():.2f}%")
    
    return df


def calculate_weekend_activity_ratio(df):
    """
    Feature 6: Weekend Activity Ratio
    
    Definition: Percentage of access on Saturday/Sunday
    
    Business Logic:
    - Normal enterprise work is Mon-Fri
    - Weekend work legitimate for on-call roles
    - High weekend activity for non-on-call roles is unusual
    
    Args:
        df: DataFrame with 'day_of_week' column (0=Mon, 6=Sun)
    
    Returns:
        DataFrame with 'weekend_activity_ratio' column added
    """
    print("  [2/3] Calculating weekend_activity_ratio...")
    
    # Flag weekend (Saturday=5, Sunday=6)
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
    
    # Calculate percentage per user
    weekend_ratio = df.groupby('user_id')['is_weekend'].mean() * 100
    df['weekend_activity_ratio'] = df['user_id'].map(weekend_ratio)
    
    print(f"      Range: [{df['weekend_activity_ratio'].min():.2f}%, {df['weekend_activity_ratio'].max():.2f}%]")
    print(f"      Mean: {df['weekend_activity_ratio'].mean():.2f}%, Std: {df['weekend_activity_ratio'].std():.2f}%")
    
    return df


def calculate_access_time_variance(df):
    """
    Feature 7: Access Time Variance
    
    Definition: Variance in hours of access across user's events
    
    Business Logic:
    - Consistent workers have low variance (e.g., always 9-5)
    - Erratic schedules have high variance
    - High variance may indicate automation/scripting or unusual behavior
    
    Args:
        df: DataFrame with 'hour' column
    
    Returns:
        DataFrame with 'access_time_variance' column added
    """
    print("  [3/3] Calculating access_time_variance...")
    
    time_variance = df.groupby('user_id')['hour'].var()
    df['access_time_variance'] = df['user_id'].map(time_variance)
    
    # Handle users with only one access (variance = NaN)
    df['access_time_variance'] = df['access_time_variance'].fillna(0)
    
    print(f"      Range: [{df['access_time_variance'].min():.2f}, {df['access_time_variance'].max():.2f}]")
    print(f"      Mean: {df['access_time_variance'].mean():.2f}, Std: {df['access_time_variance'].std():.2f}")
    
    return df


def build_all_temporal_features(df):
    """
    Build all 3 temporal features in sequence.
    
    Args:
        df: DataFrame with temporal columns (hour, day_of_week)
    
    Returns:
        DataFrame with all temporal features added
    """
    print("\n[SECTION 3] Building Temporal Features...")
    
    df = calculate_night_access_pct(df)
    df = calculate_weekend_activity_ratio(df)
    df = calculate_access_time_variance(df)
    
    print("  ✓ All 3 temporal features created\n")
    
    return df