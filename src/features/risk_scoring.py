"""
Risk Scoring Module
---
Role-aware z-score normalization and governance risk score calculation

UPDATED: Now includes 12 features (9 original + 3 privilege intelligence)
"""

import pandas as pd
import numpy as np


def calculate_role_based_zscores(df):
    """
    Calculate role-normalized z-scores for all 12 behavioral features.
    
    UPDATED: Now includes privilege-usage intelligence features
    
    Core Concept:
    - DB_Admins naturally have different patterns than HR_Admins
    - Comparing a DB_Admin to ALL users is meaningless
    - We must compare each user to their ROLE-SPECIFIC peers
    
    For EACH behavioral feature:
    1. Group by role
    2. Calculate role-specific mean and std
    3. Compute z-score: (user_value - role_mean) / role_std
    
    Interpretation:
    - Z-score ~0: User is average for their role
    - Z-score +2: User is 2 std above role average (unusual)
    - Z-score -2: User is 2 std below role average (unusual)
    - Z-score >3: Statistical outlier within role
    
    Args:
        df: DataFrame with all 12 behavioral features
    
    Returns:
        DataFrame with 12 additional z-score columns
    """
    print("\n[SECTION 6] Calculating Role-Based Z-Scores...")
    print("  (This is what makes your project resume-heavy)")
    print()
    
    # UPDATED: Now 12 features (was 9)
    features_to_normalize = [
        # Original behavioral features (4)
        'avg_daily_access',
        'export_ratio',
        'unique_resources',
        'avg_session_duration',
        
        # Temporal features (3)
        'night_access_pct',
        'weekend_activity_ratio',
        'access_time_variance',
        
        # Stability features (2)
        'weekly_access_change',
        'access_spike_score',
        
        # NEW: Privilege intelligence features (3)
        'privilege_usage_gap',
        'privilege_usage_ratio',
        'resource_access_concentration'
    ]
    
    print(f"  Normalizing {len(features_to_normalize)} features against role-specific peers...")
    print()
    
    # Calculate z-scores per role
    for feature in features_to_normalize:
        # Group by role, calculate stats
        role_stats = df.groupby('role')[feature].agg(['mean', 'std']).reset_index()
        role_stats.columns = ['role', f'{feature}_role_mean', f'{feature}_role_std']
        
        # Merge back
        df = df.merge(role_stats, on='role', how='left')
        
        # Calculate z-score (handle zero std)
        df[f'{feature}_zscore'] = (
            (df[feature] - df[f'{feature}_role_mean']) / df[f'{feature}_role_std']
        ).fillna(0)  # Handle zero std (all users same value)
        
        # Cleanup temporary columns
        df = df.drop(columns=[f'{feature}_role_mean', f'{feature}_role_std'])
        
        print(f"  ✓ Z-score calculated: {feature}_zscore")
    
    print(f"\n  ✓ All {len(features_to_normalize)} z-scores calculated\n")
    
    return df


def calculate_governance_risk_score(df):
    """
    Calculate weighted governance risk score (0-100 scale).
    
    UPDATED: New weight distribution emphasizing privilege-usage intelligence
    
    Objective: Combine 12 z-scores into single interpretable risk score
    
    Weight Philosophy:
    - Privilege Intelligence (30%): Core differentiator
    - Behavioral & Temporal (55%): Supporting indicators
    - Stability (15%): Drift detection
    
    Args:
        df: DataFrame with all 12 z-score columns
    
    Returns:
        DataFrame with 'governance_risk_score' and 'raw_risk_score' columns
    """
    print("[SECTION 7] Calculating Governance Risk Score...")
    print()
    
    # UPDATED: New weight structure prioritizing privilege intelligence
    risk_weights = {
        # PRIORITY 1: Privilege-Usage Intelligence (30% total)
        'privilege_usage_gap_zscore': 0.15,           # Highest single weight
        'privilege_usage_ratio_zscore': 0.10,         # Second highest
        'resource_access_concentration_zscore': 0.05, # Supporting
        
        # PRIORITY 2: Behavioral & Temporal Risk (55% total)
        'export_ratio_zscore': 0.15,                  # Data exfiltration risk
        'avg_daily_access_zscore': 0.12,              # Volume indicator
        'unique_resources_zscore': 0.10,              # Breadth of access
        'night_access_pct_zscore': 0.08,              # Unusual timing
        'avg_session_duration_zscore': 0.06,          # Time depth
        'weekend_activity_ratio_zscore': 0.04,        # Off-hours activity
        
        # PRIORITY 3: Stability & Drift (15% total)
        'access_time_variance_zscore': 0.05,          # Erratic behavior
        'weekly_access_change_zscore': 0.05,          # Behavioral instability
        'access_spike_score_zscore': 0.05             # Abnormal bursts
    }
    
    # Verify weights sum to 1.0
    total_weight = sum(risk_weights.values())
    assert abs(total_weight - 1.0) < 0.001, f"Weights must sum to 1.0 (got {total_weight})"
    
    print("  Risk Weight Distribution:")
    print("  " + "=" * 70)
    print("  🎯 PRIVILEGE INTELLIGENCE (30% - Core Differentiator)")
    print(f"    privilege_usage_gap             {risk_weights['privilege_usage_gap_zscore']*100:5.1f}%  {'█'*15}")
    print(f"    privilege_usage_ratio           {risk_weights['privilege_usage_ratio_zscore']*100:5.1f}%  {'█'*10}")
    print(f"    resource_access_concentration   {risk_weights['resource_access_concentration_zscore']*100:5.1f}%  {'█'*5}")
    print()
    print("  📊 BEHAVIORAL & TEMPORAL (55% - Supporting Evidence)")
    print(f"    export_ratio                    {risk_weights['export_ratio_zscore']*100:5.1f}%  {'█'*15}")
    print(f"    avg_daily_access                {risk_weights['avg_daily_access_zscore']*100:5.1f}%  {'█'*12}")
    print(f"    unique_resources                {risk_weights['unique_resources_zscore']*100:5.1f}%  {'█'*10}")
    print(f"    night_access_pct                {risk_weights['night_access_pct_zscore']*100:5.1f}%  {'█'*8}")
    print(f"    avg_session_duration            {risk_weights['avg_session_duration_zscore']*100:5.1f}%  {'█'*6}")
    print(f"    weekend_activity_ratio          {risk_weights['weekend_activity_ratio_zscore']*100:5.1f}%  {'█'*4}")
    print()
    print("  📈 STABILITY & DRIFT (15% - Temporal Monitoring)")
    print(f"    access_time_variance            {risk_weights['access_time_variance_zscore']*100:5.1f}%  {'█'*5}")
    print(f"    weekly_access_change            {risk_weights['weekly_access_change_zscore']*100:5.1f}%  {'█'*5}")
    print(f"    access_spike_score              {risk_weights['access_spike_score_zscore']*100:5.1f}%  {'█'*5}")
    print()
    
    # Calculate weighted sum of z-scores
    df['raw_risk_score'] = sum(
        df[feature] * weight 
        for feature, weight in risk_weights.items()
    )
    
    print(f"  Raw risk scores calculated:")
    print(f"    Range: [{df['raw_risk_score'].min():.4f}, {df['raw_risk_score'].max():.4f}]")
    print()
    
    # Min-max normalization to 0-100 scale
    min_score = df['raw_risk_score'].min()
    max_score = df['raw_risk_score'].max()
    
    df['governance_risk_score'] = (
        (df['raw_risk_score'] - min_score) / (max_score - min_score)
    ) * 100
    
    # Clip to 0-100 (handle any edge cases)
    df['governance_risk_score'] = df['governance_risk_score'].clip(0, 100)
    
    print(f"  Normalized to 0-100 scale:")
    print(f"    Range: [{df['governance_risk_score'].min():.2f}, {df['governance_risk_score'].max():.2f}]")
    print(f"    Mean: {df['governance_risk_score'].mean():.2f}")
    print(f"    Median: {df['governance_risk_score'].median():.2f}")
    print(f"    Std Dev: {df['governance_risk_score'].std():.2f}")
    print()
    
    return df


def categorize_risk(score):
    """
    Categorize risk score into Low/Medium/High.
    
    Thresholds:
    - Low: 0-30
    - Medium: 31-60
    - High: 61-100
    """
    if score <= 30:
        return 'Low'
    elif score <= 60:
        return 'Medium'
    else:
        return 'High'


def add_risk_categories(df):
    """
    Add risk category labels to dataframe.
    
    Args:
        df: DataFrame with 'governance_risk_score' column
    
    Returns:
        DataFrame with 'risk_category' column added
    """
    print("[SECTION 8] Categorizing Risk Levels...")
    
    df['risk_category'] = df['governance_risk_score'].apply(categorize_risk)
    
    # Count distribution
    risk_counts = df.groupby('user_id').agg({
        'risk_category': lambda x: x.mode()[0] if not x.mode().empty else 'Low'
    }).reset_index()
    
    category_counts = risk_counts['risk_category'].value_counts()
    
    print("  Risk Distribution:")
    for category in ['Low', 'Medium', 'High']:
        count = category_counts.get(category, 0)
        pct = count / len(risk_counts) * 100
        print(f"    {category:8s}: {count:3d} users ({pct:5.1f}%)")
    print()
    
    return df