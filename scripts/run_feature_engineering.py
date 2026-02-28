"""
Feature Engineering Script for Echelon Project
---
UPDATED: Now includes privilege-usage intelligence features

Orchestrates all feature engineering steps:
1. Load clean data
2. Build behavioral features (4)
3. Build temporal features (3)
4. Build stability features (2)
5. Build privilege intelligence features (3) ← NEW
6. Calculate role-based z-scores (12) ← UPDATED from 9
7. Calculate governance risk score
8. Add risk categories
9. Validate and save

Input: data/processed/cleaned_access_logs.csv
Output: data/processed/feature_engineered.csv
"""

from pathlib import Path
import pandas as pd
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.features.behavioral_features import build_all_behavioral_features
from src.features.temporal_features import build_all_temporal_features
from src.features.stability_features import build_all_stability_features
from src.features.privilege_intelligence import build_all_privilege_intelligence_features  # NEW
from src.features.risk_scoring import (
    calculate_role_based_zscores,
    calculate_governance_risk_score,
    add_risk_categories
)


def load_and_prepare_data(filepath):
    """Load cleaned data and extract temporal components."""
    print("=" * 80)
    print(" ECHELON - FEATURE ENGINEERING PIPELINE")
    print(" UPDATED: Now includes privilege-usage intelligence")
    print("=" * 80)
    print()
    
    print(f"[SECTION 1] Loading and Preparing Data...")
    print(f"  Loading from: {filepath}")
    
    df = pd.read_csv(filepath)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Extract temporal components
    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek  # 0=Monday, 6=Sunday
    df['week'] = df['timestamp'].dt.isocalendar().week
    df['month'] = df['timestamp'].dt.month
    df['date'] = df['timestamp'].dt.date
    
    print(f"  Loaded {len(df):,} clean records")
    print(f"  Users: {df['user_id'].nunique()}")
    print(f"  Roles: {df['role'].nunique()}")
    print(f"  Date range: {df['timestamp'].min().date()} to {df['timestamp'].max().date()}")
    
    # Verify privilege columns exist
    required_cols = ['assigned_resource_count', 'actively_used_resource_count']
    for col in required_cols:
        if col not in df.columns:
            print(f"\n  ❌ ERROR: Missing column '{col}'")
            print(f"     This column is required for privilege-usage intelligence features.")
            sys.exit(1)
    
    print(f"  ✓ Privilege intelligence columns present")
    print()
    
    return df


def validate_features(df):
    """Validate that all features were created successfully."""
    print("\n[SECTION 9] Validating Feature Engineering...")
    print()
    
    # UPDATED: Now 12 features (was 9)
    required_features = [
        # Behavioral (4)
        'avg_daily_access', 'export_ratio', 'unique_resources', 'avg_session_duration',
        # Temporal (3)
        'night_access_pct', 'weekend_activity_ratio', 'access_time_variance',
        # Stability (2)
        'weekly_access_change', 'access_spike_score',
        # NEW: Privilege Intelligence (3)
        'privilege_usage_gap', 'privilege_usage_ratio', 'resource_access_concentration'
    ]
    
    issues = []
    
    # Check all features exist
    for feature in required_features:
        if feature not in df.columns:
            issues.append(f"Missing feature: {feature}")
        else:
            print(f"  ✓ {feature}")
        
        zscore_col = f'{feature}_zscore'
        if zscore_col not in df.columns:
            issues.append(f"Missing z-score: {zscore_col}")
        else:
            print(f"  ✓ {zscore_col}")
    
    print()
    
    # Check risk scores
    if 'governance_risk_score' not in df.columns:
        issues.append("Missing governance_risk_score")
    else:
        min_risk = df['governance_risk_score'].min()
        max_risk = df['governance_risk_score'].max()
        
        if min_risk < 0 or max_risk > 100:
            issues.append(f"Risk score out of range: [{min_risk:.2f}, {max_risk:.2f}]")
        else:
            print(f"  ✓ governance_risk_score in valid range [0, 100]")
    
    # Check risk categories
    if 'risk_category' not in df.columns:
        issues.append("Missing risk_category")
    else:
        print(f"  ✓ risk_category assigned")
    
    # Check all users have risk scores
    user_risk_count = df.groupby('user_id')['governance_risk_score'].count()
    if user_risk_count.min() == 0:
        issues.append("Some users missing risk scores")
    else:
        print(f"  ✓ All {df['user_id'].nunique()} users have risk scores")
    
    print()
    
    if issues:
        print("  ⚠️  VALIDATION ISSUES:")
        for issue in issues:
            print(f"    - {issue}")
        return False
    else:
        print("  ✅ All validation checks passed!")
        return True


def save_engineered_data(df, output_path):
    """Save feature-engineered data."""
    print(f"\n[SECTION 10] Saving Feature-Engineered Data...")
    print(f"  Output: {output_path}")
    
    # Ensure directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save
    df.to_csv(output_path, index=False)
    
    print(f"  Saved {len(df):,} records")
    print(f"  Columns: {len(df.columns)}")
    print(f"  File size: {output_path.stat().st_size / 1024:.1f} KB")
    print()


def generate_feature_summary(df, output_dir):
    """Generate feature engineering summary report."""
    
    # Get user-level summary
    user_risk = df.groupby('user_id').agg({
        'role': 'first',
        'governance_risk_score': 'mean',
        'risk_category': lambda x: x.mode()[0] if not x.mode().empty else 'Low'
    }).reset_index()
    
    summary = f"""# Feature Engineering Summary

## Input
- **Cleaned records**: {len(df):,}
- **Users**: {df['user_id'].nunique()}
- **Roles**: {df['role'].nunique()}
- **Time span**: {df['timestamp'].min().date()} to {df['timestamp'].max().date()}

## Features Created (UPDATED: 12 total)

### Behavioral Features (4)
1. **avg_daily_access**: Average resources accessed per day
2. **export_ratio**: Percentage of export actions
3. **unique_resources**: Count of distinct resources accessed
4. **avg_session_duration**: Mean session length (minutes)

### Temporal Features (3)
5. **night_access_pct**: Percentage of access 10 PM - 6 AM
6. **weekend_activity_ratio**: Percentage of weekend access
7. **access_time_variance**: Variance in access hours

### Stability Features (2)
8. **weekly_access_change**: Week-over-week volatility (std dev of changes)
9. **access_spike_score**: Percentage of events exceeding baseline +2σ

### 🎯 Privilege-Usage Intelligence Features (3) - NEW
10. **privilege_usage_gap**: Unused assigned privileges (assigned - used)
11. **privilege_usage_ratio**: Percentage of assigned privileges actually used
12. **resource_access_concentration**: Coefficient of variation in resource usage

### Z-Score Features (12)
Each behavioral feature normalized against role-specific peers

### Risk Scores (2)
- **governance_risk_score**: 0-100 weighted ensemble score
- **risk_category**: Low (0-30), Medium (31-60), High (61-100)

## Risk Score Weights (UPDATED)

### 🎯 Privilege Intelligence (30% - Core Differentiator)
- privilege_usage_gap_zscore: 15%
- privilege_usage_ratio_zscore: 10%
- resource_access_concentration_zscore: 5%

### Behavioral & Temporal (55% - Supporting Evidence)
- export_ratio_zscore: 15%
- avg_daily_access_zscore: 12%
- unique_resources_zscore: 10%
- night_access_pct_zscore: 8%
- avg_session_duration_zscore: 6%
- weekend_activity_ratio_zscore: 4%

### Stability & Drift (15% - Temporal Monitoring)
- access_time_variance_zscore: 5%
- weekly_access_change_zscore: 5%
- access_spike_score_zscore: 5%

## Risk Distribution
- **Low Risk**: {(user_risk['risk_category']=='Low').sum()} users ({(user_risk['risk_category']=='Low').sum()/len(user_risk)*100:.1f}%)
- **Medium Risk**: {(user_risk['risk_category']=='Medium').sum()} users ({(user_risk['risk_category']=='Medium').sum()/len(user_risk)*100:.1f}%)
- **High Risk**: {(user_risk['risk_category']=='High').sum()} users ({(user_risk['risk_category']=='High').sum()/len(user_risk)*100:.1f}%)

## Privilege-Usage Intelligence Summary
- **Mean privilege gap**: {df['privilege_usage_gap'].mean():.2f} unused resources per user
- **Mean usage ratio**: {df['privilege_usage_ratio'].mean():.1f}% of assigned privileges used
- **Over-provisioned users (<50% usage)**: {(df.groupby('user_id')['privilege_usage_ratio'].first() < 50).sum()} users

## Feature Statistics

| Feature | Mean | Std | Min | Max |
|---------|------|-----|-----|-----|
| avg_daily_access | {df['avg_daily_access'].mean():.2f} | {df['avg_daily_access'].std():.2f} | {df['avg_daily_access'].min():.2f} | {df['avg_daily_access'].max():.2f} |
| export_ratio | {df['export_ratio'].mean():.2f}% | {df['export_ratio'].std():.2f}% | {df['export_ratio'].min():.2f}% | {df['export_ratio'].max():.2f}% |
| privilege_usage_gap | {df['privilege_usage_gap'].mean():.2f} | {df['privilege_usage_gap'].std():.2f} | {df['privilege_usage_gap'].min():.0f} | {df['privilege_usage_gap'].max():.0f} |
| privilege_usage_ratio | {df['privilege_usage_ratio'].mean():.1f}% | {df['privilege_usage_ratio'].std():.1f}% | {df['privilege_usage_ratio'].min():.1f}% | {df['privilege_usage_ratio'].max():.1f}% |
| governance_risk_score | {df['governance_risk_score'].mean():.2f} | {df['governance_risk_score'].std():.2f} | {df['governance_risk_score'].min():.2f} | {df['governance_risk_score'].max():.2f} |

## Output
- **File**: `data/processed/feature_engineered.csv`
- **Shape**: {df.shape}
- **Ready for**: Statistical analysis, visualization, ML modeling

## Next Steps
1. Run `notebooks/04_statistical_analysis.ipynb` for insights
2. Run `notebooks/05_visualization.ipynb` for visual analytics
3. Generate final governance report

## What Makes This Different
Traditional monitoring asks: "Did the user access the system?"
This system asks: "Should the user even have access to that system?"

The privilege-usage intelligence features are the CORE DIFFERENTIATOR.
"""
    
    summary_path = output_dir / 'feature_engineering_summary.md'
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write(summary)
    
    print(f"  Summary report saved: {summary_path}")


def main():
    """Main execution pipeline."""
    
    # File paths
    clean_file = Path('data/processed/cleaned_access_logs.csv')
    output_file = Path('data/processed/feature_engineered.csv')
    report_dir = Path('outputs/reports')
    
    # Check input exists
    if not clean_file.exists():
        print(f"ERROR: Cleaned data not found: {clean_file}")
        print("   Run run_cleaning.py first")
        sys.exit(1)
    
    # Load and prepare
    df = load_and_prepare_data(clean_file)
    
    # Build features
    df = build_all_behavioral_features(df)
    df = build_all_temporal_features(df)
    df = build_all_stability_features(df)
    df = build_all_privilege_intelligence_features(df)  # NEW
    
    # Calculate risk scores
    df = calculate_role_based_zscores(df)
    df = calculate_governance_risk_score(df)
    df = add_risk_categories(df)
    
    # Validate
    validation_passed = validate_features(df)
    
    if not validation_passed:
        print("\n⚠️  WARNING: Validation issues detected!")
        print("   Review issues before proceeding.\n")
    
    # Save
    save_engineered_data(df, output_file)
    
    # Generate summary
    generate_feature_summary(df, report_dir)
    
    # Final summary
    print("=" * 80)
    print(" FEATURE ENGINEERING COMPLETE")
    print("=" * 80)
    print()
    print(f"Input:  {df['user_id'].nunique()} users, {len(df):,} records")
    print(f"Output: {len(df.columns)} features created")
    print(f"  - 4 behavioral features")
    print(f"  - 3 temporal features")
    print(f"  - 2 stability features")
    print(f"  - 3 privilege intelligence features (NEW)")
    print(f"  - 12 role-normalized z-scores (UPDATED)")
    print(f"  - 1 governance risk score (0-100)")
    print(f"  - 1 risk category (Low/Medium/High)")
    print()
    print("🎯 Privilege-Usage Intelligence:")
    user_gap_mean = df.groupby('user_id')['privilege_usage_gap'].first().mean()
    user_ratio_mean = df.groupby('user_id')['privilege_usage_ratio'].first().mean()
    print(f"  - Mean privilege gap: {user_gap_mean:.2f} unused resources")
    print(f"  - Mean usage ratio: {user_ratio_mean:.1f}% of assigned privileges")
    print()
    print("Next steps:")
    print("  1. Run notebooks/04_statistical_analysis.ipynb")
    print("  2. Run notebooks/05_visualization.ipynb")
    print()


if __name__ == "__main__":
    main()