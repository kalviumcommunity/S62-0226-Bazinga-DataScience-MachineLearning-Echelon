# Feature Engineering Summary

## Input
- **Cleaned records**: 12,665
- **Users**: 100
- **Roles**: 5
- **Time span**: 2024-01-01 to 2024-12-30

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
- **Low Risk**: 25 users (25.0%)
- **Medium Risk**: 60 users (60.0%)
- **High Risk**: 15 users (15.0%)

## Privilege-Usage Intelligence Summary
- **Mean privilege gap**: 3.02 unused resources per user
- **Mean usage ratio**: 64.4% of assigned privileges used
- **Over-provisioned users (<50% usage)**: 15 users

## Feature Statistics

| Feature | Mean | Std | Min | Max |
|---------|------|-----|-----|-----|
| avg_daily_access | 20.74 | 7.69 | 8.52 | 43.05 |
| export_ratio | 4.11% | 4.58% | 0.00% | 27.50% |
| privilege_usage_gap | 3.02 | 1.80 | 1 | 9 |
| privilege_usage_ratio | 64.4% | 17.3% | 18.2% | 91.7% |
| governance_risk_score | 44.91 | 18.27 | 0.00 | 100.00 |

## Output
- **File**: `data/processed/feature_engineered.csv`
- **Shape**: (12665, 44)
- **Ready for**: Statistical analysis, visualization, ML modeling

## Next Steps
1. Run `notebooks/04_statistical_analysis.ipynb` for insights
2. Run `notebooks/05_visualization.ipynb` for visual analytics
3. Generate final governance report

## What Makes This Different
Traditional monitoring asks: "Did the user access the system?"
This system asks: "Should the user even have access to that system?"

The privilege-usage intelligence features are the CORE DIFFERENTIATOR.
