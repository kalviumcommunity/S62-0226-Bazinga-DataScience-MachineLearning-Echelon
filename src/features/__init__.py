"""
Feature Engineering Module for Echelon Project
---
UPDATED: Now includes privilege-usage intelligence features
"""

from .behavioral_features import (
    calculate_avg_daily_access,
    calculate_export_ratio,
    calculate_unique_resources,
    calculate_avg_session_duration,
    build_all_behavioral_features
)

from .temporal_features import (
    calculate_night_access_pct,
    calculate_weekend_activity_ratio,
    calculate_access_time_variance,
    build_all_temporal_features
)

from .stability_features import (
    calculate_weekly_access_change,
    calculate_access_spike_score,
    build_all_stability_features
)

from .privilege_intelligence import (
    calculate_privilege_usage_gap,
    calculate_privilege_usage_ratio,
    calculate_resource_access_concentration,
    build_all_privilege_intelligence_features
)

from .risk_scoring import (
    calculate_role_based_zscores,
    calculate_governance_risk_score,
    categorize_risk,
    add_risk_categories
)

__all__ = [
    # Behavioral
    'calculate_avg_daily_access',
    'calculate_export_ratio',
    'calculate_unique_resources',
    'calculate_avg_session_duration',
    'build_all_behavioral_features',
    
    # Temporal
    'calculate_night_access_pct',
    'calculate_weekend_activity_ratio',
    'calculate_access_time_variance',
    'build_all_temporal_features',
    
    # Stability
    'calculate_weekly_access_change',
    'calculate_access_spike_score',
    'build_all_stability_features',
    
    # Privilege Intelligence (NEW)
    'calculate_privilege_usage_gap',
    'calculate_privilege_usage_ratio',
    'calculate_resource_access_concentration',
    'build_all_privilege_intelligence_features',
    
    # Risk Scoring
    'calculate_role_based_zscores',
    'calculate_governance_risk_score',
    'categorize_risk',
    'add_risk_categories'
]