# 🎯 Echelon Project - Complete Structure & File Connections Guide

---

## 📋 TABLE OF CONTENTS
1. [Project Overview](#project-overview)
2. [Data Flow Pipeline](#data-flow-pipeline)
3. [Directory Structure Explained](#directory-structure-explained)
4. [Core Components](#core-components)
5. [File Connections & Dependencies](#file-connections--dependencies)
6. [Execution Workflow](#execution-workflow)

---

## 🏗️ PROJECT OVERVIEW

### What is Echelon?
**Echelon** is a Role-Aware Privileged Access Risk Scoring System that solves a **governance visibility problem**, not a threat detection problem.

### Core Problem
- Traditional RBAC only checks: *"Is this user allowed to access this system?"* ✓
- Echelon checks: *"Does this user NEED the access they have been granted?"* ← **KEY INNOVATION**

### Key Differentiator
Most enterprises over-privilege admins. Users accumulate permissions over time but don't use them all. Echelon identifies:
- **Behavioral deviation** (users acting differently from their role peers)
- **Privilege-usage misalignment** (users with unused permissions)
- **Temporal instability** (unusual access timing patterns)
- **Over-provisioning** (assigned >> actually used)

---

## 🔄 DATA FLOW PIPELINE

```
Raw Access Logs
    ↓
[Data Cleaning]
    ↓
Cleaned Data
    ↓
[Feature Engineering]
    ├─ Behavioral Features (how much they access)
    ├─ Temporal Features (when they access)
    ├─ Stability Features (consistency patterns)
    └─ Privilege Intelligence (used vs. assigned)
    ↓
[Feature-Engineered Data]
    ↓
[Role-Based Z-Score Normalization]
    ↓
[Risk Scoring & Classification]
    ↓
[Final Risk Dashboard & Insights]
```

### Why This Order?
1. **Clean first** → Remove noise, standardize formats
2. **Engineer features** → Convert raw logs into meaningful metrics
3. **Normalize by role** → DB_Admins have different patterns than HR_Admins
4. **Score risk** → Combine features into governance risk score
5. **Visualize** → Create dashboards for governance teams

---

## 📁 DIRECTORY STRUCTURE EXPLAINED

```
S62-0226-Bazinga-DataScience-MachineLearning-Echelon/
│
├── README.md                              # Project documentation
├── PROJECT_STRUCTURE_GUIDE.md             # ← YOU ARE HERE
│
├── data/                                  # Data storage layer
│   ├── raw/
│   │   └── enterprise_privileged_access_logs.csv  # Original unmodified data
│   └── processed/
│       ├── cleaned_access_logs.csv               # After cleaning pipeline
│       └── feature_engineered.csv               # After all features computed
│
├── src/                                   # Main code library (REUSABLE)
│   ├── __init__.py
│   ├── data/                             # Data handling
│   │   ├── __init__.py
│   │   ├── clean_data.py                 # Cleaning functions
│   │   └── validate_data.py              # Data validation
│   │
│   ├── features/                         # Feature engineering (CORE LOGIC)
│   │   ├── __init__.py
│   │   ├── behavioral_features.py        # 4 features: access volume, exports, resources, sessions
│   │   ├── temporal_features.py          # 3 features: night access, weekends, time variance
│   │   ├── stability_features.py         # 2 features: access time variance, weekly change
│   │   ├── privilege_intelligence.py     # 3 features: privilege gap, usage ratio, concentration
│   │   └── risk_scoring.py               # Z-scores and final risk calculation
│   │
│   ├── utils/                            # Shared utilities
│   │   ├── __init__.py
│   │   └── helpers.py                    # Reusable helper functions
│   │
│   └── visualization/                    # Plotting functions
│       ├── __init__.py
│       └── plots.py                      # Chart generation
│
├── scripts/                               # Executable scripts (ORCHESTRATION)
│   ├── generate_enterprise_access_logs.py # Data generation
│   ├── run_cleaning.py                   # Execute cleaning pipeline
│   ├── run_feature_engineering.py        # Execute feature engineering
│   ├── validate_generated_data.py        # Data validation checks
│   └── (These call functions from src/)
│
├── notebooks/                             # Interactive analysis & demos
│   ├── 01_data_exploration.ipynb         # EDA, understand raw data
│   ├── 02_data_cleaning.ipynb            # Cleaning demonstration
│   ├── 03_feature_engineering.ipynb      # Feature engineering walkthrough
│   ├── 04_statistical_analysis.ipynb     # Statistical insights
│   ├── 05_visualization.ipynb            # Governance dashboard
│   └── demos/                             # Educational notebooks
│       ├── 01_setup_verification.ipynb
│       ├── 02_notebook_structure_demo.ipynb
│       ├── 03_kernel_control_demo.ipynb
│       └── 04_markdown.ipynb
│
├── outputs/                               # Results & artifacts
│   ├── figures/                          # Generated plots
│   │   ├── eda_raw/                      # Exploratory analysis plots
│   │   └── feature_engineering/          # Feature distribution plots
│   ├── reports/                          # Analysis reports
│   │   ├── cleaning_summary.md           # Cleaning statistics
│   │   └── feature_engineering_summary.md# Feature statistics
│   └── (Generated during execution)
│
└── docs/                                  # Documentation
    └── images/                           # Reference images
```

---

## 🔧 CORE COMPONENTS

### 1. **src/data/clean_data.py** - Data Cleaning Pipeline
```
PURPOSE: Clean and standardize raw access logs

FUNCTIONS:
├── load_raw_data()           → Loads CSV, parses timestamps
├── handle_missing_values()   → Fill nulls with ROLE-SPECIFIC medians
├── standardize_columns()     → Rename, lowercase, standardize formats
├── remove_duplicates()       → Remove exact duplicates
├── validate_data_types()     → Ensure correct column types
└── save_cleaned_data()       → Output clean CSV

WHY ROLE-SPECIFIC FILLING?
- DB_Admin average session: 65 min
- HR_Admin average session: 35 min
- Using global median would distort patterns for each role
```

**Input:** `data/raw/enterprise_privileged_access_logs.csv`  
**Output:** `data/processed/cleaned_access_logs.csv`

---

### 2. **src/features/behavioral_features.py** - Access Behavior Metrics (4 Features)

```
🎯 MEASURES: How much and how admins access systems

FEATURE 1: avg_daily_access
├─ What: Average resources accessed per day
├─ Why: Higher = more active users
├─ Example: DB_Admin accessing 22 resources/day vs peer average of 18

FEATURE 2: export_ratio
├─ What: % of actions that are 'export' (data extraction)
├─ Why: Exports are security-sensitive
├─ Example: User doing 25% exports vs role average of 3%

FEATURE 3: unique_resources
├─ What: Count of distinct resources accessed
├─ Why: Scope of access privilege
├─ Example: User accessing 12 different databases vs average 7

FEATURE 4: avg_session_duration
├─ What: Average length of each session (in minutes)
├─ Why: Session patterns reveal legitimate vs unusual behavior
├─ Example: Cloud_Admin with 10min sessions vs average 50min
```

---

### 3. **src/features/temporal_features.py** - Timing Patterns (3 Features)

```
🎯 MEASURES: When users access systems (unusual timing)

FEATURE 5: night_access_pct
├─ What: % of access between 10 PM - 6 AM
├─ Why: Most users work 9-5, night access is anomalous
├─ Normal: DB_Admin 10%, On-call role 45%
├─ Flag: 35% night access for non-on-call role = unusual

FEATURE 6: weekend_activity_ratio
├─ What: % of access on Saturday/Sunday
├─ Why: Enterprise work is Mon-Fri
├─ Normal: 15% for everyone
├─ Flag: 45% for non-on-call role = potential overwork or abuse

FEATURE 7: access_time_variance
├─ What: Hourly access variability (std dev of hourly access)
├─ Why: Stable workers have consistent access patterns
├─ Normal: Predictable 9-5 worker = low variance
├─ Flag: Random 24x7 access = high variance = instability
```

---

### 4. **src/features/stability_features.py** - Consistency Patterns (2 Features)

```
🎯 MEASURES: How stable/consistent user behavior is

FEATURE 8: access_spike_score
├─ What: Frequency of sudden access increases
├─ Why: Spikes indicate behavioral changes
├─ Calculates: "How many times did daily access jump >2 std dev?"

FEATURE 9: weekly_access_change
├─ What: Week-to-week variability in access patterns
├─ Why: Stable users have consistent weekly patterns
├─ Normal: 5-10% week-to-week change
├─ Flag: 50% fluctuation = behavioral instability
```

---

### 5. **src/features/privilege_intelligence.py** - CORE DIFFERENTIATOR (3 Features)

```
🎯 THIS IS THE KEY INNOVATION - Privilege-Usage Alignment

FEATURE 10: privilege_usage_gap ⭐ MOST IMPORTANT
├─ What: assigned_resources - actively_used_resources
├─ Why: Identifies over-provisioned users
├─ Example: 
│   User A: Assigned 10 databases, uses 9 → Gap = 1 (good alignment)
│   User B: Assigned 10 databases, uses 3 → Gap = 7 (over-privileged!)
├─ Business Impact: User B represents 70% unused privilege = risk
│
├─ Traditional RBAC: "User B has permission" ✓ (passes check)
└─ Echelon: "User B has permission but doesn't need it" ⚠️ (flag)

FEATURE 11: privilege_usage_ratio
├─ What: (actively_used / assigned) * 100
├─ Why: Inverse perspective on gap
├─ Example: 
│   User A: 9/10 = 90% (efficient privilege use)
│   User B: 3/10 = 30% (wasteful privilege use)

FEATURE 12: resource_access_concentration
├─ What: Herfindahl Index - how concentrated is access across resources
├─ Why: Identifies users leveraging narrow privilege sets
├─ Example:
│   User A: Uses all 8 assigned resources equally = low concentration
│   User B: Uses only 1 of 8 resources 95% of time = high concentration
```

---

### 6. **src/features/risk_scoring.py** - Risk Calculation Engine

```
PURPOSE: Convert 12 raw features into a single governance risk score

STEP 1: Role-Based Z-Score Normalization (CRITICAL)
├─ Problem: Raw features are not comparable
│   - avg_daily_access ranges 5-40
│   - night_access_pct ranges 0-100%
│   - privilege_usage_gap ranges 0-12
│
├─ Solution: Z-score within each role
│   - Formula: z = (user_value - role_mean) / role_std
│   - Interpretation: 0 = average for role, +2 = outlier, -2 = outlier
│
├─ Why by ROLE?
│   - DB_Admins normally access 65-min sessions
│   - HR_Admins normally access 35-min sessions
│   - Cannot compare them directly!
│
└─ Result: 12 new z-score columns (one per feature)

STEP 2: Compose Final Risk Score
├─ Weights:
│   - Behavioral & Temporal: 35%
│   - Privilege Intelligence: 30%
│   - Stability & Drift: 15%
│
├─ Formula: 
│   risk_score = (behavioral_z * 0.35 + 
│                 privilege_z * 0.30 + 
│                 stability_z * 0.15)
│
└─ Result: Single 0-100 risk score per user

STEP 3: Risk Categorization
├─ Low/Medium/High/Critical
├─ Based on percentile distribution
└─ Used for governance reporting
```

---

### 7. **scripts/run_cleaning.py** - Orchestration Script #1

```
PURPOSE: Execute the entire cleaning pipeline in sequence

WORKFLOW:
1. Call clean_data.load_raw_data()           → Load from raw/
2. Call clean_data.handle_missing_values()   → Fill nulls
3. Call clean_data.standardize_columns()     → Standardize format
4. Call clean_data.remove_duplicates()       → Remove dupes
5. Call validate_data.validate_cleaned_data()→ QA check
6. Call clean_data.save_cleaned_data()       → Save to processed/
7. Call generate_cleaning_summary()          → Create report

EXECUTION:
$ python scripts/run_cleaning.py

OUTPUT:
├─ data/processed/cleaned_access_logs.csv
└─ outputs/reports/cleaning_summary.md
```

---

### 8. **scripts/run_feature_engineering.py** - Orchestration Script #2

```
PURPOSE: Execute all feature engineering steps in sequence

WORKFLOW:
1. Load cleaned data from processed/
2. Extract temporal components (hour, day_of_week, week, month, date)
3. Build 4 behavioral features      (behavioral_features.py)
4. Build 3 temporal features        (temporal_features.py)
5. Build 2 stability features       (stability_features.py)
6. Build 3 privilege features       (privilege_intelligence.py) ← NEW
7. Calculate 12 z-scores            (risk_scoring.py)
8. Calculate governance risk scores  (risk_scoring.py)
9. Add risk categories              (risk_scoring.py)
10. Validate results
11. Save to processed/

EXECUTION:
$ python scripts/run_feature_engineering.py

OUTPUT:
├─ data/processed/feature_engineered.csv (WITH 12 features + 12 z-scores + risk)
└─ outputs/reports/feature_engineering_summary.md
```

---

### 9. **Notebooks** - Interactive Exploration & Demonstration

#### **01_data_exploration.ipynb** - EDA Phase
```
PURPOSE: Understand raw data characteristics

SECTIONS:
├─ Load & inspect raw CSV
├─ Distribution of users by role
├─ Access patterns by resource type
├─ Temporal patterns (hourly, daily, weekly)
├─ Missing value analysis
├─ Outlier detection
└─ Role-stratified statistics

OUTPUT: Informs cleaning strategy
```

#### **02_data_cleaning.ipynb** - Cleaning Walkthrough
```
PURPOSE: Demonstrate and validate cleaning process

SECTIONS:
├─ Show before/after statistics
├─ Demonstrate missing value filling (role-specific)
├─ Show duplicates removed
├─ Validate data types
└─ Compare raw vs cleaned

This mirrors what run_cleaning.py automates
```

#### **03_feature_engineering.ipynb** - Feature Construction
```
PURPOSE: Show how each of 12 features is calculated

SECTIONS:
├─ Behavioral features (4) - access patterns
├─ Temporal features (3) - timing patterns
├─ Stability features (2) - consistency
├─ Privilege intelligence (3) - usage alignment
├─ Z-score normalization (by role)
├─ Risk score composition
└─ Risk category assignment

This mirrors what run_feature_engineering.py automates
```

#### **04_statistical_analysis.ipynb** - Statistical Validation
```
PURPOSE: Statistical analysis of features and risk scores

SECTIONS:
├─ Distribution analysis (are features normal?)
├─ Correlation analysis (which features matter?)
├─ Role-based comparisons
├─ Statistical significance testing
└─ Outlier analysis (who are the high-risk users?)
```

#### **05_visualization.ipynb** - Governance Dashboard
```
PURPOSE: Create final visualization dashboard for governance teams

VISUALIZATIONS:
├─ Risk Score Distribution (who has high risk?)
├─ Top Risk Drivers (what features drive high risk?)
├─ Privilege-Usage Misalignment (who's over-provisioned?)
├─ Behavioral Deviation Heatmap (feature z-scores by user)
├─ Temporal Patterns (when do high-risk users access?)
└─ Role-Based Risk Distribution (risk by role)

OUTPUT: outputs/figures/ and outputs/reports/
```

---

## 🔗 FILE CONNECTIONS & DEPENDENCIES

### Dependency Graph

```
Raw Data
    ↓
clean_data.py ←─── run_cleaning.py ←─── validate_data.py
    ↓
Cleaned Data
    ↓
behavioral_features.py ┐
temporal_features.py ──┤
stability_features.py ─┼─── run_feature_engineering.py
privilege_intelligence.py ┤
                         ↓
                    risk_scoring.py
                         ↓
                  Feature-Engineered Data
                         ↓
        ┌───────────────────┬─────────────────┐
        ↓                   ↓                 ↓
    01_data_exploration  02_data_cleaning  03_feature_engineering
    04_statistical_analysis  05_visualization
        ↓
    plots.py (visualization module)
        ↓
    outputs/ (figures & reports)
```

### Import Chain
```
Notebooks (01-05)
    ↓
    import from src.features.*
    import from src.data.*
    import from src.visualization.plots
    
Scripts (run_*.py)
    ↓
    import from src.features.*
    import from src.data.*
    import from src.utils.helpers
```

---

## 🚀 EXECUTION WORKFLOW

### Typical Usage (Production)

```bash
# Step 1: Clean data
python scripts/run_cleaning.py
→ data/processed/cleaned_access_logs.csv

# Step 2: Engineer features
python scripts/run_feature_engineering.py
→ data/processed/feature_engineered.csv
→ outputs/reports/feature_engineering_summary.md

# Step 3: Analyze in notebooks
jupyter notebook notebooks/05_visualization.ipynb
→ View governance dashboard
→ Export insights
```

### Typical Usage (Development/Analysis)

```
1. Start with 01_data_exploration.ipynb
2. Understand data characteristics
3. Move to 02_data_cleaning.ipynb
4. Validate cleaning logic
5. Run 03_feature_engineering.ipynb
6. Inspect features created
7. Run 04_statistical_analysis.ipynb
8. Understand statistical properties
9. Run 05_visualization.ipynb
10. Create governance dashboard
```

---

## 📊 HOW DATA TRANSFORMS

### Input Format (Raw)
```
user_id, role, resource_type, action, timestamp, session_duration, access_volume, success_flag
USER_001, DB_Admin, database_prod, read, 2025-01-15 09:30:00, 45, 3, 1
USER_001, DB_Admin, database_prod, write, 2025-01-15 09:35:00, 45, 1, 1
USER_002, HR_Admin, resource_db, read, 2025-01-15 10:00:00, 35, 2, 1
...
```

### After Cleaning
```
[Same format but:]
- Timestamps properly parsed
- Missing values filled (role-specific)
- Duplicates removed
- Data types validated
- Outliers documented
```

### After Feature Engineering
```
[Original columns PLUS:]
- avg_daily_access = 22.5
- export_ratio = 0.08
- unique_resources = 7
- avg_session_duration = 45
- night_access_pct = 12.3
- weekend_activity_ratio = 8.5
- access_time_variance = 3.2
- access_spike_score = 2
- weekly_access_change = 15.4
- privilege_usage_gap = 3
- privilege_usage_ratio = 0.70
- resource_access_concentration = 0.25
[PLUS 12 Z-score columns]
- avg_daily_access_z = +0.8
- export_ratio_z = +1.2
- ... etc
- governance_risk_score = 62.5
- risk_category = MEDIUM
```

---

## 🎓 KEY INSIGHTS FOR UNDERSTANDING

### Why 12 Features Matter
- **Behavioral (4)**: What do users access?
- **Temporal (3)**: When do users access?
- **Stability (2)**: How consistent are they?
- **Privilege Intelligence (3)**: Do they NEED what they're assigned?

→ **Together**: Holistic view of governance risk

### Why Role-Based Normalization
```
Raw feature comparison WRONG:
├─ DB_Admin avg_session = 65 min
├─ HR_Admin avg_session = 35 min
└─ "DB_Admins work longer" ← This is NORMAL, not a risk!

Z-score by role CORRECT:
├─ DB_Admin with 50min session = z = -0.8 (below average for role)
├─ HR_Admin with 50min session = z = +2.5 (well above average for role)
└─ "HR_Admin is unusual" ← Now comparing to relevant peers!
```

### Why Privilege Intelligence is Novel
```
Traditional Systems:
- Check: "Is user allowed?" → YES/NO
- Limitation: Doesn't detect over-privilege

Echelon:
- Check: "Does user NEED what they're assigned?"
- Detection: Identifies unused permissions = governance risk
- Example: User with 10 assigned databases but uses only 2
```

---

## 📈 OUTPUT ARTIFACTS

After running the full pipeline, you get:

```
outputs/
├── figures/
│   ├── eda_raw/
│   │   ├── role_distribution_pie
│   │   ├── access_timeline_heatmap
│   │   ├── action_type_distribution
│   │   └── ... (20+ EDA plots)
│   │
│   └── feature_engineering/
│       ├── feature_distributions_by_role
│       ├── correlation_heatmap
│       ├── privilege_gap_analysis
│       ├── risk_score_distribution
│       ├── user_59_feature_profile
│       └── ... (15+ analysis plots)
│
└── reports/
    ├── cleaning_summary.md
    │   ├── Records processed
    │   ├── Missing values handled
    │   ├── Duplicates removed
    │   └── Validation results
    │
    └── feature_engineering_summary.md
        ├── Feature statistics by role
        ├── Z-score distributions
        ├── Risk score summary
        ├── High-risk users identified
        └── Governance insights
```

---

## 🔍 TRACING A USER THROUGH THE SYSTEM

### Example: USER_059 (Cloud_Admin)

```
[Stage 1: Discovery]
Raw logs show: USER_059 accesses 40 resources/day
(Q: Is that normal? Can't tell without context)

↓

[Stage 2: Cleaning]
Logs deduplicated, timestamps validated
Still shows: 40 resources/day accesses
(Q: Still can't judge without role comparison)

↓

[Stage 3: Feature Engineering]
- avg_daily_access = 39 (role average for Cloud_Admin = 22) → FLAG
- export_ratio = 0.15 (role average = 0.05) → FLAG
- night_access_pct = 38 (role average = 10) → FLAG
- privilege_usage_gap = 3 (role average = 1) → FLAG
- access_time_variance = 55 (role average = 35) → FLAG

↓

[Stage 4: Normalization]
- avg_daily_access_z = +2.1 (2.1 std above role mean) → OUTLIER
- export_ratio_z = +2.8 (2.8 std above role mean) → OUTLIER
- night_access_pct_z = +1.9 → ELEVATED
- privilege_usage_gap_z = +1.5 → ELEVATED
- All z-scores combined

↓

[Stage 5: Risk Scoring]
governance_risk_score = 72/100 (MEDIUM-HIGH RISK)
risk_category = MEDIUM

↓

[Stage 6: Interpretation]
"USER_059 is a MEDIUM-RISK Cloud_Admin because:"
1. Much higher access volume than peers
2. Unusual high evening/night access pattern
3. High export ratio (data extraction sensitivity)
4. Over-provisioned (assigned >> used)
5. Inconsistent access patterns (behavioral drift)

→ Recommended Action: Review privilege assignments, 
   investigate access justification
```

---

## ✅ THE COMPLETE PICTURE

```
┌─────────────────────────────────────────────────────────────┐
│                    ECHELON SYSTEM                           │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  INPUT: Raw access logs from enterprise systems            │
│         ↓                                                   │
│  CLEAN: remove noise, standardize format (clean_data.py)  │
│         ↓                                                   │
│  ENGINEER: compute 12 behavioral & privilege features      │
│           (behavioral, temporal, stability, intelligence)  │
│         ↓                                                   │
│  NORMALIZE: role-based z-scores (risk_scoring.py)         │
│            (compare users to their role peers)             │
│         ↓                                                   │
│  SCORE: combine features → governance risk (0-100)        │
│         ↓                                                   │
│  OUTPUT: Risk scores, insights, dashboards (notebooks)    │
│                                                             │
│  GOVERNANCE QUESTION ANSWERED:                            │
│  "Which legitimately-privileged users represent the       │
│   highest governance risk due to behavioral deviation,    │
│   privilege misalignment, or instability?"                │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

**End of Guide** | For questions, refer to README.md and individual file docstrings.
