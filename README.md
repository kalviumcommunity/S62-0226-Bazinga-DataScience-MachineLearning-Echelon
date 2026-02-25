    # Echelon - Role-Aware Privileged Access Risk Scoring System with Usage Intelligence

-----------------------------------------------------------

## Problem Statement

Modern enterprises rely on privileged users such as system
administrators, database administrators, and cloud engineers who possess
elevated access to critical systems and sensitive data. While such
access is essential for operations, over time the actual usage of
granted privileges may diverge from what was originally assigned. This
leads to governance blind spots, persistent over-privileging, behavioral
instability, and elevated organizational risk.

Traditional access control systems such as RBAC determine whether access
is permitted but do not assess how access is used after it is granted.
Rule-based monitoring systems focus on threshold violations and fail to
detect gradual behavioral drift, privilege redundancy, or peer-relative
deviations.

This project designs and implements a role-aware, data-driven, and
machine-learning--based risk scoring system that analyzes privileged
user behavior, learns role-specific usage patterns, identifies
behavioral deviation and privilege--usage misalignment, tracks temporal
stability, and produces interpretable, continuous risk scores without
relying on labeled incident data.

------------------------------------------------------------------------

## What We Are Solving

We are solving a governance visibility problem, not a threat detection
problem.

**Core Question:**\
Among users who are legitimately allowed to access sensitive systems,
whose access patterns indicate increasing governance risk,
over-privilege, or behavioral inconsistency compared to peers in the
same role?

The system does NOT: 
- Detect malicious intent
- Block access
- Enforce policies

The system DOES: 
- Measure behavioral deviation
- Quantify privilege--usage misalignment
- Track behavioral stability over time
- Provide explainable governance risk scores

------------------------------------------------------------------------

## End-to-End System Flow

Access Logs → Data Cleaning → Feature Engineering → Role-Based
Segmentation → Representation Learning → Behavioral Clustering →
Deviation & Misalignment Modeling → Temporal Stability Analysis →
Ensemble Risk Scoring → Explainable Governance Insights

------------------------------------------------------------------------

## Data Description

Simulated enterprise privileged access logs.

### Dataset Columns

-   user_id
-   role (DB_Admin, HR_Admin, Cloud_Admin)
-   resource_type
-   action (read, write, delete, export)
-   timestamp
-   session_duration
-   access_volume
-   success_flag

No malicious/normal labels are included by design.

------------------------------------------------------------------------

## Data Science Phase

### Data Cleaning

-   Handle missing values
-   Standardize column names
-   Convert timestamp into datetime
-   Remove duplicates
-   Validate data types

### Feature Engineering (Core DS Work)

**Access Behavior Features** 
- Average access volume per day
- Export action ratio
- Unique resources accessed
- Average session duration

**Temporal Features** 
- Night access percentage
- Weekend activity ratio
- Access time variance

**Stability Features** 
- Week-over-week change
- Sudden access spikes

**Statistical Analysis** 
- Mean, variance, standard deviation
- Z-score comparison against role averages

### Governance Risk Index (Statistical)

Risk Index = Weighted combination of standardized deviations.

Normalize final score to 0-100.

**Risk Categories** 
- 0-30: Low Risk
- 31-60: Medium Risk
- 61-100: High Risk

------------------------------------------------------------------------

## Machine Learning Phase

### Unsupervised Anomaly Detection

Algorithm: Isolation Forest\
Purpose: Learn normal behavior per role and detect deviations without
labels.

### Representation Learning

Technique: PCA or Autoencoders\
Purpose: Reduce feature dimensionality and capture latent behavioral
structure.

### Behavioral Clustering

Algorithms: K-Means / DBSCAN\
Purpose: Identify access archetypes and behavioral segments.

### Distance-Based Risk Modeling

Techniques: Euclidean / Mahalanobis Distance\
Purpose: Quantify privilege--usage misalignment.

### Temporal Drift & Stability Modeling

Rolling statistics, variance tracking, trend detection.\
Purpose: Detect long-term instability and governance risk.

### Ensemble Risk Scoring

Combine: 
- Anomaly score
- Misalignment score
- Cluster rarity score
- Temporal instability score

Final risk = Aggregated normalized ensemble score.

### Explainable ML

Feature deviation ranking, Z-score explanation, cluster comparison.\
Provide human-readable governance insights.

------------------------------------------------------------------------

## Final Outputs

-   User-level risk score table
-   Role-wise risk distribution plots
-   Temporal risk trends
-   Explainable risk factor summaries
-   Governance prioritization list

------------------------------------------------------------------------

## Tools & Technologies

Programming: Python\
Data Handling: Pandas, NumPy\
Visualization: Matplotlib, Seaborn\
Machine Learning: Scikit-learn\
Optional Dashboard: Streamlit

------------------------------------------------------------------------

## Conclusion

This project demonstrates end-to-end data science and machine learning
applied to a realistic enterprise governance problem. It combines
feature engineering, statistical modeling, unsupervised learning,
ensemble risk synthesis, temporal analysis, and explainability to build
a decision-support system that enhances privileged access governance.

---


---
## Installing Python and Anaconda on Local Machine

### Objective

This milestone ensures that the local system is properly configured for
Data Science and Machine Learning development.\
The setup establishes a stable environment that will be used throughout
the sprint for notebooks, scripts, ML models, and deployment workflows.

------------------------------------------------------------------------

### System Information

-   **Operating System:** Windows 11 (64-bit)
-   **Python Version:** 3.13.9 (Anaconda distribution)
-   **Anaconda Version:** Conda 26.1.0
-   **Active Environment:** base

------------------------------------------------------------------------

### Installation Process

**1. Python Verification**

Python was verified through the terminal using the following commands:

python --version

Output:

Python 3.13.9

Python interactive shell test:

python print("Python Working") exit()

Output:

Python Working

![Python Verification](docs/python_version_verification.png)

------------------------------------------------------------------------

**2️. Anaconda Installation & Verification**

Anaconda was installed using the official Windows installer.

*Verification commands:*

conda --version conda env list

*Output:*

conda 26.1.0

*conda environments:*

# 

#### base \* C:`\Users`{=tex}`\varsha`{=tex}`\anaconda3`{=tex}

*Environment activation:*

conda activate base

![Python Verification](docs/conda_verification.png.png)


------------------------------------------------------------------------

**3️. Environment Validation**

The environment was validated by:

-   Launching Python via terminal
-   Running a basic print statement
-   Confirming Conda environment activation
-   Ensuring Jupyter Notebook can launch successfully

Command used:

jupyter notebook

Jupyter successfully started at:

http://localhost:8888/

![Python Verification](docs/jupyter_running.png)