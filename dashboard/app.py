import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ── Page config ────────────────────────────────────────────────
st.set_page_config(
    page_title="Echelon · Risk Intelligence",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────
st.markdown("""
<style>
/* Global font */
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

/* Sidebar */
[data-testid="stSidebar"] {
    background: #0f1117;
    border-right: 1px solid #1e2130;
}
[data-testid="stSidebar"] * { color: #e2e8f0 !important; }
[data-testid="stSidebar"] .stSlider > div > div > div {
    background: #3b4fd8 !important;
}

/* Main background */
.stApp { background: #0d1117; }

/* Remove default padding on main block */
.block-container { padding: 2rem 2.5rem 2rem 2.5rem; }

/* KPI cards */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    margin-bottom: 2rem;
}
.kpi-card {
    background: #161b27;
    border: 1px solid #1e2a3a;
    border-radius: 12px;
    padding: 20px 24px;
    position: relative;
    overflow: hidden;
}
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    border-radius: 12px 12px 0 0;
}
.kpi-card.blue::before  { background: #3b82f6; }
.kpi-card.red::before   { background: #ef4444; }
.kpi-card.amber::before { background: #f59e0b; }
.kpi-card.green::before { background: #10b981; }

.kpi-label {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: #64748b;
    margin-bottom: 8px;
}
.kpi-value {
    font-size: 28px;
    font-weight: 700;
    color: #f1f5f9;
    line-height: 1;
}
.kpi-sub {
    font-size: 12px;
    color: #475569;
    margin-top: 6px;
}

/* Section headers */
.section-header {
    display: flex;
    align-items: center;
    gap: 10px;
    margin: 2rem 0 1rem 0;
    padding-bottom: 10px;
    border-bottom: 1px solid #1e2a3a;
}
.section-header h3 {
    font-size: 15px;
    font-weight: 600;
    color: #cbd5e1;
    margin: 0;
    letter-spacing: 0.01em;
}
.section-dot {
    width: 8px; height: 8px;
    border-radius: 50%;
    flex-shrink: 0;
}

/* Sidebar branding */
.sidebar-brand {
    padding: 1rem 0 1.5rem 0;
    border-bottom: 1px solid #1e2130;
    margin-bottom: 1.5rem;
}
.sidebar-brand h2 {
    font-size: 18px;
    font-weight: 700;
    color: #f1f5f9 !important;
    margin: 0 0 2px 0;
}
.sidebar-brand p {
    font-size: 12px;
    color: #475569 !important;
    margin: 0;
}

/* Streamlit metric override */
[data-testid="metric-container"] {
    background: #161b27;
    border: 1px solid #1e2a3a;
    border-radius: 12px;
    padding: 1rem;
}

/* Table styling */
[data-testid="stDataFrame"] {
    border-radius: 12px;
    overflow: hidden;
    border: 1px solid #1e2a3a;
}

/* Hide streamlit branding */
#MainMenu, footer, header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── Load & prep data ───────────────────────────────────────────
@st.cache_data
def load_data():
    df = pd.read_csv("notebooks/processed_user_risk_data.csv")
    df["anomaly_label_str"] = df["anomaly_label"].map({1: "Normal", -1: "Anomaly"}).fillna("Unknown")
    return df

df = load_data()

# Plotly dark theme base
PLOT_BG   = "#161b27"
GRID_CLR  = "#1e2a3a"
TEXT_CLR  = "#94a3b8"
FONT_FAM  = "Inter, sans-serif"

# ...existing code...

def dark_layout(fig, title="", height=340):
    layout_updates = dict(
        height=height,
        paper_bgcolor=PLOT_BG,
        plot_bgcolor=PLOT_BG,
        font=dict(family=FONT_FAM, color=TEXT_CLR, size=12),
        margin=dict(l=12, r=12, t=36 if title else 16, b=12),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(color="#94a3b8", size=12),
            borderwidth=0,
        ),
        xaxis=dict(gridcolor=GRID_CLR, linecolor=GRID_CLR, tickcolor=GRID_CLR, zerolinecolor=GRID_CLR),
        yaxis=dict(gridcolor=GRID_CLR, linecolor=GRID_CLR, tickcolor=GRID_CLR, zerolinecolor=GRID_CLR),
    )

    # ✅ Only set a title if provided (prevents "undefined")
    if title:
        layout_updates["title"] = dict(
            text=title,
            font=dict(size=14, color="#cbd5e1"),
            x=0,
            xanchor="left",
        )

    fig.update_layout(**layout_updates)
    return fig

# ...existing code...

# ── Sidebar ────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div class="sidebar-brand">
        <h2>ECHELON</h2>
        <p>Risk Intelligence Platform</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("**Filters**")
    risk_threshold = st.slider("Min ML Risk Score", 0, 100, 0, help="Filter events by minimum ML risk score")

    st.markdown("---")

    roles = ["All Roles"] + sorted(df["role"].unique().tolist())
    selected_role = st.selectbox("Role", roles)

    st.markdown("---")
    st.markdown(f"""
    <div style="font-size:11px; color:#334155; line-height:1.8;">
        <div>Total events &nbsp;<strong style="color:#64748b">{len(df):,}</strong></div>
        <div>Total users &nbsp;&nbsp;<strong style="color:#64748b">{df['user_id'].nunique()}</strong></div>
        <div>Date range &nbsp;&nbsp;&nbsp;<strong style="color:#64748b">2024</strong></div>
    </div>
    """, unsafe_allow_html=True)

# ── Filter data ────────────────────────────────────────────────
filtered = df[df["ml_risk_score"] >= risk_threshold]
if selected_role != "All Roles":
    filtered = filtered[filtered["role"] == selected_role]

# ── KPI row ────────────────────────────────────────────────────
total_users   = filtered["user_id"].nunique()
anomalous     = df.loc[df["anomaly_label"] == -1, "user_id"].nunique()
high_risk_pct = (filtered["risk_category"] == "High").mean() * 100
avg_ml_score  = filtered["ml_risk_score"].mean()

st.markdown(f"""
<div class="kpi-grid">
    <div class="kpi-card blue">
        <div class="kpi-label">Users in view</div>
        <div class="kpi-value">{total_users}</div>
        <div class="kpi-sub">{len(filtered):,} access events</div>
    </div>
    <div class="kpi-card red">
        <div class="kpi-label">Anomalous users</div>
        <div class="kpi-value">{anomalous}</div>
        <div class="kpi-sub">Detected by Isolation Forest</div>
    </div>
    <div class="kpi-card amber">
        <div class="kpi-label">High-risk events</div>
        <div class="kpi-value">{high_risk_pct:.1f}%</div>
        <div class="kpi-sub">Of filtered events</div>
    </div>
    <div class="kpi-card green">
        <div class="kpi-label">Avg ML risk score</div>
        <div class="kpi-value">{avg_ml_score:.1f}</div>
        <div class="kpi-sub">Out of 100</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Row 1: Distribution + ML vs Governance ─────────────────────
col1, col2 = st.columns([1, 1], gap="medium")

with col1:
    st.markdown("""<div class="section-header">
        <div class="section-dot" style="background:#3b82f6;"></div>
        <h3>ML Risk Score Distribution</h3>
    </div>""", unsafe_allow_html=True)

    fig1 = go.Figure()
    fig1.add_trace(go.Histogram(
        x=filtered["ml_risk_score"],
        nbinsx=30,
        marker=dict(
            color=filtered["ml_risk_score"],
            colorscale=[[0, "#1d4ed8"], [0.5, "#7c3aed"], [1, "#dc2626"]],
            line=dict(width=0),
        ),
        hovertemplate="Score: %{x:.0f}<br>Count: %{y}<extra></extra>",
    ))
    dark_layout(fig1, height=300)
    fig1.update_layout(showlegend=False, bargap=0.04)
    fig1.update_xaxes(title_text="ML Risk Score", title_font=dict(size=11))
    fig1.update_yaxes(title_text="Events", title_font=dict(size=11))
    st.plotly_chart(fig1, use_container_width=True, config={"displayModeBar": False})

with col2:
    st.markdown("""<div class="section-header">
        <div class="section-dot" style="background:#8b5cf6;"></div>
        <h3>ML vs Statistical Risk</h3>
    </div>""", unsafe_allow_html=True)

    fig2 = px.scatter(
        filtered,
        x="governance_risk_score",
        y="ml_risk_score",
        color="risk_category",
        color_discrete_map={"Low": "#22c55e", "Medium": "#f59e0b", "High": "#ef4444"},
        opacity=0.45,
        hover_data={"user_id": True, "role": True, "risk_category": True,
                    "governance_risk_score": ":.1f", "ml_risk_score": ":.1f"},
        labels={"governance_risk_score": "Governance Risk Score", "ml_risk_score": "ML Risk Score"},
    )
    dark_layout(fig2, height=300)
    fig2.update_traces(marker=dict(size=5))
    fig2.update_layout(legend_title_text="Risk level", legend=dict(orientation="h", y=-0.2))
    st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})

# ── Row 2: PCA + Anomaly count ─────────────────────────────────
col3, col4 = st.columns([3, 2], gap="medium")

with col3:
    st.markdown("""<div class="section-header">
        <div class="section-dot" style="background:#06b6d4;"></div>
        <h3>Behavioral Space — PCA Projection</h3>
    </div>""", unsafe_allow_html=True)

    fig3 = px.scatter(
        filtered,
        x="pca1", y="pca2",
        color="anomaly_label_str",
        color_discrete_map={"Normal": "#3b82f6", "Anomaly": "#ef4444"},
        symbol="anomaly_label_str",
        symbol_map={"Normal": "circle", "Anomaly": "x"},
        size="ml_risk_score",
        size_max=10,
        opacity=0.65,
        hover_data={"user_id": True, "role": True, "ml_risk_score": ":.1f",
                    "pca1": False, "pca2": False, "anomaly_label_str": False},
        labels={"pca1": "PC 1", "pca2": "PC 2", "anomaly_label_str": ""},
    )
    # Make anomaly markers pop
    fig3.update_traces(
        selector=dict(name="Anomaly"),
        marker=dict(size=10, line=dict(width=1.5, color="#ef4444")),
    )
    dark_layout(fig3, height=360)
    fig3.update_layout(legend=dict(
        orientation="h", y=1.08, x=0,
        font=dict(size=12, color="#94a3b8"),
    ))
    st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar": False})

with col4:
    st.markdown("""<div class="section-header">
        <div class="section-dot" style="background:#f59e0b;"></div>
        <h3>Normal vs Anomalous</h3>
    </div>""", unsafe_allow_html=True)

    label_counts = filtered["anomaly_label_str"].value_counts().reset_index()
    label_counts.columns = ["type", "count"]

    fig5 = go.Figure(go.Bar(
        x=label_counts["type"],
        y=label_counts["count"],
        marker=dict(
            color=["#3b82f6" if t == "Normal" else "#ef4444" for t in label_counts["type"]],
            cornerradius=6,
        ),
        text=label_counts["count"].apply(lambda v: f"{v:,}"),
        textposition="outside",
        textfont=dict(size=13, color="#cbd5e1"),
        hovertemplate="%{x}: %{y:,}<extra></extra>",
    ))
    dark_layout(fig5, height=180)
    fig5.update_layout(showlegend=False, margin=dict(t=12, b=12))
    fig5.update_yaxes(showgrid=False, showticklabels=False)
    fig5.update_xaxes(tickfont=dict(size=13, color="#94a3b8"))
    st.plotly_chart(fig5, use_container_width=True, config={"displayModeBar": False})

    st.markdown("""<div class="section-header" style="margin-top:1rem;">
        <div class="section-dot" style="background:#10b981;"></div>
        <h3>Risk Score Spread</h3>
    </div>""", unsafe_allow_html=True)

    fig4 = go.Figure()
    fig4.add_trace(go.Box(
        y=filtered["ml_risk_score"],
        marker_color="#7c3aed",
        line_color="#8b5cf6",
        fillcolor="rgba(124,58,237,0.15)",
        boxmean=True,
        hovertemplate="Score: %{y:.1f}<extra></extra>",
    ))
    dark_layout(fig4, height=160)
    fig4.update_layout(showlegend=False, margin=dict(t=8, b=8))
    fig4.update_xaxes(showticklabels=False)
    fig4.update_yaxes(title_text="ML Risk Score", title_font=dict(size=11))
    st.plotly_chart(fig4, use_container_width=True, config={"displayModeBar": False})

# ── Top risky users table ──────────────────────────────────────
st.markdown("""<div class="section-header">
    <div class="section-dot" style="background:#f43f5e;"></div>
    <h3>Top risky users</h3>
</div>""", unsafe_allow_html=True)

top_users = (
    filtered
    .groupby("user_id")
    .agg(
        ml_risk_score   =("ml_risk_score",         "mean"),
        gov_risk_score  =("governance_risk_score",  "mean"),
        role            =("role",                   "first"),
        anomaly_status  =("anomaly_label_str",      "first"),
        risk_category   =("risk_category",          lambda x: x.mode()[0] if not x.mode().empty else "—"),
        total_events    =("ml_risk_score",          "count"),
    )
    .reset_index()
    .sort_values("ml_risk_score", ascending=False)
    .head(10)
    .round(2)
    .rename(columns={
        "user_id":       "User",
        "role":          "Role",
        "ml_risk_score": "ML Score",
        "gov_risk_score":"Gov. Score",
        "anomaly_status":"Status",
        "risk_category": "Category",
        "total_events":  "Events",
    })
)

st.dataframe(
    top_users,
    use_container_width=True,
    hide_index=True,
    column_config={
        "ML Score":   st.column_config.ProgressColumn("ML Score",  min_value=0, max_value=100, format="%.1f"),
        "Gov. Score": st.column_config.ProgressColumn("Gov. Score",min_value=0, max_value=100, format="%.1f"),
        "Events":     st.column_config.NumberColumn("Events", format="%d"),
    }
)