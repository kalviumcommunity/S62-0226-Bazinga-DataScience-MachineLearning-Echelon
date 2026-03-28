import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import plotly.colors as pc

st.set_page_config(
    page_title="Echelon · Risk Intelligence",
    layout="wide",
    initial_sidebar_state="expanded",
)

if "show_dashboard" not in st.session_state:
    st.session_state.show_dashboard = False

@st.cache_data
def load_data():
    df = pd.read_csv("notebooks/processed_user_risk_data.csv")

    df["anomaly_label"] = pd.to_numeric(df.get("anomaly_label"), errors="coerce").fillna(1).astype(int)
    for c in ["ml_risk_score", "governance_risk_score", "pca1", "pca2"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["anomaly_label_str"] = df["anomaly_label"].map({1: "Normal", -1: "Anomaly"}).fillna("Unknown")
    return df

df = load_data()

total_users     = df["user_id"].nunique()
total_events    = len(df)
anomalous_users = df[df["anomaly_label"] == -1]["user_id"].nunique()
peak_risk       = df["ml_risk_score"].max()
avg_risk        = df["ml_risk_score"].mean()
high_risk_pct   = (df["risk_category"] == "High").mean() * 100

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@800&family=JetBrains+Mono:wght@400;500&display=swap');

html, body, [class*="css"] { font-family: 'JetBrains Mono', monospace; }
.stApp { background: #0d1117; }
.block-container { padding: 0 !important; }
#MainMenu, footer, header { visibility: hidden; }

[data-testid="stSidebar"] { background: #0f1117; border-right: 1px solid #1e2130; }
[data-testid="stSidebar"] * { color: #e2e8f0 !important; }
[data-testid="stSidebar"] .stSlider > div > div > div { background: #3b4fd8 !important; }

.dash-wrap { padding: 2rem 2.5rem; }

.kpi-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 2rem; }
.kpi-card { background: #161b27; border: 1px solid #1e2a3a; border-radius: 12px; padding: 20px 24px; position: relative; overflow: hidden; }
.kpi-card::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px; border-radius: 12px 12px 0 0; }
.kpi-card.blue::before  { background: #3b82f6; }
.kpi-card.red::before   { background: #ef4444; }
.kpi-card.amber::before { background: #f59e0b; }
.kpi-card.green::before { background: #10b981; }
.kpi-label { font-size: 11px; font-weight: 600; letter-spacing: 0.08em; text-transform: uppercase; color: #64748b; margin-bottom: 8px; }
.kpi-value { font-size: 28px; font-weight: 700; color: #f1f5f9; line-height: 1; }
.kpi-sub   { font-size: 12px; color: #475569; margin-top: 6px; }

.section-header { display: flex; align-items: center; gap: 10px; margin: 2rem 0 1rem 0; padding-bottom: 10px; border-bottom: 1px solid #1e2a3a; }
.section-header h3 { font-size: 15px; font-weight: 600; color: #cbd5e1; margin: 0; }
.section-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }

.sidebar-brand { padding: 1rem 0 1.5rem 0; border-bottom: 1px solid #1e2130; margin-bottom: 1.5rem; }
.sidebar-brand h2 { font-size: 18px; font-weight: 800; color: #f1f5f9 !important; margin: 0 0 2px 0; font-family: 'Syne', sans-serif; }
.sidebar-brand p  { font-size: 12px; color: #475569 !important; margin: 0; }

[data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; border: 1px solid #1e2a3a; }

/* landing */
.landing { min-height: 100vh; background: #0d1117; display: flex; flex-direction: column; align-items: center; justify-content: center; position: relative; overflow: hidden; padding: 2rem 1rem; }
.grid-bg { position: fixed; inset: 0; background-image: linear-gradient(rgba(59,130,246,0.05) 1px, transparent 1px), linear-gradient(90deg, rgba(59,130,246,0.05) 1px, transparent 1px); background-size: 44px 44px; pointer-events: none; z-index: 0; }
.corner { position: absolute; width: 40px; height: 40px; }
.corner::before, .corner::after { content: ''; position: absolute; background: #3b82f6; opacity: 0.45; }
.corner::before { width: 2px; height: 24px; }
.corner::after  { width: 24px; height: 2px; }
.c-tl { top: 18px; left: 18px; } .c-tl::before { top: 0; left: 0; } .c-tl::after { top: 0; left: 0; }
.c-tr { top: 18px; right: 18px; } .c-tr::before { top: 0; right: 0; } .c-tr::after { top: 0; right: 0; }
.c-bl { bottom: 18px; left: 18px; } .c-bl::before { bottom: 0; left: 0; } .c-bl::after { bottom: 0; left: 0; }
.c-br { bottom: 18px; right: 18px; } .c-br::before { bottom: 0; right: 0; } .c-br::after { bottom: 0; right: 0; }
.l-tag { font-size: 10px; letter-spacing: 0.18em; color: #3b82f6; background: rgba(59,130,246,0.08); border: 1px solid rgba(59,130,246,0.2); padding: 5px 14px; border-radius: 2px; margin-bottom: 22px; text-transform: uppercase; }
.wordmark { font-family: 'Syne', 'Arial Black', sans-serif; font-size: clamp(56px, 10vw, 108px); font-weight: 900; letter-spacing: -0.02em; line-height: 1; color: #f1f5f9; text-align: center; margin-bottom: 8px; }
.wordmark span { color: #3b82f6; }
.l-tagline { font-size: 11px; letter-spacing: 0.24em; color: #475569; margin-bottom: 36px; text-transform: uppercase; }
.strip { display: flex; border: 1px solid #1e2a3a; border-radius: 8px; overflow: hidden; margin-bottom: 36px; background: #161b27; flex-wrap: wrap; position: relative; z-index: 1; }
.s-metric { padding: 14px 26px; text-align: center; border-right: 1px solid #1e2a3a; min-width: 110px; }
.s-metric:last-child { border-right: none; }
.s-val { font-size: 22px; font-weight: 500; line-height: 1; margin-bottom: 5px; }
.s-lbl { font-size: 9px; letter-spacing: 0.1em; color: #475569; text-transform: uppercase; }
.lm-blue  { color: #3b82f6; } .lm-red { color: #ef4444; }
.lm-amber { color: #f59e0b; } .lm-green { color: #10b981; }
.l-hint { font-size: 10px; color: #334155; letter-spacing: 0.08em; margin-bottom: 24px; margin-top: 4px; }
.fpills { display: flex; flex-wrap: wrap; justify-content: center; gap: 7px; max-width: 560px; position: relative; z-index: 1; }
.fpill { font-size: 9px; letter-spacing: 0.1em; color: #475569; background: #161b27; border: 1px solid #1e2a3a; padding: 4px 11px; border-radius: 999px; text-transform: uppercase; }

div[data-testid="stButton"] > button {
    background: #3b82f6 !important; color: #fff !important; border: none !important;
    padding: 13px 44px !important; font-family: 'Syne', sans-serif !important;
    font-size: 13px !important; font-weight: 700 !important;
    letter-spacing: 0.1em !important; text-transform: uppercase !important;
    border-radius: 4px !important; position: relative; z-index: 1;
}
div[data-testid="stButton"] > button:hover { background: #2563eb !important; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════
#  LANDING
# ══════════════════════════════════════════════════════
if not st.session_state.show_dashboard:

    st.markdown(f"""
    <div class="landing">
      <div class="grid-bg"></div>
      <div class="corner c-tl"></div><div class="corner c-tr"></div>
      <div class="corner c-bl"></div><div class="corner c-br"></div>

      <div class="l-tag">Privileged Access Governance · Confidential</div>

      <div class="wordmark">ECHEL<span>ON</span></div>
      <div class="l-tagline">Risk Intelligence Platform</div>

      <div class="strip">
        <div class="s-metric"><div class="s-val lm-blue">{total_users}</div><div class="s-lbl">Users Monitored</div></div>
        <div class="s-metric"><div class="s-val lm-red">{anomalous_users}</div><div class="s-lbl">Anomalies Detected</div></div>
        <div class="s-metric"><div class="s-val lm-amber">{peak_risk:.1f}</div><div class="s-lbl">Peak Risk Score</div></div>
        <div class="s-metric"><div class="s-val lm-green">{avg_risk:.1f}</div><div class="s-lbl">Avg Risk Score</div></div>
        <div class="s-metric"><div class="s-val lm-amber">{high_risk_pct:.1f}%</div><div class="s-lbl">High-risk Events</div></div>
        <div class="s-metric"><div class="s-val lm-blue">{total_events:,}</div><div class="s-lbl">Total Events</div></div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([2, 1, 2])
    with col2:
        if st.button("  LAUNCH DASHBOARD"):
            st.session_state.show_dashboard = True
            st.rerun()

    st.markdown("""
    <div style="display:flex;flex-direction:column;align-items:center;margin-top:6px;">
      <div class="l-hint">Initializing risk intelligence engine</div>
      <div class="fpills">
        <div class="fpill">Isolation Forest</div>
        <div class="fpill">PCA Projection</div>
        <div class="fpill">Behavioral Clustering</div>
        <div class="fpill">Temporal Drift</div>
        <div class="fpill">Ensemble Scoring</div>
        <div class="fpill">SHAP Explainability</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.stop()


# ══════════════════════════════════════════════════════
#  DASHBOARD
# ══════════════════════════════════════════════════════

st.markdown('<div class="dash-wrap">', unsafe_allow_html=True)

PLOT_BG  = "#161b27"
GRID_CLR = "#1e2a3a"
TEXT_CLR = "#94a3b8"
FONT_FAM = "'JetBrains Mono', monospace"

def dark_layout(fig, title="", height=340):
    layout_updates = dict(
        height=height,
        paper_bgcolor=PLOT_BG,
        plot_bgcolor=PLOT_BG,
        font=dict(family=FONT_FAM, color=TEXT_CLR, size=12),
        margin=dict(l=12, r=12, t=36 if title else 16, b=12),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#94a3b8", size=12), borderwidth=0),
        xaxis=dict(gridcolor=GRID_CLR, linecolor=GRID_CLR, tickcolor=GRID_CLR, zerolinecolor=GRID_CLR),
        yaxis=dict(gridcolor=GRID_CLR, linecolor=GRID_CLR, tickcolor=GRID_CLR, zerolinecolor=GRID_CLR),
    )
    if title:
        layout_updates["title"] = dict(text=title, font=dict(size=14, color="#cbd5e1"), x=0, xanchor="left")
    fig.update_layout(**layout_updates)
    return fig

# ── Sidebar ──────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div class="sidebar-brand">
        <h2>ECHELON</h2>
        <p>Risk Intelligence Platform</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("**Filters**")
    risk_threshold = st.slider("Min ML Risk Score", 0, 100, 0)
    st.markdown("---")
    roles = ["All Roles"] + sorted(df["role"].unique().tolist())
    selected_role = st.selectbox("Role", roles)
    st.markdown("---")
    st.markdown(f"""
    <div style="font-size:11px;color:#334155;line-height:1.8;">
        <div>Total events &nbsp;<strong style="color:#64748b">{len(df):,}</strong></div>
        <div>Total users &nbsp;&nbsp;<strong style="color:#64748b">{df['user_id'].nunique()}</strong></div>
        <div>Date range &nbsp;&nbsp;&nbsp;<strong style="color:#64748b">2024</strong></div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("---")
    if st.button("← Back to Landing"):
        st.session_state.show_dashboard = False
        st.rerun()

# ── Filter ───────────────────────────────────────────
filtered = df[df["ml_risk_score"] >= risk_threshold]
if selected_role != "All Roles":
    filtered = filtered[filtered["role"] == selected_role]

if filtered.empty:
    st.info("No data for the selected filters. Try lowering the ML risk threshold or choosing another role.")
    st.stop()

user_view = (
    filtered
    .groupby("user_id", as_index=False)
    .agg(
        governance_risk_score=("governance_risk_score", "mean"),
        ml_risk_score=("ml_risk_score", "mean"),
        role=("role", "first"),
        risk_category=("risk_category", lambda x: x.mode().iat[0] if not x.mode().empty else "—"),
        events=("user_id", "size"),
        anomaly_label=("anomaly_label", lambda s: -1 if (s == -1).any() else 1),
    )
)
user_view["anomaly_label_str"] = user_view["anomaly_label"].map({1: "Normal", -1: "Anomaly"}).fillna("Unknown")

pca_view = (
    filtered
    .groupby("user_id", as_index=False)
    .agg(
        pca1=("pca1", "mean"),
        pca2=("pca2", "mean"),
    )
    .merge(
        user_view[["user_id", "ml_risk_score", "role", "events", "anomaly_label", "anomaly_label_str"]],
        on="user_id",
        how="left",
    )
)

total_users_f   = user_view["user_id"].nunique()
anomalous_f     = (user_view["anomaly_label"] == -1).sum()
high_risk_pct_f = (filtered["risk_category"] == "High").mean() * 100
avg_ml_score_f  = user_view["ml_risk_score"].mean()

st.markdown(f"""
<div class="kpi-grid">
    <div class="kpi-card blue">
        <div class="kpi-label">Users in view</div>
        <div class="kpi-value">{total_users_f}</div>
        <div class="kpi-sub">{len(filtered):,} access events</div>
    </div>
    <div class="kpi-card red">
        <div class="kpi-label">Anomalous users</div>
        <div class="kpi-value">{anomalous_f}</div>
        <div class="kpi-sub">Detected by Isolation Forest</div>
    </div>
    <div class="kpi-card amber">
        <div class="kpi-label">High-risk events</div>
        <div class="kpi-value">{high_risk_pct_f:.1f}%</div>
        <div class="kpi-sub">Of filtered events</div>
    </div>
    <div class="kpi-card green">
        <div class="kpi-label">Avg ML risk score</div>
        <div class="kpi-value">{avg_ml_score_f:.1f}</div>
        <div class="kpi-sub">Out of 100</div>
    </div>
</div>
""", unsafe_allow_html=True)

col1, col2 = st.columns([1, 1], gap="medium")

with col1:
    st.markdown("""<div class="section-header">
        <div class="section-dot" style="background:#3b82f6;"></div>
        <h3>ML Risk Score Distribution</h3>
    </div>""", unsafe_allow_html=True)

    scores = filtered["ml_risk_score"].dropna().astype(float).clip(0, 100)
    edges = np.linspace(0, 100, 31)
    counts, _ = np.histogram(scores, bins=edges)

    bin_labels = [f"{int(edges[i])}-{int(edges[i+1])}" for i in range(len(edges) - 1)]
    bin_centers = (edges[:-1] + edges[1:]) / 2

    colorscale = [[0, "#1d4ed8"], [0.5, "#7c3aed"], [1, "#dc2626"]]
    denom = (bin_centers.max() - bin_centers.min()) or 1.0
    tvals = (bin_centers - bin_centers.min()) / denom
    bar_colors = [pc.sample_colorscale(colorscale, float(t))[0] for t in tvals]

    fig1 = go.Figure()
    fig1.add_trace(go.Bar(
        x=bin_labels,
        y=counts,
        marker=dict(color=bar_colors, line=dict(width=0)),
        hovertemplate="Score bin: %{x}<br>Count: %{y:,}<extra></extra>",
    ))
    dark_layout(fig1, height=300)
    fig1.update_layout(showlegend=False, bargap=0.04)
    fig1.update_xaxes(title_text="ML Risk Score (bin)", title_font=dict(size=11))
    fig1.update_yaxes(title_text="Events", title_font=dict(size=11))
    st.plotly_chart(fig1, use_container_width=True, config={"displayModeBar": False})

with col2:
    st.markdown("""<div class="section-header">
        <div class="section-dot" style="background:#8b5cf6;"></div>
        <h3>ML vs Statistical Risk</h3>
    </div>""", unsafe_allow_html=True)

    fig2 = px.scatter(
        user_view,
        x="governance_risk_score",
        y="ml_risk_score",
        color="risk_category",
        size="events",
        size_max=14,
        color_discrete_map={"Low": "#22c55e", "Medium": "#f59e0b", "High": "#ef4444"},
        opacity=0.75,
        hover_data={
            "user_id": True,
            "role": True,
            "risk_category": True,
            "events": True,
            "governance_risk_score": ":.1f",
            "ml_risk_score": ":.1f",
        },
        labels={
            "governance_risk_score": "Governance Risk Score (avg per user)",
            "ml_risk_score": "ML Risk Score (avg per user)",
            "events": "Events",
        },
        render_mode="webgl",
    )
    dark_layout(fig2, height=300)
    fig2.update_traces(marker=dict(line=dict(width=0)))
    fig2.update_layout(legend_title_text="Risk level", legend=dict(orientation="h", y=-0.2))
    st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})

col3, col4 = st.columns([3, 2], gap="medium")

with col3:
    st.markdown("""<div class="section-header">
        <div class="section-dot" style="background:#06b6d4;"></div>
        <h3>Behavioral Space — PCA Projection</h3>
    </div>""", unsafe_allow_html=True)

    fig3 = px.scatter(
        pca_view,
        x="pca1", y="pca2",
        color="anomaly_label_str",
        color_discrete_map={"Normal": "#3b82f6", "Anomaly": "#ef4444", "Unknown": "#94a3b8"},
        symbol="anomaly_label_str",
        symbol_map={"Normal": "circle", "Anomaly": "x", "Unknown": "diamond"},
        size="ml_risk_score",
        size_max=14,
        opacity=0.8,
        render_mode="webgl",
        hover_data={
            "user_id": True,
            "role": True,
            "events": True,
            "ml_risk_score": ":.1f",
            "pca1": ":.2f",
            "pca2": ":.2f",
        },
        labels={"pca1": "PC 1", "pca2": "PC 2", "anomaly_label_str": ""},
    )
    fig3.update_traces(
        selector=dict(name="Anomaly"),
        marker=dict(size=10, line=dict(width=1.5, color="#ef4444")),
    )
    dark_layout(fig3, height=360)
    fig3.update_layout(legend=dict(orientation="h", y=1.08, x=0, font=dict(size=12, color="#94a3b8")))
    st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar": False})

with col4:
    st.markdown("""<div class="section-header">
        <div class="section-dot" style="background:#f59e0b;"></div>
        <h3>Normal vs Anomalous</h3>
    </div>""", unsafe_allow_html=True)

    label_counts = filtered["anomaly_label_str"].value_counts().reset_index()
    label_counts.columns = ["type", "count"]

    fig5 = go.Figure(go.Bar(
        x=label_counts["type"], y=label_counts["count"],
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

st.markdown("""<div class="section-header">
    <div class="section-dot" style="background:#f43f5e;"></div>
    <h3>Top risky users</h3>
</div>""", unsafe_allow_html=True)

top_users = (
    user_view
    .sort_values(["anomaly_label", "ml_risk_score"], ascending=[True, False])
    .head(10)
    .rename(columns={
        "user_id": "User",
        "ml_risk_score": "ML Score",
        "governance_risk_score": "Gov. Score",
        "role": "Role",
        "anomaly_label_str": "Status",
        "risk_category": "Category",
        "events": "Events",
    })
    [["User", "ML Score", "Gov. Score", "Role", "Status", "Category", "Events"]]
    .round({"ML Score": 1, "Gov. Score": 1})
)

st.dataframe(
    top_users,
    use_container_width=True,
    hide_index=True,
    column_config={
        "ML Score":   st.column_config.ProgressColumn("ML Score",  min_value=0, max_value=100, format="%.1f"),
        "Gov. Score": st.column_config.ProgressColumn("Gov. Score", min_value=0, max_value=100, format="%.1f"),
        "Events":     st.column_config.NumberColumn("Events", format="%d"),
    }
)

st.markdown('</div>', unsafe_allow_html=True)