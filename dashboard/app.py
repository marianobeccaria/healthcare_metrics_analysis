# ============================================================
# Healthcare Metrics Dashboard
# Step 6 — Streamlit Dashboard
# ============================================================
# HOW TO RUN:
#   conda activate healthcare-cdk
#   cd dashboard
#   streamlit run app.py
# ============================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import os
from dotenv import load_dotenv

load_dotenv()

# ── Stage 1: Page config ──────────────────────────────────────
# Must be the very first Streamlit command in the script.
# layout="wide" uses full browser width instead of narrow column.
st.set_page_config(
    page_title="Healthcare Staffing Metrics",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Stage 2: Config and data loading ─────────────────────────
GOLD_FACILITY_PATH = os.environ.get(
    "GOLD_FACILITY_PATH",
    "s3://mbeccaria-dea-healthcare-metrics/gold/facility_summary/"
)
GOLD_STAFFING_PATH = os.environ.get(
    "GOLD_STAFFING_PATH",
    "s3://mbeccaria-dea-healthcare-metrics/gold/staffing_metrics/"
)

# @st.cache_data caches the result after the first call.
# Without it: S3 is re-read on every user interaction (slow + costly)
# With it:    S3 is read once and result reused every rerun
@st.cache_data
def load_facility_data():
    """
    Load Gold facility summary table from S3.
    One row per facility per quarter.
    Contains aggregated staffing metrics and CMS compliance flags.
    """
    return pd.read_parquet(
        GOLD_FACILITY_PATH,
        storage_options={"anon": False}
    )

@st.cache_data
def load_staffing_data():
    """
    Load Gold daily staffing metrics table from S3.
    One row per facility per day.
    Used for trend charts and time-series analysis.
    """
    df = pd.read_parquet(
        GOLD_STAFFING_PATH,
        storage_options={"anon": False}
    )
    df["WorkDate"] = pd.to_datetime(df["WorkDate"])
    return df

# load data with spinner so user sees loading feedback
with st.spinner("Loading data from S3..."):
    try:
        df_facility = load_facility_data()
        df_staffing = load_staffing_data()
        data_loaded = True
    except Exception as e:
        st.error(f"Could not load data from S3: {e}")
        data_loaded = False

# halt script here if data failed to load
if not data_loaded:
    st.stop()

# ── Stage 3: Sidebar filters ──────────────────────────────────
# IMPORTANT: sidebar must be built BEFORE any code that uses
# df_filtered — Streamlit runs top to bottom so filters must
# be defined before the filtered dataframe is created.

st.sidebar.title("Filters")
st.sidebar.markdown("---")

# state filter — multiselect lets users pick multiple states
all_states = sorted(df_facility["STATE"].unique())
selected_states = st.sidebar.multiselect(
    "Filter by State",
    options=all_states,
    default=all_states
)

# ownership type filter
ownership_types = sorted(
    df_facility["ownership_type"].dropna().unique()
)
selected_ownership = st.sidebar.multiselect(
    "Filter by Ownership Type",
    options=ownership_types,
    default=ownership_types
)

# checkbox filter — True or False
show_understaffed_only = st.sidebar.checkbox(
    "Show chronically understaffed only",
    value=False
)

st.sidebar.markdown("---")

# apply filters — runs every time a filter changes
df_filtered = df_facility[
    (df_facility["STATE"].isin(selected_states)) &
    (df_facility["ownership_type"].isin(selected_ownership))
].copy()

if show_understaffed_only:
    df_filtered = df_filtered[
        df_filtered["chronically_understaffed"] == True
    ]

# sidebar summary
st.sidebar.metric("Facilities shown", f"{len(df_filtered):,}")
st.sidebar.metric("States selected", f"{df_filtered['STATE'].nunique()}")
st.sidebar.caption(f"Quarter: {df_facility['quarter'].iloc[0]}")

# ── Stage 1: Title ────────────────────────────────────────────
st.title("🏥 Healthcare Staffing Metrics")
st.markdown("**CMS Nursing Home Staffing Data — Q2 2024**")
st.markdown("---")

# ── Stage 4: KPI cards ────────────────────────────────────────
# st.columns(4) divides the page into 4 equal columns.
# delta_color="normal" → green if positive, red if negative
# delta_color="off"    → always grey (no color judgment)

st.subheader("Pipeline Summary — Q2 2024")

col1, col2, col3, col4 = st.columns(4)

with col1:
    pct_meeting = (
        df_filtered["pct_days_meeting_cms"] >= 50
    ).mean() * 100
    st.metric(
        label="Facilities Meeting CMS Standards",
        value=f"{pct_meeting:.1f}%",
        delta=f"of {len(df_filtered):,} facilities",
        delta_color="off"
    )

with col2:
    chronic = int(df_filtered["chronically_understaffed"].sum())
    pct_chronic = chronic / len(df_filtered) * 100
    st.metric(
        label="Chronically Understaffed",
        value=f"{chronic:,}",
        delta=f"{pct_chronic:.1f}% of filtered facilities",
        delta_color="off"
    )

with col3:
    avg_cna = df_filtered["avg_CNA_hrs_per_patient"].mean()
    diff_cna = avg_cna - 2.45
    st.metric(
        label="Avg CNA Hrs / Patient / Day",
        value=f"{avg_cna:.2f}",
        delta=f"{diff_cna:+.2f} vs CMS min (2.45)",
        delta_color="normal"
    )

with col4:
    avg_rn = df_filtered["avg_RN_hrs_per_patient"].mean()
    diff_rn = avg_rn - 0.55
    st.metric(
        label="Avg RN Hrs / Patient / Day",
        value=f"{avg_rn:.2f}",
        delta=f"{diff_rn:+.2f} vs CMS min (0.55)",
        delta_color="normal"
    )

st.markdown("---")

col5, col6, col7, col8 = st.columns(4)

with col5:
    avg_total = df_filtered["avg_total_hrs_per_patient"].mean()
    diff_total = avg_total - 3.48
    st.metric(
        label="Avg Total Nurse Hrs / Patient",
        value=f"{avg_total:.2f}",
        delta=f"{diff_total:+.2f} vs CMS min (3.48)",
        delta_color="normal"
    )

with col6:
    avg_occupancy = df_filtered["avg_bed_occupancy_rate"].mean() * 100
    st.metric(
        label="Avg Bed Occupancy Rate",
        value=f"{avg_occupancy:.1f}%",
        delta=None
    )

with col7:
    avg_contracted = df_filtered["avg_contracted_rn_ratio"].mean() * 100
    st.metric(
        label="Avg Contracted RN Ratio",
        value=f"{avg_contracted:.1f}%",
        delta="% of RN hours from agency/contract",
        delta_color="off"
    )

with col8:
    avg_weekend_gap = df_filtered["weekend_staffing_gap"].mean()
    st.metric(
        label="Avg Weekend Staffing Gap",
        value=f"{avg_weekend_gap:+.2f} hrs",
        delta="vs weekday (negative = worse on weekends)",
        delta_color="off"
    )

st.markdown("---")
st.info("Charts coming in Stage 5...")

# ── Stage 5: Charts ───────────────────────────────────────────
# We use Plotly Express for all charts.
# st.plotly_chart() renders a Plotly figure in Streamlit.
# use_container_width=True makes the chart fill its column.

# ── Chart 1: Staffing vs Occupancy (Question 1) ───────────────
# Scatter plot — each dot is one facility
# X axis: bed occupancy rate
# Y axis: CNA hours per patient
# Color: chronically understaffed flag
# This answers: is there a relationship between how full a
# facility is and how well it's staffed?

st.subheader("Q1 — Staffing Levels vs Bed Occupancy Rate")
st.markdown(
    "Each point is one facility. "
    "Red = chronically understaffed (below CMS minimum >50% of days)."
)

fig1 = px.scatter(
    df_filtered,
    x="avg_bed_occupancy_rate",
    y="avg_CNA_hrs_per_patient",
    color="chronically_understaffed",
    color_discrete_map={True: "#ef4444", False: "#22c55e"},
    hover_data=["PROVNAME", "STATE", "ownership_type"],
    labels={
        "avg_bed_occupancy_rate": "Avg Bed Occupancy Rate",
        "avg_CNA_hrs_per_patient": "Avg CNA Hrs / Patient / Day",
        "chronically_understaffed": "Chronically Understaffed"
    },
    opacity=0.5,
    render_mode="svg"
)

# add CMS minimum line — horizontal reference line
fig1.add_hline(
    y=2.45,
    line_dash="dash",
    line_color="orange",
    annotation_text="CMS Min (2.45)",
    annotation_position="top right"
)

fig1.update_layout(height=450)
st.plotly_chart(fig1, use_container_width=True)

st.markdown("---")

# ── Chart 2: Contracted vs Employed Hours (Question 2) ────────
# Bar chart showing top 20 states by contracted RN ratio
# Answers: which states rely most on agency/contracted nurses?
# High contracted ratio = potential overtime cost concern

st.subheader("Q2 — Contracted vs Employed RN Hours by State")
st.markdown(
    "States with high contracted ratios rely heavily on agency nurses "
    "— typically more expensive than employed staff."
)

# aggregate to state level
df_state_contract = df_filtered.groupby("STATE").agg(
    avg_contracted=("avg_contracted_rn_ratio", "mean"),
    avg_employed=("total_employed_RN_hours", "sum"),
    avg_contract_hrs=("total_contracted_RN_hours", "sum"),
    facility_count=("PROVNUM", "count")
).reset_index()

df_state_contract["contracted_pct"] = (
    df_state_contract["avg_contracted"] * 100
).round(1)

df_state_contract = df_state_contract.sort_values(
    "contracted_pct", ascending=False
).head(20)

fig2 = px.bar(
    df_state_contract,
    x="STATE",
    y="contracted_pct",
    color="contracted_pct",
    color_continuous_scale="Reds",
    labels={
        "STATE": "State",
        "contracted_pct": "Avg Contracted RN Ratio (%)"
    },
    hover_data=["facility_count"]
)

fig2.update_layout(height=400, coloraxis_showscale=False)
st.plotly_chart(fig2, use_container_width=True)

st.markdown("---")

# ── Chart 3: Avg Staffing by State and Ownership (Question 3) ─
# Two charts side by side using st.columns()
# Left: CNA hours by state (top 15 and bottom 15)
# Right: CNA hours by ownership type

st.subheader("Q3 — Average Staffing by State and Ownership Type")

col_left, col_right = st.columns(2)

with col_left:
    st.markdown("**CNA Hours by State (bottom 15 — most understaffed)**")

    df_by_state = df_filtered.groupby("STATE").agg(
        avg_cna=("avg_CNA_hrs_per_patient", "mean"),
        facility_count=("PROVNUM", "count")
    ).reset_index().sort_values("avg_cna")

    # bottom 15 most understaffed states
    df_bottom_states = df_by_state.head(15)

    fig3a = px.bar(
        df_bottom_states,
        x="avg_cna",
        y="STATE",
        orientation="h",     # horizontal bar chart
        color="avg_cna",
        color_continuous_scale="RdYlGn",
        labels={
            "avg_cna": "Avg CNA Hrs / Patient",
            "STATE": "State"
        }
    )
    fig3a.add_vline(
        x=2.45,
        line_dash="dash",
        line_color="red",
        annotation_text="CMS Min"
    )
    fig3a.update_layout(height=450, coloraxis_showscale=False)
    st.plotly_chart(fig3a, use_container_width=True)

with col_right:
    st.markdown("**CNA Hours by Ownership Type**")

    df_by_ownership = df_filtered.groupby("ownership_type").agg(
        avg_cna=("avg_CNA_hrs_per_patient", "mean"),
        facility_count=("PROVNUM", "count")
    ).reset_index().sort_values("avg_cna")

    fig3b = px.bar(
        df_by_ownership,
        x="avg_cna",
        y="ownership_type",
        orientation="h",
        color="avg_cna",
        color_continuous_scale="RdYlGn",
        labels={
            "avg_cna": "Avg CNA Hrs / Patient",
            "ownership_type": "Ownership Type"
        },
        hover_data=["facility_count"]
    )
    fig3b.add_vline(
        x=2.45,
        line_dash="dash",
        line_color="red",
        annotation_text="CMS Min"
    )
    fig3b.update_layout(height=450, coloraxis_showscale=False)
    st.plotly_chart(fig3b, use_container_width=True)

st.markdown("---")

# ── Chart 4: Staffing trends over time (Question 4) ───────────
# Line chart using the daily staffing_metrics Gold table
# Shows average staffing levels change week by week
# Answers the time-series / trends question

st.subheader("Q4 — Staffing Trends Over Time (Q2 2024)")
st.markdown(
    "Weekly average staffing hours per patient across selected facilities."
)

# filter staffing data to match selected states
df_staffing_filtered = df_staffing[
    df_staffing["STATE"].isin(selected_states)
].copy()

# aggregate to weekly level — resample by week
# groupby WorkDate then resample
df_staffing_filtered["week"] = df_staffing_filtered[
    "WorkDate"
].dt.to_period("W").dt.start_time

df_weekly = df_staffing_filtered.groupby("week").agg(
    avg_cna=("CNA_hrs_per_patient", "mean"),
    avg_rn=("RN_hrs_per_patient", "mean"),
    avg_total=("total_hrs_per_patient", "mean"),
).reset_index()

# melt from wide to long format for plotly
# pandas melt = unpivot — turns multiple columns into rows
# needed for plotly to show multiple lines on one chart
df_weekly_long = df_weekly.melt(
    id_vars="week",
    value_vars=["avg_cna", "avg_rn", "avg_total"],
    var_name="Staff Type",
    value_name="Hrs per Patient"
)

# rename for cleaner legend labels
df_weekly_long["Staff Type"] = df_weekly_long["Staff Type"].map({
    "avg_cna":   "CNA",
    "avg_rn":    "RN",
    "avg_total": "Total Nurses"
})

fig4 = px.line(
    df_weekly_long,
    x="week",
    y="Hrs per Patient",
    color="Staff Type",
    labels={"week": "Week"},
    color_discrete_map={
        "CNA": "#3b82f6",
        "RN": "#22c55e",
        "Total Nurses": "#f59e0b"
    }
)

# add CMS minimum reference lines
fig4.add_hline(
    y=2.45, line_dash="dot", line_color="#3b82f6",
    annotation_text="CNA min", annotation_position="right"
)
fig4.add_hline(
    y=0.55, line_dash="dot", line_color="#22c55e",
    annotation_text="RN min", annotation_position="right"
)

fig4.update_layout(height=400)
st.plotly_chart(fig4, use_container_width=True)

st.markdown("---")

# ── Footer ────────────────────────────────────────────────────
st.caption(
    "Data source: CMS Nursing Home Staffing (PBJ) Q2 2024 | "
    "Pipeline: AWS Glue + Delta Lake on S3 | "
    "Dashboard: Streamlit"
)
