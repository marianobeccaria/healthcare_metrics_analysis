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

