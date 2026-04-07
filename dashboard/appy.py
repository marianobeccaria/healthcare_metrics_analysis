# ============================================================
# Healthcare Metrics Dashboard
# Stage 1 — Page config and title
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

# ── Page config ───────────────────────────────────────────────
# st.set_page_config MUST be the very first Streamlit command.
# If you put anything else before it you get an error.
# layout="wide" uses the full browser width instead of
# a narrow centered column.
st.set_page_config(
    page_title="Healthcare Staffing Metrics",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Congig ────────────────────────────────────────────────────
# Read S3 paths from .env file
# or fallback to hardcoded defaults if .env not found
GOLD_FACILITY_PATH = os.environ.get(
    "GOLD_FACILITY_PATH",
    "s3://mbeccaria-dea-healthcare-metrics/gold/facility_summary/"
)
GOLD_STAFFING_PATH = os.environ.get(
    "GOLD_STAFFING_PATH",
    "s3://mbeccaria-dea-healthcare-metrics/gold/staffing_metrics/"
)

# ── Data loading ──────────────────────────────────────────────
# @st.cache_data caches the result so S3 is only read once
# per app session — not on every user interaction

@st.cache_data
def load_facility_data():
    """
    Load Gold facility summary table from S3.
    One row per facility per quarter.
    Contains aggregated staffing metrics and CMS compliance flags.
    """
    df = pd.read_parquet(
        GOLD_FACILITY_PATH,
        storage_options={"anon": False}  # use local AWS credentials
    )
    return df

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
    # convert WorkDate string to proper datetime
    # needed for time-series charts later
    df["WorkDate"] = pd.to_datetime(df["WorkDate"])
    return df

# ── Load data ─────────────────────────────────────────────────
# st.spinner() shows a loading message while the code
# inside the with block runs — gives user visual feedback
# that something is happening during the S3 read
with st.spinner("Loading data from S3..."):
    try:
        df_facility = load_facility_data()
        df_staffing = load_staffing_data()
        data_loaded = True
    except Exception as e:
        # st.error() shows a red error box
        st.error(f"Could not load data from S3: {e}")
        data_loaded = False

# st.stop() halts the script here if data failed to load
# nothing below this line runs if data_loaded is False
if not data_loaded:
    st.stop()

# ── Title and header ──────────────────────────────────────────
st.title("🏥 Healthcare Staffing Metrics")
st.markdown("**CMS Nursing Home Staffing Data — Q2 2024**")
st.markdown("---")

# ── Data confirmation ─────────────────────────────────────────
# st.success() shows a green confirmation box
# This confirms the data loaded correctly before we build charts
st.success(
    f"Data loaded successfully — "
    f"{len(df_facility):,} facilities across "
    f"{df_facility['STATE'].nunique()} states"
)

# show the first few rows so we can verify the data looks right
# st.expander() creates a collapsible section —
# click to expand, click again to collapse
with st.expander("Preview facility data (first 5 rows)"):
    st.dataframe(df_facility.head())

with st.expander("Preview staffing data (first 5 rows)"):
    st.dataframe(df_staffing.head())
