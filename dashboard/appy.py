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

# ── Title and header ──────────────────────────────────────────
# st.title() renders a large H1 heading
# st.markdown() renders markdown — **bold**, *italic*, etc.
# "---" renders a horizontal divider line
st.title("🏥 Healthcare Staffing Metrics")
st.markdown("**CMS Nursing Home Staffing Data — Q2 2024**")
st.markdown("---")

# ── Placeholder ───────────────────────────────────────────────
# st.info() renders a blue info box
# We use this as a placeholder until we add real content
st.info("Stage 1 complete — page config and title working.")