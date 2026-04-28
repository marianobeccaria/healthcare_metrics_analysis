# %%
# ============================================================
# STEP 2 — Load & Inspect PBJ Daily Nurse Staffing Data
# Healthcare Metrics Project
# ============================================================
# HOW TO RUN:
#   1. Place this file in the same folder as your CSV
#   2. Update FILE_PATH below to point to your CSV
#   3. Run: python step2_inspect_pbj_data.py
#
# REQUIREMENTS:
#   pip install pandas matplotlib seaborn
# ============================================================

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import os

# ── CONFIG ────────────────────────────────────────────────────
# Update this path to where your CSV file lives on your machine
FILE_PATH = "../data/raw/PBJ_Daily_Nurse_Staffing_Q2_2024.csv"

# To avoid loading the entire file at once (it's large!),
# we'll load a sample first, then give you the option to load all
SAMPLE_ROWS = 100_000   # start with 100k rows — fast and safe

# Path to NH_ProviderInfo supporting file — update to your file location
# Set to None to skip the join test section entirely
PROVIDER_INFO_PATH = "../data/raw/NH_ProviderInfo_Oct2024.csv"
# ──────────────────────────────────────────────────────────────


# ============================================================
# SECTION 1 — LOAD THE DATA
# ============================================================
print("\n" + "="*60)
print("SECTION 1: LOADING DATA")
print("="*60)

print(f"\nLoading first {SAMPLE_ROWS:,} rows from:\n  {FILE_PATH}\n")

try:
    # Read the CSV — keep PROVNUM as string to preserve leading zeros!
    df = pd.read_csv(
        FILE_PATH,
        #nrows=SAMPLE_ROWS,
        dtype={"PROVNUM": str, "COUNTY_FIPS": str},  # never convert IDs to numbers
        parse_dates=["WorkDate"],         # automatically parse the date column
        encoding="latin-1"
    )
    print(f"✓ Loaded successfully!")
    print(f"  Rows loaded : {len(df):,}")
    print(f"  Columns     : {len(df.columns)}")

except FileNotFoundError:
    print(f"✗ ERROR: File not found at '{FILE_PATH}'")
    print("  → Update FILE_PATH at the top of this script to match your file location.")
    exit()


# ============================================================
# SECTION 2 — BASIC SHAPE & COLUMN OVERVIEW
# ============================================================
print("\n" + "="*60)
print("SECTION 2: SHAPE & COLUMN OVERVIEW")
print("="*60)

print(f"\nDataset shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

print("\nColumn names and data types:")
print("-" * 45)
for col in df.columns:
    print(f"  {col:<30} {str(df[col].dtype)}")


# ============================================================
# SECTION 3 — FIRST LOOK AT THE DATA
# ============================================================
print("\n" + "="*60)
print("SECTION 3: FIRST 5 ROWS")
print("="*60)

# Set pandas to show all columns without truncating
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
pd.set_option("display.float_format", "{:.2f}".format)

print(df.head())


# ============================================================
# SECTION 4 — KEY IDENTIFIER CHECKS
# ============================================================
print("\n" + "="*60)
print("SECTION 4: KEY IDENTIFIER CHECKS")
print("="*60)

# How many unique facilities?
n_facilities = df["PROVNUM"].nunique()
print(f"\nUnique facilities (PROVNUM) : {n_facilities:,}")

# How many unique states?
n_states = df["STATE"].nunique()
print(f"Unique states              : {n_states}")
print(f"States in sample           : {sorted(df['STATE'].unique())}")

# Date range
print(f"\nDate range in data:")
print(f"  Earliest WorkDate : {df['WorkDate'].min().date()}")
print(f"  Latest WorkDate   : {df['WorkDate'].max().date()}")
print(f"  Quarter           : {df['CY_Qtr'].unique()}")

# Quick check: is there one row per facility per day?
dupe_check = df.groupby(["PROVNUM", "WorkDate"]).size()
duplicates = dupe_check[dupe_check > 1]
if len(duplicates) == 0:
    print("\n✓ No duplicate PROVNUM + WorkDate combinations found.")
else:
    print(f"\n⚠ WARNING: {len(duplicates):,} duplicate PROVNUM+WorkDate combos found!")
    print("  This means some facilities have more than one row per day.")
    print("  Sample duplicates:")
    print(duplicates.head())


# ============================================================
# SECTION 5 — MISSING VALUES
# ============================================================
print("\n" + "="*60)
print("SECTION 5: MISSING VALUES")
print("="*60)

missing = df.isnull().sum()
missing_pct = (missing / len(df) * 100).round(2)
missing_report = pd.DataFrame({
    "Missing Count": missing,
    "Missing %": missing_pct
}).query("`Missing Count` > 0").sort_values("Missing %", ascending=False)

if missing_report.empty:
    print("\n✓ No missing (null) values found in any column.")
else:
    print(f"\nColumns with missing values:")
    print(missing_report.to_string())


# ============================================================
# SECTION 6 — ZERO VALUE ANALYSIS (nursing hours)
# ============================================================
print("\n" + "="*60)
print("SECTION 6: ZERO VALUE ANALYSIS (nursing hours columns)")
print("="*60)

# In this dataset, 0 often means "no hours worked" — NOT a missing value
# But we need to know how often it happens per column
hours_cols = [c for c in df.columns if c.startswith("Hrs_")]
print(f"\nAnalyzing {len(hours_cols)} nursing hours columns...\n")

zero_report = []
for col in hours_cols:
    zeros = (df[col] == 0).sum()
    pct = zeros / len(df) * 100
    zero_report.append({
        "Column": col,
        "Zero Count": zeros,
        "Zero %": round(pct, 1),
        "Mean (non-zero)": round(df[col][df[col] > 0].mean(), 2) if zeros < len(df) else 0
    })

zero_df = pd.DataFrame(zero_report).sort_values("Zero %", ascending=False)
print(zero_df.to_string(index=False))

print("\n TIP: Columns with very high Zero % (like Hrs_MedAide, Hrs_NAtrn)")
print("   are common because not all facilities use those staff types.")
print("   This is normal — NOT a data quality issue.")


# ============================================================
# SECTION 7 — BASIC STATISTICS FOR KEY COLUMNS
# ============================================================
print("\n" + "="*60)
print("SECTION 7: STATISTICS FOR KEY COLUMNS")
print("="*60)

key_cols = ["MDScensus", "Hrs_RN", "Hrs_LPN", "Hrs_CNA"]
print("\nDescriptive statistics (count, mean, min, max, etc.):\n")
print(df[key_cols].describe().round(2).to_string())

# Flag impossible values (e.g. a single day can't have > 24 hours per person)
# For a whole facility, very high numbers could be valid — but let's flag extremes
print("\n\nChecking for suspicious outlier values...")
print("-" * 45)
for col in key_cols[1:]:   # skip MDScensus
    max_val = df[col].max()
    n_over_500 = (df[col] > 500).sum()
    print(f"  {col:<15} max={max_val:>8.1f}   rows > 500hrs: {n_over_500}")

print("\n TIP: Values over 500 hours in a single day for one facility")
print("   are worth investigating — they may be data entry errors.")


# ============================================================
# SECTION 8 — VISUALIZATIONS (saved to PNG files)
# ============================================================
print("\n" + "="*60)
print("SECTION 8: GENERATING VISUALIZATIONS")
print("="*60)

OUTPUT_DIR = "step2_charts"
os.makedirs(OUTPUT_DIR, exist_ok=True)

sns.set_theme(style="whitegrid", palette="muted")

# --- Chart 1: Facilities per state ---
print("\n  Generating chart 1: Facilities per state...")
fac_per_state = (
    df.groupby("STATE")["PROVNUM"]
    .nunique()
    .sort_values(ascending=False)
    .head(20)
    .reset_index()
)
fac_per_state.columns = ["State", "Facilities"]

fig, ax = plt.subplots(figsize=(12, 5))
sns.barplot(data=fac_per_state, x="State", y="Facilities", ax=ax, color="#5B8DB8")
ax.set_title("Top 20 States by Number of Facilities (sample)", fontsize=13)
ax.set_xlabel("State")
ax.set_ylabel("Unique Facilities")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/chart1_facilities_per_state.png", dpi=150)
plt.close()
print(f"  ✓ Saved to {OUTPUT_DIR}/chart1_facilities_per_state.png")

# --- Chart 2: Distribution of patient census ---
print("\n  Generating chart 2: Patient census distribution...")
fig, ax = plt.subplots(figsize=(10, 4))
df["MDScensus"].clip(upper=300).hist(bins=50, ax=ax, color="#5B8DB8", edgecolor="white")
ax.set_title("Distribution of Daily Patient Census (MDScensus, capped at 300)", fontsize=13)
ax.set_xlabel("Number of Patients")
ax.set_ylabel("Number of Days (rows)")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x)}"))
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/chart2_patient_census_distribution.png", dpi=150)
plt.close()
print(f"  ✓ Saved to {OUTPUT_DIR}/chart2_patient_census_distribution.png")

# --- Chart 3: Average nursing hours by type ---
print("\n  Generating chart 3: Average hours by nurse type...")
avg_hours = {
    "RN": df["Hrs_RN"].mean(),
    "LPN": df["Hrs_LPN"].mean(),
    "CNA": df["Hrs_CNA"].mean(),
    "RN Admin": df["Hrs_RNadmin"].mean(),
    "LPN Admin": df["Hrs_LPNadmin"].mean(),
    "Med Aide": df["Hrs_MedAide"].mean(),
    "NA Trainee": df["Hrs_NAtrn"].mean(),
}
fig, ax = plt.subplots(figsize=(10, 4))
colors = ["#5B8DB8", "#6BAF92", "#E8A85F", "#A87CB8", "#D4736B", "#7BBFB5", "#C2A96B"]
bars = ax.bar(avg_hours.keys(), avg_hours.values(), color=colors, edgecolor="white")
ax.set_title("Average Daily Hours per Facility by Nurse Type (sample)", fontsize=13)
ax.set_xlabel("Nurse Type")
ax.set_ylabel("Average Hours")
for bar, val in zip(bars, avg_hours.values()):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
            f"{val:.1f}", ha="center", va="bottom", fontsize=9)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/chart3_avg_hours_by_nurse_type.png", dpi=150)
plt.close()
print(f"  ✓ Saved to {OUTPUT_DIR}/chart3_avg_hours_by_nurse_type.png")

# --- Chart 4: Zero % by hours column (heatmap-style bar) ---
print("\n  Generating chart 4: Zero % per hours column...")
fig, ax = plt.subplots(figsize=(12, 4))
colors_zero = ["#D4736B" if z > 80 else "#E8A85F" if z > 40 else "#6BAF92"
               for z in zero_df["Zero %"]]
ax.barh(zero_df["Column"], zero_df["Zero %"], color=colors_zero, edgecolor="white")
ax.set_title("Percentage of Zero Values per Nursing Hours Column", fontsize=13)
ax.set_xlabel("% of rows that are zero")
ax.axvline(x=50, color="gray", linestyle="--", linewidth=0.8, label="50% line")
ax.legend(fontsize=9)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/chart4_zero_pct_by_column.png", dpi=150)
plt.close()
print(f"  ✓ Saved to {OUTPUT_DIR}/chart4_zero_pct_by_column.png")


# ============================================================
# SECTION 9 — SUMMARY REPORT
# ============================================================
print("\n" + "="*60)
print("SECTION 9: SUMMARY — WHAT TO NOTE FOR STEP 3")
print("="*60)

print(f"""
KEY FINDINGS FROM THIS INSPECTION:
───────────────────────────────────────────────────────────
  Rows loaded       : {len(df):,} (sample of {SAMPLE_ROWS:,})
  Unique facilities : {n_facilities:,}
  States covered    : {n_states}
  Date range        : {df['WorkDate'].min().date()} → {df['WorkDate'].max().date()}
  Duplicate rows    : {"None found ✓" if len(duplicates) == 0 else f"{len(duplicates)} found "}
  Null values       : {"None found ✓" if missing_report.empty else f"{missing.sum()} total "}

COLUMN GROUPS:
  Identifier cols   : PROVNUM, PROVNAME, CITY, STATE, COUNTY_NAME, COUNTY_FIPS
  Time cols         : CY_Qtr, WorkDate
  Patient volume    : MDScensus
  Nursing hours     : {len(hours_cols)} columns (Hrs_RN, Hrs_LPN, Hrs_CNA, etc.)

JOIN KEY FOR STEP 3:
  PROVNUM (this file) = CCN (NH_ProviderInfo and all other supporting files)
  Always keep PROVNUM as a STRING — never convert to integer!

───────────────────────────────────────────────────────────
Charts saved to: ./{OUTPUT_DIR}/
""")

# ============================================================
# SECTION 10 — CATEGORIZE ROWS INTO STAFFING TIERS
# ============================================================
#  Categorize all rows into staffing tiers
#  for TOP understaffing facilities
# ============================================================

print("\n" + "="*60)
print("SECTION 10: CATEGORIZE ROWS INTO STAFFING TIERS")
print("="*60)

df["CNA_hrs_per_patient"] = df["Hrs_CNA"] / df["MDScensus"].replace(0, pd.NA)

def staffing_tier(row):
    if row["MDScensus"] < 10:
        return "exclude_low_census"       # reopening / edge case
    ratio = row["CNA_hrs_per_patient"]
    if pd.isna(ratio):
        return "exclude_zero_census"
    if ratio == 0:
        return "critical_no_staff"        # zero CNA hours reported
    if ratio < 1:
        return "critical_understaffed"    # below 1 hr/patient
    if ratio < 2.45:
        return "below_cms_minimum"        # below CMS recommended floor
    if ratio >= 2.45:
        return "meets_cms_minimum"

# go through the dataframe and call  function repeatedly    
df["staffing_tier"] = df.apply(staffing_tier, axis=1)

# summary
print(df["staffing_tier"].value_counts())
print(f"\n% of days meeting CMS minimum: "
      f"{(df['staffing_tier']=='meets_cms_minimum').sum() / len(df) * 100:.1f}%")

# chronic offenders — facilities below CMS minimum on most days
chronic = (df[df["staffing_tier"].isin(["critical_no_staff", "critical_understaffed", "below_cms_minimum"])]
           .groupby(["PROVNUM", "PROVNAME", "STATE"])
           .agg(
               days_below_minimum=("WorkDate", "count"),
               avg_CNA_ratio=("CNA_hrs_per_patient", "mean"),
               avg_patients=("MDScensus", "mean")
           )
           .sort_values("days_below_minimum", ascending=False)
           .reset_index())

print("\nTop chronic understaffing facilities:")
print(chronic.head(15).to_string(index=False))


# ============================================================
# SECTION 11 — SUPPORTING FILE JOIN TEST
# ============================================================
# PURPOSE: Validate that PROVNUM in the main file matches CCN
# in supporting files before building the pipeline.
# Reusable: update PROVIDER_INFO_PATH in CONFIG to point to
# any supporting file that uses CCN as its join key.
# ============================================================
print("\n" + "="*60)
print("SECTION 11: SUPPORTING FILE JOIN TEST (NH_ProviderInfo)")
print("="*60)

if PROVIDER_INFO_PATH is None:
    print("\n Skipped - set PROVIDER_INFO_PATH in CONFIG to run this section.")
else:
    try:
        print(f"\nLoading: {PROVIDER_INFO_PATH}")

        # CCN must be loaded as string - same reason as PROVNUM
        provider_info = pd.read_csv(
            PROVIDER_INFO_PATH,
            dtype={"CMS Certification Number (CCN)": str}
        )
        print(f"Loaded {len(provider_info):,} rows, {len(provider_info.columns)} columns")

        # ── Join test ───────────────────────────────────────────
        merged = df.merge(
            provider_info,
            left_on="PROVNUM",
            right_on="CMS Certification Number (CCN)",
            how="left"       # keep ALL rows from PBJ, match where possible
        )

        pbj_facilities  = df["PROVNUM"].nunique()
        matched         = merged[merged["CMS Certification Number (CCN)"].notna()]["PROVNUM"].nunique()
        unmatched       = merged[merged["CMS Certification Number (CCN)"].isna()]["PROVNUM"].nunique()
        match_pct       = matched / pbj_facilities * 100

        print(f"\nJoin results (PROVNUM <-> CCN):")
        print(f"  PBJ facilities total     : {pbj_facilities:,}")
        print(f"  Matched in ProviderInfo  : {matched:,}  ({match_pct:.1f}%)")
        print(f"  Unmatched (no match)     : {unmatched:,}")

        # ── Show unmatched PROVNUMs if any ─────────────────────
        if unmatched > 0:
            unmatched_ids = (
                merged[merged["CMS Certification Number (CCN)"].isna()]["PROVNUM"]
                .unique()
            )
            print(f"\nWARNING: Unmatched PROVNUM samples (first 10):")
            print(f"  {list(unmatched_ids[:10])}")
            print(f"\n  These facilities exist in PBJ but not in ProviderInfo.")
            print(f"  Possible reasons: recently opened, closed, or CCN format mismatch.")
            print(f"  Investigate before including in pipeline metrics.")
        else:
            print(f"\nAll PBJ facilities matched in ProviderInfo - join is clean!")

        # ── Preview joined columns ──────────────────────────────
        useful_cols = [
            "PROVNUM", "PROVNAME", "STATE",
            "CMS Certification Number (CCN)",
            "Number of Certified Beds",
            "Overall Rating",
            "Ownership Type",
            "Total nursing staff turnover",
            "Reported RN Staffing Hours per Resident per Day"
        ]

        available    = [c for c in useful_cols if c in merged.columns]
        missing_cols = [c for c in useful_cols if c not in merged.columns]

        print(f"\nKey columns available after join:")
        for c in available:
            print(f"  OK: {c}")

        if missing_cols:
            print(f"\nExpected columns NOT found in this file version:")
            for c in missing_cols:
                print(f"  MISSING: {c}  <- check column name in your CSV header")

        print(f"\nSample of joined data (first 3 rows, key columns):")
        print(merged[available].head(3).to_string(index=False))

        print(f"""
JOIN TEST SUMMARY FOR ARCHITECTURE PLANNING:
-----------------------------------------------------------
  Join type used    : LEFT JOIN (PBJ is the primary table)
  Join key          : PROVNUM = CMS Certification Number (CCN)
  Match rate        : {match_pct:.1f}%
  Unmatched rows    : {unmatched:,} facilities need investigation
  Columns gained    : bed count, star rating, ownership type, turnover
-----------------------------------------------------------
""")

    except FileNotFoundError:
        print(f"\nERROR: File not found at '{PROVIDER_INFO_PATH}'")
        print("  Update PROVIDER_INFO_PATH in CONFIG at the top of this script.")


# ============================================================
# SECTION 12 — CORRELATIONS BETWEEN STAFFING AND QUALIT MEASURES
# ============================================================
# PURPOSE: MDS Quality Measures file alongside staffing data to find correlations
# ============================================================
print("\n" + "="*60)
print("SECTION 12: CORRELATIONS BETWEEN STAFFING AND QUALIT MEASURES")
print("="*60)


# ── load your files ───────────────────────────────────────────
# raw_df = pd.read_csv(
#     "../data/raw/PBJ_Daily_Nurse_Staffing_Q2_2024.csv",
#     dtype={"PROVNUM": str, "COUNTY_FIPS": str},
#     parse_dates=["WorkDate"],
#     encoding="latin-1"
# )

raw_df = df

# ── calculate per-patient ratios first (on daily rows) ────────
raw_df["CNA_hrs_per_patient"]   = raw_df["Hrs_CNA"] / raw_df["MDScensus"].replace(0, pd.NA)
raw_df["RN_hrs_per_patient"]    = raw_df["Hrs_RN"]  / raw_df["MDScensus"].replace(0, pd.NA)
raw_df["total_hrs_per_patient"] = (
    (raw_df["Hrs_RN"] + raw_df["Hrs_LPN"] + raw_df["Hrs_CNA"])
    / raw_df["MDScensus"].replace(0, pd.NA)
)

# ── aggregate to one row per facility ─────────────────────────
# use .copy() so the aggregated df is independent from raw_df
staffing_by_facility = (
    raw_df[raw_df["MDScensus"] >= 10]   # exclude low census edge cases
    .groupby("PROVNUM")
    .agg(
        avg_CNA_ratio   = ("CNA_hrs_per_patient",   "mean"),
        avg_RN_ratio    = ("RN_hrs_per_patient",    "mean"),
        avg_total_ratio = ("total_hrs_per_patient", "mean"),
        avg_census      = ("MDScensus",             "mean"),
        days_in_quarter = ("WorkDate",              "count")
    )
    .reset_index()
)

print(f"Facility-level staffing table: {staffing_by_facility.shape}")
print(staffing_by_facility.head())

# %%