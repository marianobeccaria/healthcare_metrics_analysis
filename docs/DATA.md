# Data Sources & Setup

This project uses publicly available CMS (Centers for Medicare & Medicaid Services)
nursing home staffing data. Raw files are **not committed to this repository** due to
size — download them using the link below.

---

## Download

📁 **[Google Drive — All Project Data Files](https://drive.google.com/drive/folders/15KqJ1MZ7JcgAkOfqcaWcALWkG0dh3jpE)**

After downloading, place all CSV files into the `data/raw/` folder:

```
healthcare-metrics/
└── data/
    └── raw/
        ├── PBJ_Daily_Nurse_Staffing_Q2_2024.csv   ← main file (~large)
        ├── NH_ProviderInfo_*.csv
        ├── NH_QualityMsr_MDS_*.csv
        ├── NH_QualityMsr_Claims_*.csv
        ├── NH_HealthCitations_*.csv
        ├── NH_FireSafetyCitations_*.csv
        ├── NH_SurveySummary_*.csv
        ├── NH_SurveyDates_*.csv
        ├── NH_Penalties_*.csv
        ├── NH_Ownership_*.csv
        ├── NH_StateUSAverages_*.csv
        ├── NH_CovidVaxProvider_*.csv
        ├── FY_2024_SNF_VBP_Facility_Performance.csv
        ├── Skilled_Nursing_Facility_Quality_Reporting_Program_Provider_Data_*.csv
        └── Skilled_Nursing_Facility_Quality_Reporting_Program_National_Data_*.csv
```

---

## File Descriptions

| File | Rows (approx) | Key Columns | Used For |
|------|--------------|-------------|----------|
| `PBJ_Daily_Nurse_Staffing_Q2_2024.csv` | ~3M+ | PROVNUM, WorkDate, MDScensus, Hrs_* | Core staffing metrics |
| `NH_ProviderInfo_*.csv` | ~15,000 | CCN, Certified beds, Overall rating, Ownership | Facility context, occupancy |
| `NH_QualityMsr_MDS_*.csv` | ~300,000 | CCN, Measure code, Q1–Q4 scores | Quality metrics (falls, ulcers) |
| `NH_QualityMsr_Claims_*.csv` | ~150,000 | CCN, Adjusted/Observed scores | Readmissions, ER visits |
| `FY_2024_SNF_VBP_*.csv` | ~15,000 | CCN, Performance score, Readmission rate | Value-based purchasing ranking |
| `NH_HealthCitations_*.csv` | ~200,000 | CCN, Deficiency tag, Severity | Inspection red flags |
| `NH_Penalties_*.csv` | ~50,000 | CCN, Fine amount, Penalty type | Compliance and fines |

---

## Join Key

All supporting files link to the main staffing file via:

```
PBJ file  ->  PROVNUM  (6-character string, e.g. "015009")
All other ->  CMS Certification Number / CCN  (same 6-character value)
```

> ⚠️ **Always load PROVNUM and CCN as strings in Python.**
> They contain leading zeros that are silently lost if loaded as integers.
> Use: `dtype={"PROVNUM": str, "CCN": str}` in `pd.read_csv()`

---

## Data Dictionary

Full column definitions for all files are documented in the official CMS data
dictionary, a copy of which is stored in this repo:

📄 `docs/NH_Data_Dictionary.pdf`

---

## Notes on Data Size

The main PBJ staffing file covers Q2 2024 (April–June) across ~15,000 facilities
with one row per facility per day, resulting in roughly 3–4 million rows.

When running scripts locally:
- Use `nrows=100_000` in `pd.read_csv()` during development
- Remove the `nrows` limit only when running final analysis
- On AWS (Step 4), the full file will be processed in the pipeline
