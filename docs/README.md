# Healthcare Metrics Project

An end-to-end data pipeline and analytics dashboard built on CMS nursing home
staffing data (PBJ Q2 2024), designed to surface insights on nurse availability,
patient load, facility performance, and care quality across U.S. facilities.

---

## Project Structure

```
healthcare-metrics/
├── data/
│   ├── raw/          ← original CSVs from CMS (not tracked by Git)
│   └── processed/    ← cleaned/transformed data (not tracked by Git)
├── notebooks/
│   ├── 01_eda.ipynb              ← Step 2: exploratory data analysis
│   └── 02_metrics.ipynb          ← Step 5: metric calculations
├── src/
│   ├── step2_inspect_pbj_data.py ← initial data inspection script
│   ├── pipeline/                 ← Step 4: ETL pipeline scripts
│   └── metrics/                  ← Step 5: metric calculation scripts
├── dashboard/
│   └── app.py                    ← Step 6: Streamlit dashboard
├── docs/
│   ├── architecture.md           ← Step 3: pipeline architecture writeup
│   └── data_dictionary.md        ← column reference guide
├── environment.yml               ← conda environment definition
├── .gitignore
└── README.md
```

---

## Data Sources

| File | Source | Description |
|------|--------|-------------|
| `PBJ_Daily_Nurse_Staffing_Q2_2024.csv` | CMS PBJ | Daily nursing hours per facility |
| `NH_ProviderInfo_*.csv` | CMS PDC | Facility details, ratings, bed counts |
| `NH_QualityMsr_MDS_*.csv` | CMS PDC | MDS quality measures per facility |
| `NH_QualityMsr_Claims_*.csv` | CMS PDC | Medicare claims quality measures |
| `FY_2024_SNF_VBP_*.csv` | CMS PDC | Value-based purchasing scores |
| *(+ 11 more supporting files)* | CMS PDC | Deficiencies, penalties, ownership |

> Raw data files are **not stored in this repository** (too large for GitHub).
> Download all CSV files from the link below and place them in `data/raw/`:
>
> 📁 **[Download Data from Google Drive](https://drive.google.com/drive/folders/15KqJ1MZ7JcgAkOfqcaWcALWkG0dh3jpE)**

---

## Setup

**1. Clone the repository**
```bash
git clone https://github.com/YOUR_USERNAME/healthcare-metrics.git
cd healthcare-metrics
```

**2. Create and activate the conda environment**
```bash
conda env create -f environment.yml
conda activate healthcare-metrics
```

**3. Add your data**
```
Place all CSV files into the data/raw/ folder.
```

**4. Run the initial inspection**
```bash
python src/step2_inspect_pbj_data.py
```

---

## Steps / Progress

| Step | Description | Status |
|------|-------------|--------|
| 1 | Source data download and verification | Done |
| 2 | Initial data analysis and EDA | In progress |
| 3 | Pipeline architecture design | Pending |
| 4 | Build data pipeline (AWS) | Pending |
| 5 | Define and calculate metrics | Pending |
| 6 | Build Streamlit dashboard | Pending |
| 7 | Documentation and submission | Pending |

---

## Key Metrics (planned)

- **Nurse-to-patient ratio** by hospital and state
- **Bed occupancy rate** vs certified capacity
- **Contracted vs employed** nursing hours ratio
- **Staffing vs CMS star rating** gap analysis
- **High-risk facilities** — high census, low staffing

---

## Tech Stack

| Layer | Tool | Why |
|-------|------|-----|
| Language | Python 3.11 | Industry standard for data work |
| Data processing | Pandas, NumPy | Fast, flexible tabular data manipulation |
| Visualization | Matplotlib, Seaborn | Charts for EDA and reporting |
| Dashboard | Streamlit | Fast Python-native dashboarding |
| Pipeline | AWS Glue + S3 | Scalable, serverless ETL on cloud |
| Warehouse | Amazon Redshift / Athena | SQL-queryable data lake gold layer |
| Orchestration | AWS Step Functions | Pipeline scheduling and monitoring |

---

## Data Join Key

All supporting CMS files connect to the main staffing file via:

```
PBJ file: PROVNUM  <-->  Supporting files: CMS Certification Number (CCN)
```

> Always keep this field as a **string** — it has leading zeros (e.g. `015009`)
> that will be silently dropped if converted to an integer.

---

## Environments

| File | Env name | Purpose |
|------|----------|---------|
| `environment.yml` | `ds4b_201_p` | Data analysis — pandas, numpy, EDA scripts |
| `environment_cdk.yml` | `healthcare-cdk` | Infrastructure deployment — CDK + all data libs |

To recreate the CDK environment from scratch:
```bash
conda env create -f environment_cdk.yml
conda activate healthcare-cdk
```
