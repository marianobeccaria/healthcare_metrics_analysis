# Healthcare Metrics Pipeline

An end-to-end AWS data engineering pipeline and analytics dashboard built on
CMS nursing home staffing data (PBJ Q2 2024), designed to surface insights on
nurse availability, patient load, facility performance, and care quality across
U.S. nursing facilities.

**Dashboard:** Live on EC2 at port 8501
**Pipeline:** AWS Glue Workflow (4 jobs, Delta Lake on S3)
**Infrastructure:** Fully managed via AWS CDK — deploy or destroy with one command

---

## Architecture

```
Google Drive
     |
     v (Glue Workflow — quarterly schedule)
+--------------------------------------------------+
|  Job 1: Glue Python Shell — ingestion            |
|  Job 2: Glue Spark — Bronze to Silver            |
|  Job 3: Glue Spark — Silver to facility_summary  | (parallel)
|  Job 4: Glue Spark — Silver to staffing_metrics  | (parallel)
+--------------------------------------------------+
     |
     v
Streamlit Dashboard — EC2 t3.small
```

All infrastructure defined in `infrastructure/infrastructure/healthcare_stack.py`
and deployed via AWS CDK.

---

## Project Structure

```
healthcare-metrics/
├── data/
│   └── raw/                      <- CMS CSV files (not tracked by Git)
├── src/
│   └── pipeline/                 <- Glue PySpark ETL scripts
│       ├── glue_bronze_to_silver.py
│       ├── glue_silver_to_facility_summary.py
│       ├── glue_silver_to_staffing_metrics.py
│       └── glue_ingestion.py     <- Google Drive ingestion (placeholder)
├── dashboard/
│   └── app.py                    <- Streamlit dashboard
├── infrastructure/
│   ├── app.py                    <- CDK entry point
│   ├── infrastructure/
│   │   └── healthcare_stack.py   <- All AWS resources defined here
│   └── scripts/
│       └── ec2_setup.sh          <- EC2 bootstrap script
├── scripts/
│   └── create_q3_test_data.py    <- Test data generator for new quarters
├── docs/
│   └── architecture_design.md   <- Pipeline architecture (v2.2)
├── environment.yml               <- Data analysis conda env (ds4b_201_p)
├── environment_cdk.yml           <- CDK + infrastructure conda env (healthcare-cdk)
└── README.md
```

---

## Data Sources

| File | Description | Rows (Q2 2024) |
|------|-------------|---------------|
| PBJ_Daily_Nurse_Staffing_Q2_2024.csv | Daily nursing hours per facility | 1,325,324 |
| NH_ProviderInfo_*.csv | Facility details, ratings, bed counts | 14,814 |
| NH_QualityMsr_MDS_*.csv | MDS quality measures | ~300,000 |
| *(+ 18 additional supporting files)* | Deficiencies, penalties, ownership | varies |

> Raw data files are not stored in this repository.
> Download from Google Drive and upload to S3 Bronze:
>
> [Download Data from Google Drive](https://drive.google.com/drive/folders/15KqJ1MZ7JcgAkOfqcaWcALWkG0dh3jpE)

**Join key:** PROVNUM (PBJ) = CMS Certification Number CCN (supporting files)
Always load as STRING to preserve leading zeros (e.g. 015009)

---

## Setup

### Data Analysis Environment

```bash
conda env create -f environment.yml
conda activate p3.11.15
```

### CDK Infrastructure Environment

```bash
conda env create -f environment_cdk.yml
conda activate healthcare-cdk
cd infrastructure
```

### Deploy Infrastructure to AWS

```bash
conda activate healthcare-cdk
cd infrastructure
cdk deploy          # create all AWS resources
cdk destroy         # tear down everything when done
```

---

## Running the Pipeline

**Trigger full pipeline manually:**
```bash
aws glue start-workflow-run --name healthcare-metrics-pipeline
```

**Monitor workflow:**
```bash
aws glue get-workflow \
    --name healthcare-metrics-pipeline \
    --include-graph \
    --query "Workflow.LastRun.{Status:Status, Stats:Statistics}"
```

**Run individual jobs:**
```bash
aws glue start-job-run \
    --job-name healthcare-bronze-to-silver \
    --arguments '{
        "--BUCKET_NAME": "mbeccaria-dea-healthcare-metrics",
        "--BRONZE_PATH": "s3://mbeccaria-dea-healthcare-metrics/bronze/quarter=2024Q2/",
        "--SILVER_PATH": "s3://mbeccaria-dea-healthcare-metrics/silver/staffing/",
        "--AUDIT_PATH":  "s3://mbeccaria-dea-healthcare-metrics/audit/unmatched_ccn/",
        "--QUARTER":     "2024Q2"
    }'
```

---

## Running the Dashboard Locally

```bash
conda activate healthcare-cdk
cd dashboard
streamlit run app.py
```

Dashboard reads directly from Gold Delta Lake tables on S3 using your AWS credentials.

---

## Project Steps

| Step | Description | Status |
|------|-------------|--------|
| 1 | Source data download and S3 upload | Done |
| 2 | EDA — data quality, join analysis, CMS thresholds | Done |
| 3 | Architecture design (v2.2 — SME approved) | Done |
| 4 | CDK infrastructure + Glue PySpark scripts | Done |
| 5 | Metrics calculated in Gold layer | Done |
| 6 | Streamlit dashboard — live on EC2 | Done |
| 7 | Documentation and submission | In progress |

---

## Key Findings

- Only **24.5%** of facility-days meet CMS minimum staffing thresholds
- **90.9%** of facilities are chronically understaffed
- Average CNA hours **(2.11)** fall below CMS minimum **(2.45)** nationally
- **Government facilities** consistently outperform for-profit ownership types
- **Weekend staffing** is worse than weekday by -0.31 hrs on average
- **Texas** had 15 facilities chronically understaffed for the entire Q2 2024 quarter

---

## CMS Minimum Staffing Thresholds (2024 Rule)

| Staff Type | CMS Minimum |
|-----------|-------------|
| CNA | 2.45 hrs/patient/day |
| RN | 0.55 hrs/patient/day |
| Total nurses | 3.48 hrs/patient/day |

---

## Tech Stack

| Layer | Tool |
|-------|------|
| Language | Python 3.11 |
| ETL | AWS Glue (PySpark 3.3) |
| Storage | Amazon S3 + Delta Lake |
| Orchestration | AWS Glue Workflow + Triggers |
| Infrastructure | AWS CDK (Python) |
| Dashboard | Streamlit on EC2 t3.small |
| Monitoring | AWS CloudWatch |

---

## Environments

| File | Env name | Purpose |
|------|----------|---------|
| environment.yml | ds4b_201_p | Data analysis — pandas, numpy, EDA |
| environment_cdk.yml | healthcare-cdk | CDK + infrastructure deployment |