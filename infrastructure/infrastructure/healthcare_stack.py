import os
from dotenv import load_dotenv
from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_glue as glue,
)
from constructs import Construct

# load .env file from the infrastructure/ folder
load_dotenv()


class HealthcareMetricsStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── CONFIG ────────────────────────────────────────────
        # Priority: CDK context flag → .env file → hardcoded default
        BUCKET_NAME = (
            self.node.try_get_context("bucket_name")
            or os.environ.get("HEALTHCARE_BUCKET")
            or "mbeccaria-dea-healthcare-metrics"
        )

        CREATE_BUCKET = (
            self.node.try_get_context("create_bucket")
            or os.environ.get("CREATE_BUCKET", "false")
        ) == "true"

        QUARTER = (
            self.node.try_get_context("quarter")
            or os.environ.get("HEALTHCARE_QUARTER")
            or "2024Q2"
        )
        # ──────────────────────────────────────────────────────

        # ── 1. IAM Role for Glue ──────────────────────────────
        glue_role = iam.Role(
            self, "GlueRole",
            role_name="healthcare-glue-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description="Glue service role for Healthcare Metrics pipeline",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ]
        )

        # ── 2. S3 bucket — create or import ───────────────────
        if CREATE_BUCKET:
            bucket = s3.Bucket(
                self, "HealthcareBucket",
                bucket_name=BUCKET_NAME,
                versioned=True,
                removal_policy=RemovalPolicy.RETAIN,
                block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                encryption=s3.BucketEncryption.S3_MANAGED,
            )
        else:
            bucket = s3.Bucket.from_bucket_name(
                self, "HealthcareBucket",
                bucket_name=BUCKET_NAME
            )

        # Grant Glue role read/write access regardless of which path taken
        bucket.grant_read_write(glue_role)

        # ── 3. S3 folder structure ────────────────────────────
        # Runs regardless of whether bucket was created or imported.
        # BucketDeployment is idempotent — if the folder already exists
        # it leaves it untouched. If it was deleted, it recreates it.
        # This means cdk deploy always self-heals missing folders.
        folders = [
            f"bronze/quarter={QUARTER}/",
            "silver/staffing/",
            "gold/staffing_metrics/",
            "gold/facility_summary/",
            "gold/quality_correlations/",
            "audit/unmatched_ccn/",
            "watermark/",
            "scripts/",
        ]

        for folder in folders:
            construct_id = (
                folder.replace("/", "-")
                      .replace("=", "-")
                      .strip("-")
            )
            s3deploy.BucketDeployment(
                self, f"Folder-{construct_id}",
                sources=[s3deploy.Source.data(
                    ".keep",    # placeholder filename
                    ""          # empty content
                )],
                destination_bucket=bucket,
                destination_key_prefix=folder,
                retain_on_delete=True,   # never delete folders on cdk destroy
                prune=False
            )

# ── 3. Glue Python Shell job — ingestion ──────────────
        # Lightweight Python job — no Spark, no cluster
        # Connects to Google Drive API, checks for new quarters,
        # downloads CSVs directly to S3 Bronze
        ingestion_job = glue.CfnJob(
            self, "IngestionJob",
            name="healthcare-ingestion",
            description="Download new CMS quarterly files from Google Drive to S3 Bronze",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",         # lightweight — no Spark cluster
                python_version="3.9",
                script_location=f"s3://{BUCKET_NAME}/scripts/glue_ingestion.py"
            ),
            default_arguments={
                "--BUCKET_NAME":  BUCKET_NAME,
                "--BRONZE_PATH":  f"s3://{BUCKET_NAME}/bronze/quarter={QUARTER}/",
                "--QUARTER":      QUARTER,
                "--job-language": "python",
                "--enable-job-insights": "true",
            },
            max_capacity=0.0625,    # 1/16 DPU — minimum for Python Shell
            max_retries=1,
            timeout=30,             # minutes
            glue_version="1.0",     # Python Shell jobs use Glue 1.0
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            )
        )

# ── 4. Glue Spark job — Bronze to Silver ─────────────
        # Full PySpark job — reads raw CSVs from Bronze, applies
        # all EDA-informed transformations, writes Delta Lake to Silver
        bronze_to_silver_job = glue.CfnJob(
            self, "BronzeToSilverJob",
            name="healthcare-bronze-to-silver",
            description="Clean, validate, join PBJ + ProviderInfo, write Delta Lake to Silver",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",             # Spark ETL — different from pythonshell
                python_version="3",
                script_location=f"s3://{BUCKET_NAME}/scripts/glue_bronze_to_silver.py"
            ),
            default_arguments={
                "--BUCKET_NAME":        BUCKET_NAME,
                "--BRONZE_PATH":        f"s3://{BUCKET_NAME}/bronze/quarter={QUARTER}/",
                "--SILVER_PATH":        f"s3://{BUCKET_NAME}/silver/staffing/",
                "--AUDIT_PATH":         f"s3://{BUCKET_NAME}/audit/unmatched_ccn/",
                "--QUARTER":            QUARTER,
                "--datalake-formats":   "delta",
                "--conf":               (
                    "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
                    " --conf spark.sql.catalog.spark_catalog="
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                ),
                "--enable-job-insights":              "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics":                   "true",
                "--job-language":                     "python",
            },
            glue_version="4.0",          # Glue 4.0 = Spark 3.3, Python 3.10
            worker_type="G.1X",          # 1 DPU per worker — sufficient for 1.3M rows
            number_of_workers=2,         # 2 workers = 2 DPU total
            max_retries=1,
            timeout=60,                  # minutes
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            )
        )

# ── 5. Glue Spark job — Silver to Gold ───────────────
        # Reads Silver Delta Lake, calculates all staffing metrics
        # against CMS thresholds, writes Gold Delta Lake tables
        silver_to_gold_job = glue.CfnJob(
            self, "SilverToGoldJob",
            name="healthcare-silver-to-gold",
            description="Calculate staffing metrics from Silver, write Delta Lake to Gold",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{BUCKET_NAME}/scripts/glue_silver_to_gold.py"
            ),
            default_arguments={
                "--BUCKET_NAME":        BUCKET_NAME,
                "--SILVER_PATH":        f"s3://{BUCKET_NAME}/silver/staffing/",
                "--GOLD_PATH":          f"s3://{BUCKET_NAME}/gold/",
                "--QUARTER":            QUARTER,
                "--datalake-formats":   "delta",
                "--conf":               (
                    "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
                    " --conf spark.sql.catalog.spark_catalog="
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                ),
                "--enable-job-insights":              "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics":                   "true",
                "--job-language":                     "python",
            },
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=2,
            max_retries=1,
            timeout=60,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            )
        )