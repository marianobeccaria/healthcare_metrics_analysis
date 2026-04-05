import os
from dotenv import load_dotenv
from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
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