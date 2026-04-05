#!/usr/bin/env python3
# ============================================================
# Healthcare Metrics Pipeline — CDK App Entry Point
# ============================================================
# HOW TO USE:
#   cdk synth    → preview all resources (no changes made)
#   cdk deploy   → create/update all AWS resources
#   cdk destroy  → tear down all resources when project is done
#
# ENVIRONMENT:
#   conda activate healthcare-cdk
# ============================================================
import os
from dotenv import load_dotenv
import aws_cdk as cdk
from infrastructure.healthcare_stack import HealthcareMetricsStack

load_dotenv()

app = cdk.App()

HealthcareMetricsStack(
    app,
    "HealthcareMetricsStack",
    env=cdk.Environment(
        account=os.environ.get("HEALTHCARE_ACCOUNT"),
        region=os.environ.get("HEALTHCARE_REGION", "us-east-1")
    ),
    description="Healthcare Metrics Pipeline — Glue Workflow + Delta Lake on S3"
)

app.synth()
