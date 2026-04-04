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

import aws_cdk as cdk
from infrastructure.healthcare_stack import HealthcareMetricsStack

app = cdk.App()

HealthcareMetricsStack(
    app,
    "HealthcareMetricsStack",
    env=cdk.Environment(
        account="858477419022",
        region="us-east-1"
    ),
    description="Healthcare Metrics Pipeline — Glue Workflow + Delta Lake on S3"
)

app.synth()
