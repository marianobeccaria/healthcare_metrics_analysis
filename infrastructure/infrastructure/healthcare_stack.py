from aws_cdk import (
    Stack,
    aws_iam as iam,
)
from constructs import Construct

class HealthcareMetricsStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

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