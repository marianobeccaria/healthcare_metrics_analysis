import os
from dotenv import load_dotenv
from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    CfnOutput,    
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_glue as glue,
    aws_logs as logs,
    aws_ec2 as ec2,
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
        # ── 3b. Upload EC2 setup script to S3 ────────────────
        # BucketDeployment keeps the script in sync —
        # any changes to ec2_setup.sh are uploaded on cdk deploy

        s3deploy.BucketDeployment(
            self, "EC2SetupScript",
            sources=[s3deploy.Source.asset(
                os.path.join(os.path.dirname(__file__), "..", "scripts")
            )],
            destination_bucket=bucket,
            destination_key_prefix="scripts/",
            retain_on_delete=True,
            prune=False
        )

        # ── 4. Glue Python Shell job — ingestion ──────────────
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

        # ── 5. Glue Spark job — Bronze to Silver ─────────────
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

        # ── 6. Glue Spark job — Silver to Gold ───────────────
        # Reads Silver Delta Lake, calculates all staffing metrics
        # against CMS thresholds, writes Gold Delta Lake tables
        # silver_to_gold_job = glue.CfnJob(
        #     self, "SilverToGoldJob",
        #     name="healthcare-silver-to-gold",
        #     description="Calculate staffing metrics from Silver, write Delta Lake to Gold",
        #     role=glue_role.role_arn,
        #     command=glue.CfnJob.JobCommandProperty(
        #         name="glueetl",
        #         python_version="3",
        #         script_location=f"s3://{BUCKET_NAME}/scripts/glue_silver_to_gold.py"
        #     ),
        #     default_arguments={
        #         "--BUCKET_NAME":        BUCKET_NAME,
        #         "--SILVER_PATH":        f"s3://{BUCKET_NAME}/silver/staffing/",
        #         "--GOLD_PATH":          f"s3://{BUCKET_NAME}/gold/",
        #         "--QUARTER":            QUARTER,
        #         "--datalake-formats":   "delta",
        #         "--conf":               (
        #             "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
        #             " --conf spark.sql.catalog.spark_catalog="
        #             "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        #         ),
        #         "--enable-job-insights":              "true",
        #         "--enable-continuous-cloudwatch-log": "true",
        #         "--enable-metrics":                   "true",
        #         "--job-language":                     "python",
        #     },
        #     glue_version="4.0",
        #     worker_type="G.1X",
        #     number_of_workers=2,
        #     max_retries=1,
        #     timeout=60,
        #     execution_property=glue.CfnJob.ExecutionPropertyProperty(
        #         max_concurrent_runs=1
        #     )
        # )

        # ── 6a. Glue Spark job — Silver to Facility Summary ──────
        silver_to_facility_job = glue.CfnJob(
            self, "SilverToFacilityJob",
            name="healthcare-silver-to-facility-summary",
            description="Aggregate Silver to facility level — write Gold facility_summary Delta table",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{BUCKET_NAME}/scripts/glue_silver_to_facility_summary.py"
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

        # ── 6b. Glue Spark job — Silver to Staffing Metrics ──────
        silver_to_staffing_job = glue.CfnJob(
            self, "SilverToStaffingJob",
            name="healthcare-silver-to-staffing-metrics",
            description="Write daily Silver rows to Gold staffing_metrics Delta table",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{BUCKET_NAME}/scripts/glue_silver_to_staffing_metrics.py"
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

        # ── 7. Glue Workflow ──────────────────────────────────
        # Single workflow that owns all three jobs.
        # Acts as the container — triggers are attached to it.
        workflow = glue.CfnWorkflow(
            self, "PipelineWorkflow",
            name="healthcare-metrics-pipeline",
            description="End-to-end pipeline: Google Drive → Bronze → Silver → Gold",
            max_concurrent_runs=1   # never run two full pipelines simultaneously
        )

        # ── 8. Glue Triggers ──────────────────────────────────
        # Trigger 1: Schedule — starts the ingestion job
        # Runs quarterly: 1st of Jan, Apr, Jul, Oct at 6am UTC
        schedule_trigger = glue.CfnTrigger(
            self, "ScheduleTrigger",
            name="healthcare-schedule-trigger",
            description="Quarterly schedule — starts ingestion job",
            workflow_name=workflow.name,
            type="SCHEDULED",
            schedule="cron(0 6 1 1,4,7,10 ? *)",
            start_on_creation=True,
            actions=[glue.CfnTrigger.ActionProperty(
                job_name=ingestion_job.name,
                timeout=30
            )]
        )
        # ensure workflow exists before triggers are created
        schedule_trigger.add_dependency(workflow)
        schedule_trigger.add_dependency(ingestion_job)

        # Trigger 2: Conditional — starts Bronze→Silver
        # only fires when ingestion job SUCCEEDS
        bronze_trigger = glue.CfnTrigger(
            self, "BronzeTrigger",
            name="healthcare-bronze-trigger",
            description="Starts Bronze to Silver after ingestion succeeds",
            workflow_name=workflow.name,
            type="CONDITIONAL",
            start_on_creation=True,
            predicate=glue.CfnTrigger.PredicateProperty(
                logical="AND",
                conditions=[glue.CfnTrigger.ConditionProperty(
                    job_name=ingestion_job.name,
                    logical_operator="EQUALS",
                    state="SUCCEEDED"
                )]
            ),
            actions=[glue.CfnTrigger.ActionProperty(
                job_name=bronze_to_silver_job.name,
                timeout=60
            )]
        )
        bronze_trigger.add_dependency(workflow)
        bronze_trigger.add_dependency(ingestion_job)
        bronze_trigger.add_dependency(bronze_to_silver_job)

        # Trigger 3: Conditional — starts Silver→Gold
        # only fires when Bronze→Silver SUCCEEDS
        
        # gold_trigger = glue.CfnTrigger(
        #     self, "GoldTrigger",
        #     name="healthcare-gold-trigger",
        #     description="Starts Silver to Gold after Bronze to Silver succeeds",
        #     workflow_name=workflow.name,
        #     type="CONDITIONAL",
        #     start_on_creation=True,
        #     predicate=glue.CfnTrigger.PredicateProperty(
        #         logical="AND",
        #         conditions=[glue.CfnTrigger.ConditionProperty(
        #             job_name=bronze_to_silver_job.name,
        #             logical_operator="EQUALS",
        #             state="SUCCEEDED"
        #         )]
        #     ),
        #     actions=[glue.CfnTrigger.ActionProperty(
        #         job_name=silver_to_gold_job.name,
        #         timeout=60
        #     )]
        # )
        # gold_trigger.add_dependency(workflow)
        # gold_trigger.add_dependency(bronze_to_silver_job)
        # gold_trigger.add_dependency(silver_to_gold_job)


        # Trigger 3a: Conditional — starts Silver→FacilitySummary
        # fires in parallel with Trigger 3b after Bronze→Silver succeeds
        facility_trigger = glue.CfnTrigger(
            self, "FacilityTrigger",
            name="healthcare-facility-trigger",
            description="Starts Silver to Facility Summary after Bronze to Silver succeeds",
            workflow_name=workflow.name,
            type="CONDITIONAL",
            start_on_creation=True,
            predicate=glue.CfnTrigger.PredicateProperty(
                logical="AND",
                conditions=[glue.CfnTrigger.ConditionProperty(
                    job_name=bronze_to_silver_job.name,
                    logical_operator="EQUALS",
                    state="SUCCEEDED"
                )]
            ),
            actions=[glue.CfnTrigger.ActionProperty(
                job_name=silver_to_facility_job.name,
                timeout=60
            )]
        )
        facility_trigger.add_dependency(workflow)
        facility_trigger.add_dependency(bronze_to_silver_job)
        facility_trigger.add_dependency(silver_to_facility_job)

        # Trigger 3b: Conditional — starts Silver→StaffingMetrics
        # fires in parallel with Trigger 3a after Bronze→Silver succeeds
        staffing_trigger = glue.CfnTrigger(
            self, "StaffingTrigger",
            name="healthcare-staffing-trigger",
            description="Starts Silver to Staffing Metrics after Bronze to Silver succeeds",
            workflow_name=workflow.name,
            type="CONDITIONAL",
            start_on_creation=True,
            predicate=glue.CfnTrigger.PredicateProperty(
                logical="AND",
                conditions=[glue.CfnTrigger.ConditionProperty(
                    job_name=bronze_to_silver_job.name,
                    logical_operator="EQUALS",
                    state="SUCCEEDED"
                )]
            ),
            actions=[glue.CfnTrigger.ActionProperty(
                job_name=silver_to_staffing_job.name,
                timeout=60
            )]
        )
        staffing_trigger.add_dependency(workflow)
        staffing_trigger.add_dependency(bronze_to_silver_job)
        staffing_trigger.add_dependency(silver_to_staffing_job)

        # ── 9. CloudWatch Log Groups ──────────────────────────
        # One log group per Glue job.
        # Glue writes logs here automatically when jobs run.
        # Retained for 30 days then auto-deleted to control costs.

        # from aws_cdk import aws_logs as logs

        log_groups = [
            ("IngestionLogs",      "/aws-glue/healthcare-ingestion"),
            ("BronzeToSilverLogs", "/aws-glue/healthcare-bronze-to-silver"),
            # ("SilverToGoldLogs",   "/aws-glue/healthcare-silver-to-gold"),
            ("FacilitySummaryLogs",   "/aws-glue/healthcare-silver-to-facility-summary"),
            ("StaffingMetricsLogs",   "/aws-glue/healthcare-silver-to-staffing-metrics"),
        ]

        for construct_id, log_group_name in log_groups:
            logs.LogGroup(
                self, construct_id,
                log_group_name=log_group_name,
                retention=logs.RetentionDays.ONE_MONTH,
                removal_policy=RemovalPolicy.DESTROY
            )

        # ── 10. EC2 — Streamlit Dashboard ─────────────────────
        # t3.small is sufficient for Streamlit serving Gold data
        # from S3

        # ── 10a. VPC — use default VPC ────────────────────────
        # Use the default VPC 
        vpc = ec2.Vpc.from_lookup(
            self, "DefaultVpc",
            is_default=True
        )

        # ── 10b. Security group ────────────────────────────────
        # Controls traffic that can reach the EC2 instance
        dashboard_sg = ec2.SecurityGroup(
            self, "DashboardSG",
            vpc=vpc,
            description="Security group for Healthcare Metrics Streamlit dashboard",
            allow_all_outbound=True     # EC2 can reach S3 and internet
        )

        # allow SSH from anywhere — for management
        dashboard_sg.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(22),
            description="SSH access"
        )

        # allow Streamlit port from anywhere — for dashboard access
        dashboard_sg.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(8501),
            description="Streamlit dashboard access"
        )

        # ── 10c. IAM role for EC2 ──────────────────────────────
        # EC2 needs to read from S3 Gold tables
        # Using IAM role is more secure than storing AWS keys on the instance
        ec2_role = iam.Role(
            self, "DashboardEC2Role",
            role_name="healthcare-dashboard-ec2-role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            description="EC2 role for Streamlit dashboard. Read access to S3 Gold",
        )

        # grant read access to Gold tables in S3
        bucket.grant_read(ec2_role)

        # attach SSM policy — allows you to connect via AWS Systems Manager
        # as an alternative to SSH if needed
        ec2_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonSSMManagedInstanceCore"
            )
        )

        # ── 10d. Key pair ──────────────────────────────────────
        key_pair = ec2.KeyPair(
            self, "DashboardKeyPair",
            key_pair_name="healthcare-dashboard-key",
            type=ec2.KeyPairType.RSA,
        )

        # ── 10e. User data — download and run setup script ────
        # Setup script lives in S3 (infrastructure/scripts/ec2_setup.sh)
        # uploaded via BucketDeployment in section 3b above.

        user_data = ec2.UserData.for_linux()
        user_data.add_commands(
            # update system and install base packages
            "dnf update -y",
            "dnf install -y python3 python3-pip git",

            # download setup script from S3 using AWS CLI
            # instance IAM role grants read access to the bucket
            f"aws s3 cp s3://{BUCKET_NAME}/scripts/ec2_setup.sh "
            f"/tmp/ec2_setup.sh",

            # run setup script as root
            # script handles su to ec2-user for pip installs
            "chmod +x /tmp/ec2_setup.sh",
            "bash /tmp/ec2_setup.sh",
        )

        # ── 10f. EC2 instance ──────────────────────────────────
        instance = ec2.Instance(
            self, "DashboardInstance",
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3,
                ec2.InstanceSize.SMALL      # 2 vCPU, 2GB RAM — sufficient for Streamlit
            ),
            machine_image=ec2.MachineImage.latest_amazon_linux2023(),
            vpc=vpc,
            security_group=dashboard_sg,
            role=ec2_role,
            key_pair=key_pair,
            user_data=user_data,
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",
                    volume=ec2.BlockDeviceVolume.ebs(
                        20,                             # 20GB storage
                        volume_type=ec2.EbsDeviceVolumeType.GP3,
                        delete_on_termination=True      # clean up on cdk destroy
                    )
                )
            ]
        )

        # ── 10g. Stack outputs ─────────────────────────────────
        # CfnOutput prints useful info after cdk deploy completes
        CfnOutput(
            self, "DashboardURL",
            value=f"http://{instance.instance_public_ip}:8501",
            description="Streamlit dashboard URL"
        )

        CfnOutput(
            self, "SSHCommand",
            value=f"ssh -i ~/.ssh/healthcare-dashboard-key.pem ec2-user@{instance.instance_public_ip}",
            description="SSH command to connect to the dashboard instance"
        )

        CfnOutput(
            self, "InstanceId",
            value=instance.instance_id,
            description="EC2 instance ID"
        )
