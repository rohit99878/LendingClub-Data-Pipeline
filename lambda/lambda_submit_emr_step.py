import os
import boto3


emr = boto3.client("emr")


def lambda_handler(event, context):
    cluster_id = os.environ["EMR_CLUSTER_ID"]
    code_bucket = os.environ["CODE_BUCKET"]

    response = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": "LoanShield PySpark Pipeline",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--conf",
                        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
                        "--conf",
                        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
                        f"s3://{code_bucket}/loanshield/jobs/main_pipeline.py",
                    ],
                },
            }
        ],
    )

    return {
        "statusCode": 200,
        "body": {
            "message": "LoanShield EMR step submitted",
            "step_ids": response["StepIds"],
        },
    }

