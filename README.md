# LoanShield AWS Data Engineering Pipeline

LoanShield is a portfolio data engineering project built on AWS. It processes raw loan data from S3 into Bronze, Silver, and Gold analytical layers using EMR, PySpark, SparkSQL, and Delta Lake, then prepares the Gold layer for Redshift analytics and CloudWatch monitoring.

## Portfolio Focus

This repository is notebook-first because the project is intended for GitHub and resume review. The main notebook should be run in EMR Studio and saved with outputs visible.

```text
S3 Raw CSV
  -> EMR Studio PySpark Notebook
  -> Bronze Delta on S3
  -> Silver Delta + Rejected Records on S3
  -> Gold Delta on S3
  -> Redshift Analytics Layer
  -> CloudWatch Logs / Lambda Trigger
```

## Repository Structure

```text
LoanShield_AWS_Portfolio/
  README.md
  notebooks/
    01_data_exploration.ipynb
    02_bronze_ingestion.ipynb
    03_silver_validation.ipynb
    04_gold_transformation.ipynb
    05_quality_report_and_analytics.ipynb
    06_aws_orchestration_monitoring.ipynb
  src/
    loanshield/
      bronze.py
      silver.py
      gold.py
      quality.py
      settings.py
      spark_session.py
  jobs/
    main_pipeline.py
  lambda/
    lambda_submit_emr_step.py
  docs/
    resume_bullets.md
```

## Important Note About Dataset

The full input dataset should stay in S3, not GitHub. GitHub should include the notebook, code, screenshots/output, and the S3 path pattern. Large datasets should not be committed to the repository.

## How To Use For Portfolio

1. Open the notebooks in EMR Studio.
2. Attach it to an EMR cluster with Spark and Delta Lake support.
3. Run notebooks `01` through `05` in order.
4. First run with `SAMPLE_ROWS = 10000` to validate cheaply.
5. For final portfolio output, set `SAMPLE_ROWS = None` in notebooks `01` and `02`, then rerun the full dataset.
6. Save the notebooks with outputs.
7. Upload the saved notebooks to GitHub.

## Notebook Guide

| Notebook | Purpose |
| --- | --- |
| `01_data_exploration.ipynb` | Reads raw S3 data, shows schema, samples, null checks, and category distributions. |
| `02_bronze_ingestion.ipynb` | Writes raw data to the Bronze Delta layer on S3. |
| `03_silver_validation.ipynb` | Applies data quality rules and writes Silver plus rejected records. |
| `04_gold_transformation.ipynb` | Uses SparkSQL to create analytics-ready Gold data. |
| `05_quality_report_and_analytics.ipynb` | Shows final counts, quality score, and business analytics outputs. |
| `06_aws_orchestration_monitoring.ipynb` | Documents Lambda, CloudWatch, Redshift, and production orchestration design. |

## Notebook And Production Code Strategy

This project includes both notebooks and Python files intentionally.

- `notebooks/` shows the development workflow and portfolio-visible outputs.
- `src/loanshield/` contains the production-style reusable PySpark modules.
- `jobs/main_pipeline.py` is the Spark job entry point for EMR `spark-submit`.

This mirrors a common data engineering workflow:

```text
Explore and validate in notebooks
  -> move stable logic into Python modules
  -> deploy as a Spark job
  -> orchestrate and monitor on AWS
```

## AWS Services Used

- Amazon S3 for data lake storage
- Amazon EMR / EMR Studio for PySpark execution
- PySpark and SparkSQL for transformations
- Delta Lake for Bronze/Silver/Gold storage
- Amazon Redshift for analytics consumption
- AWS Lambda / EventBridge for orchestration design
- CloudWatch for logs and monitoring

## Resume Bullet

Built an AWS data engineering pipeline using S3, EMR Studio, PySpark, SparkSQL, Delta Lake, Redshift, Lambda, and CloudWatch to process raw loan data into Bronze, Silver, and Gold analytical layers with validation, rejected-record handling, and quality reporting.

## Production Spark Submit

Package the source code:

```bash
cd LoanShield_AWS_Portfolio
cd src
zip -r ../loanshield_src.zip loanshield
cd ..
aws s3 cp loanshield_src.zip s3://YOUR-CODE-BUCKET/loanshield/jobs/loanshield_src.zip
aws s3 cp jobs/main_pipeline.py s3://YOUR-CODE-BUCKET/loanshield/jobs/main_pipeline.py
```

Run a cost-controlled sample:

```bash
spark-submit \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --py-files s3://YOUR-CODE-BUCKET/loanshield/jobs/loanshield_src.zip \
  s3://YOUR-CODE-BUCKET/loanshield/jobs/main_pipeline.py \
  --sample-rows 10000
```
