# LendingClub Data Pipeline 🚀

Production-grade PySpark data pipeline on AWS EMR processing 2.26M loan records through Medallion Architecture (Bronze→Silver→Gold) with Delta Lake, AWS Glue, and Athena.

## Pipeline Results (Live AWS Run)
| Metric | Value |
|--------|-------|
| Records Processed | 2,260,701 |
| Silver Records | 2,252,828 |
| Gold Records | 2,252,828 |
| Rejected Records | 5,873 |
| Data Quality Score | 99.65% |
| Pipeline Status | SUCCESS |

## Architecture

## Tech Stack
- **Compute:** AWS EMR EC2 (m5.xlarge, auto-terminating)
- **Storage:** AWS S3 (6 buckets, Delta Lake format)
- **Processing:** PySpark 3.4, Delta Lake
- **Catalog:** AWS Glue Data Catalog
- **Analytics:** AWS Athena (SQL on Delta Lake)
- **Automation:** AWS Lambda (S3 event trigger)
- **Monitoring:** AWS CloudWatch Dashboard
- **Language:** Python 3.11

## Medallion Architecture
| Layer | Records | Size | Description |
|-------|---------|------|-------------|
| Bronze | 2,260,701 | 389 MB | Raw ingestion from CSV |
| Silver | 2,252,828 | 35 MB | Validated + cleaned |
| Gold | 2,252,828 | 37 MB | Enriched with risk categories |
| Rejected | 5,873 | 158 KB | Bad records with rejection reasons |

## Athena SQL Insights
| Risk Category | Loan Count | Avg Loan Amount |
|---------------|------------|-----------------|
| Low Risk | 1,093,881 | $14,333.82 |
| Medium Risk | 970,776 | $15,260.44 |
| High Risk | 188,171 | $18,045.52 |

## Project Structure

```
LendingClub-Data-Pipeline/
├── notebooks/
│   ├── 01_data_exploration.ipynb
│   ├── 02_bronze_ingestion.ipynb
│   ├── 03_silver_validation.ipynb
│   ├── 04_gold_transformation.ipynb
│   ├── 05_quality_report_and_analytics.ipynb
│   └── 06_aws_orchestration_monitoring.ipynb
├── src/loanshield/
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   ├── quality.py
│   ├── settings.py
│   └── spark_session.py
├── jobs/
│   └── main_pipeline.py
├── lambda/
│   └── lambda_submit_emr_step.py
└── README.md
```


## How to Run
```bash
aws emr create-cluster \
  --name "lendingclub-pipeline" \
  --release-label emr-6.15.0 \
  --applications Name=Spark \
  --ec2-attributes SubnetId=<your-subnet>,InstanceProfile=EMR_EC2_DefaultRole \
  --service-role EMR_DefaultRole \
  --instance-groups \
    InstanceGroupType=MASTER,InstanceType=m5.xlarge,InstanceCount=1 \
    InstanceGroupType=CORE,InstanceType=m5.xlarge,InstanceCount=2 \
  --steps Type=Spark,Name="Pipeline",ActionOnFailure=TERMINATE_CLUSTER,\
Args=[--deploy-mode,cluster,--py-files,s3://<bucket>/scripts/loanshield.zip,\
s3://<bucket>/scripts/main_pipeline.py] \
  --auto-terminate
```

## Key Features
- ✅ Medallion Architecture (Bronze/Silver/Gold)
- ✅ Delta Lake ACID transactions
- ✅ 14 data validation business rules
- ✅ Auto-terminating EMR cluster (cost optimized)
- ✅ Automated Lambda trigger on S3 upload
- ✅ CloudWatch monitoring dashboard
- ✅ Full rejection audit trail