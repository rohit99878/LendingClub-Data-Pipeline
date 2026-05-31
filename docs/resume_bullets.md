# Resume Bullets

## Main Bullet

- Built an AWS data engineering pipeline using S3, EMR Studio, PySpark, SparkSQL, Delta Lake, Redshift, Lambda, and CloudWatch to process raw loan data into Bronze, Silver, and Gold analytical layers with validation, rejected-record handling, and quality reporting.

## Short Bullet

- Developed a PySpark-based AWS data lake pipeline on EMR Studio to transform raw loan data from S3 into validated and analytics-ready Delta Lake tables.

## Interview Talking Points

- Used S3 as the data lake storage layer for raw, bronze, silver, gold, rejected, and report outputs.
- Used EMR Studio for notebook-based PySpark development and portfolio-visible output.
- Implemented validation rules for annual income, loan amount, DTI, FICO score, grade, and loan status.
- Preserved bad records in a rejected-record layer instead of silently dropping them.
- Used SparkSQL to generate analytical features such as risk category, loan size category, DTI category, and FICO category.
- Designed Lambda/EventBridge orchestration and CloudWatch monitoring for production-style AWS execution.

