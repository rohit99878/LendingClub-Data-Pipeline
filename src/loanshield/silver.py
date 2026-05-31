from datetime import datetime
from pyspark.sql.functions import col, lit, trim, when

from loanshield.settings import REQUIRED_COLUMNS, VALID_GRADES, VALID_LOAN_STATUS


def assert_required_columns(df):
    missing = [name for name in REQUIRED_COLUMNS if name not in df.columns]
    if missing:
        raise ValueError(f"Missing required source columns: {missing}")


def run_silver(spark, config):
    print("=" * 70)
    print("SILVER LAYER STARTED")
    print("=" * 70)
    start = datetime.now()

    df_bronze = spark.read.format("delta").load(config.bronze_path)
    assert_required_columns(df_bronze)

    df_selected = df_bronze.select(REQUIRED_COLUMNS)
    df_clean = df_selected.filter(~col("id").cast("string").rlike("[a-zA-Z]"))
    df_clean = df_clean.filter(
        ~col("dti").cast("string").rlike("[a-zA-Z]")
        & ~col("fico_range_low").cast("string").rlike("[a-zA-Z]")
    )

    df_typed = (
        df_clean.withColumn("loan_amnt", col("loan_amnt").cast("double"))
        .withColumn("annual_inc", col("annual_inc").cast("double"))
        .withColumn("dti", col("dti").cast("double"))
        .withColumn("fico_range_low", col("fico_range_low").cast("double"))
        .withColumn("grade", trim(col("grade")))
        .withColumn("loan_status", trim(col("loan_status")))
        .withColumn("term", trim(col("term")))
    )

    df_validated = df_typed.withColumn(
        "rejection_reason",
        when(col("annual_inc").isNull(), lit("NULL_ANNUAL_INC"))
        .when(col("annual_inc") <= 0, lit("ZERO_ANNUAL_INC"))
        .when(col("annual_inc") > 1000000, lit("EXTREME_ANNUAL_INC"))
        .when(col("loan_amnt").isNull(), lit("NULL_LOAN_AMNT"))
        .when(col("loan_amnt") < 500, lit("LOW_LOAN_AMNT"))
        .when(col("loan_amnt") > 40000, lit("HIGH_LOAN_AMNT"))
        .when(~col("loan_status").isin(VALID_LOAN_STATUS), lit("INVALID_LOAN_STATUS"))
        .when(~col("grade").isin(VALID_GRADES), lit("INVALID_GRADE"))
        .when(col("dti").isNull(), lit("NULL_DTI"))
        .when(col("dti") < 0, lit("NEGATIVE_DTI"))
        .when(col("dti") > 100, lit("EXTREME_DTI"))
        .when(col("fico_range_low").isNull(), lit("NULL_FICO"))
        .when(col("fico_range_low") < 300, lit("LOW_FICO"))
        .when(col("fico_range_low") > 850, lit("HIGH_FICO"))
        .otherwise(lit("CLEAN")),
    )

    df_silver = (
        df_validated.filter(col("rejection_reason") == "CLEAN")
        .drop("rejection_reason")
        .fillna({"emp_length": "Unknown"})
    )
    df_rejected = df_validated.filter(col("rejection_reason") != "CLEAN")

    df_silver.write.format("delta").mode("overwrite").save(config.silver_path)
    df_rejected.write.format("delta").mode("overwrite").save(config.rejected_path)

    silver_count = df_silver.count()
    rejected_count = df_rejected.count()
    print(f"Silver records: {silver_count:,}")
    print(f"Rejected records: {rejected_count:,}")
    df_rejected.groupBy("rejection_reason").count().orderBy("count", ascending=False).show(50, False)
    print(f"Duration: {datetime.now() - start}")
    print("SILVER LAYER COMPLETE")
    return {"silver_records": silver_count, "rejected_records": rejected_count}

