from datetime import datetime


def run_gold(spark, config):
    print("=" * 70)
    print("GOLD LAYER STARTED")
    print("=" * 70)
    start = datetime.now()

    df_silver = spark.read.format("delta").load(config.silver_path)
    df_silver.createOrReplaceTempView("silver_loans")

    df_gold = spark.sql(
        """
        SELECT
            id,
            loan_amnt,
            term,
            int_rate,
            grade,
            emp_length,
            home_ownership,
            annual_inc,
            loan_status,
            dti,
            addr_state,
            fico_range_low,
            CASE
                WHEN grade IN ('A', 'B') THEN 'Low Risk'
                WHEN grade IN ('C', 'D') THEN 'Medium Risk'
                WHEN grade IN ('E', 'F', 'G') THEN 'High Risk'
                ELSE 'Unknown'
            END AS risk_category,
            CASE
                WHEN loan_amnt < 10000 THEN 'Small'
                WHEN loan_amnt BETWEEN 10000 AND 25000 THEN 'Medium'
                WHEN loan_amnt > 25000 THEN 'Large'
                ELSE 'Unknown'
            END AS loan_size_category,
            CASE
                WHEN dti < 15 THEN 'Healthy'
                WHEN dti BETWEEN 15 AND 35 THEN 'Moderate'
                WHEN dti > 35 THEN 'High Risk'
                ELSE 'Unknown'
            END AS dti_category,
            CASE
                WHEN fico_range_low BETWEEN 300 AND 649 THEN 'Fair'
                WHEN fico_range_low BETWEEN 650 AND 699 THEN 'Good'
                WHEN fico_range_low BETWEEN 700 AND 749 THEN 'Very Good'
                WHEN fico_range_low BETWEEN 750 AND 850 THEN 'Excellent'
                ELSE 'Unknown'
            END AS fico_category
        FROM silver_loans
        """
    )

    df_gold.write.format("delta").mode("overwrite").save(config.gold_path)

    gold_count = df_gold.count()
    print(f"Gold records: {gold_count:,}")
    print(f"Gold columns: {len(df_gold.columns)}")
    df_gold.show(5, False)
    print(f"Gold saved to: {config.gold_path}")
    print(f"Duration: {datetime.now() - start}")
    print("GOLD LAYER COMPLETE")
    return {"gold_records": gold_count}

