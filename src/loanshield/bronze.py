from datetime import datetime


def run_bronze(spark, config):
    print("=" * 70)
    print("BRONZE LAYER STARTED")
    print("=" * 70)
    start = datetime.now()

    df_raw = spark.read.csv(config.raw_file_path, header=True, inferSchema=True)
    if config.sample_rows:
        print(f"Using sample rows for cost-controlled testing: {config.sample_rows:,}")
        df_raw = df_raw.limit(config.sample_rows)

    row_count = df_raw.count()
    print(f"Raw records: {row_count:,}")
    print(f"Raw columns: {len(df_raw.columns)}")

    df_raw.write.format("delta").mode("overwrite").save(config.bronze_path)

    print(f"Bronze saved to: {config.bronze_path}")
    print(f"Duration: {datetime.now() - start}")
    print("BRONZE LAYER COMPLETE")
    return {"bronze_records": row_count}

