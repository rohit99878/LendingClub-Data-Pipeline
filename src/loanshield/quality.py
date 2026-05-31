from datetime import datetime


def run_quality_report(spark, config, metrics):
    print("=" * 70)
    print("QUALITY REPORT STARTED")
    print("=" * 70)

    bronze_count = int(metrics.get("bronze_records", 0))
    silver_count = int(metrics.get("silver_records", 0))
    gold_count = int(metrics.get("gold_records", 0))
    rejected_count = int(metrics.get("rejected_records", 0))
    quality_score = round((gold_count / bronze_count) * 100, 2) if bronze_count else 0
    report_date = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

    report_data = [
        ("run_date", str(datetime.now())),
        ("raw_file_path", config.raw_file_path),
        ("sample_rows", str(config.sample_rows or "FULL_DATASET")),
        ("bronze_records", str(bronze_count)),
        ("silver_records", str(silver_count)),
        ("gold_records", str(gold_count)),
        ("rejected_records", str(rejected_count)),
        ("total_removed", str(bronze_count - gold_count)),
        ("data_quality_score", f"{quality_score}%"),
        ("pipeline_status", "SUCCESS"),
    ]

    df_report = spark.createDataFrame(report_data, ["metric", "value"])
    report_path = f"{config.reports_path.rstrip('/')}/quality_report_{report_date}"
    df_report.write.format("csv").mode("overwrite").option("header", "true").save(report_path)

    df_report.show(50, False)
    print(f"Quality report saved to: {report_path}")
    print("QUALITY REPORT COMPLETE")
    return {"data_quality_score": quality_score, "report_path": report_path}

