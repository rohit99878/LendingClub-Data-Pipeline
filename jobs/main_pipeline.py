import argparse
from datetime import datetime

from loanshield.bronze import run_bronze
from loanshield.gold import run_gold
from loanshield.quality import run_quality_report
from loanshield.settings import build_config
from loanshield.silver import run_silver
from loanshield.spark_session import create_spark_session


def parse_args():
    parser = argparse.ArgumentParser(description="Run the LoanShield AWS PySpark pipeline.")
    parser.add_argument("--raw-file-path", default=None)
    parser.add_argument("--bronze-path", default=None)
    parser.add_argument("--silver-path", default=None)
    parser.add_argument("--gold-path", default=None)
    parser.add_argument("--rejected-path", default=None)
    parser.add_argument("--reports-path", default=None)
    parser.add_argument("--sample-rows", type=int, default=None)
    return parser.parse_args()


def main():
    args = parse_args()
    config = build_config(args)
    spark = create_spark_session()
    pipeline_start = datetime.now()
    metrics = {}

    print("=" * 70)
    print("LOANSHIELD AWS PIPELINE STARTED")
    print(f"Start time: {pipeline_start}")
    print(f"Raw input: {config.raw_file_path}")
    print("=" * 70)

    try:
        metrics.update(run_bronze(spark, config))
        metrics.update(run_silver(spark, config))
        metrics.update(run_gold(spark, config))
        metrics.update(run_quality_report(spark, config, metrics))
        print("=" * 70)
        print("LOANSHIELD AWS PIPELINE COMPLETED")
        print(f"Duration: {datetime.now() - pipeline_start}")
        print(f"Metrics: {metrics}")
        print("=" * 70)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

