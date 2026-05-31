from dataclasses import dataclass
import os


@dataclass(frozen=True)
class PipelineConfig:
    raw_file_path: str
    bronze_path: str
    silver_path: str
    gold_path: str
    rejected_path: str
    reports_path: str
    sample_rows: "int | None" = None


REQUIRED_COLUMNS = [
    "id",
    "loan_amnt",
    "term",
    "int_rate",
    "grade",
    "emp_length",
    "home_ownership",
    "annual_inc",
    "loan_status",
    "dti",
    "addr_state",
    "fico_range_low",
]

VALID_LOAN_STATUS = [
    "Fully Paid",
    "Current",
    "Charged Off",
    "Late (31-120 days)",
    "In Grace Period",
    "Late (16-30 days)",
    "Default",
]

VALID_GRADES = ["A", "B", "C", "D", "E", "F", "G"]


def env_or_default(name: str, default: str) -> str:
    return os.environ.get(name, default)


def build_config(args) -> PipelineConfig:
    return PipelineConfig(
        raw_file_path=args.raw_file_path
        or env_or_default("LOANSHIELD_RAW_FILE", "s3://loanshield-raw/accepted_2007_to_2018Q4.csv"),
        bronze_path=args.bronze_path or env_or_default("LOANSHIELD_BRONZE_PATH", "s3://loanshield-bronze/"),
        silver_path=args.silver_path or env_or_default("LOANSHIELD_SILVER_PATH", "s3://loanshield-silver/"),
        gold_path=args.gold_path or env_or_default("LOANSHIELD_GOLD_PATH", "s3://loanshield-gold/"),
        rejected_path=args.rejected_path
        or env_or_default("LOANSHIELD_REJECTED_PATH", "s3://loanshield-rejected/"),
        reports_path=args.reports_path or env_or_default("LOANSHIELD_REPORTS_PATH", "s3://loanshield-reports/"),
        sample_rows=args.sample_rows,
    )

