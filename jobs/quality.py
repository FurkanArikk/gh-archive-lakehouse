"""Databricks job task: data quality checks for one layer; fails the task on any error.

    quality.py --layer silver   bronze + silver checks for the dates gold will process next
    quality.py --layer gold     gold checks for the dates the last gold run rebuilt
    quality.py --layer silver --dates 2026-10-06   re-check specific days on demand

Results are appended to <gold_schema>.dq_results whether they pass or not. No sys.exit:
serverless runs this file in an IPython kernel; failures raise DataQualityError instead.
"""

import argparse
from datetime import date

from pyspark.sql import SparkSession

from gh_lakehouse.config import add_settings_args, settings_from_args
from gh_lakehouse.quality import raise_on_failure, run_checks, summarize


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--layer", choices=["silver", "gold"], required=True)
    parser.add_argument("--run-id", default="manual", help="job run id, for dq_results")
    parser.add_argument(
        "--dates",
        # Empty (the job parameter's default) means "the pending dates".
        type=lambda s: [date.fromisoformat(d) for d in s.split(",")] if s.strip() else None,
        help="re-check these days (YYYY-MM-DD,...) instead of the pending ones",
    )
    add_settings_args(parser)
    args = parser.parse_args(argv)
    settings = settings_from_args(args)

    spark = SparkSession.builder.getOrCreate()
    results = run_checks(spark, settings, args.layer, args.run_id, args.dates)
    print(summarize(results) if results else f"quality[{args.layer}]: no dates to check")
    raise_on_failure(results)


if __name__ == "__main__":
    main()
