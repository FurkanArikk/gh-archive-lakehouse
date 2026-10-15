"""Databricks job task: rebuild gold tables for the days silver has new data for.

Thin entry point: parse settings, call gh_lakehouse.gold. No sys.exit: serverless runs
this file in an IPython kernel, where any SystemExit fails the task.
"""

import argparse

from pyspark.sql import SparkSession

from gh_lakehouse.config import add_settings_args, settings_from_args
from gh_lakehouse.gold import run_gold


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    add_settings_args(parser)
    settings = settings_from_args(parser.parse_args(argv))

    spark = SparkSession.builder.getOrCreate()
    plan = run_gold(spark, settings)
    if plan is None:
        print("gold: no new silver data, nothing to rebuild")
        return
    print(f"gold: rebuilt dates {[d.isoformat() for d in plan.dates]}")
    for name in plan.tables:
        table = settings.gold_table(name)
        print(f"gold: {table} has {spark.table(table).count()} row(s)")


if __name__ == "__main__":
    main()
