"""Databricks job task: new landing files -> bronze events table.

Thin entry point: parse settings, call gh_lakehouse.bronze. The job passes every setting
as a parameter (see resources/pipeline_job.yml).

Serverless runs this file inside an IPython kernel, where any SystemExit (even exit code 0)
fails the task. So: no sys.exit; errors fail the task by raising.
"""

import argparse
import sys

from pyspark.sql import SparkSession

from gh_lakehouse.bronze import BRONZE_TABLE, run_bronze
from gh_lakehouse.config import add_settings_args, settings_from_args


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    add_settings_args(parser)
    settings = settings_from_args(parser.parse_args(argv))

    spark = SparkSession.builder.getOrCreate()
    # Local tests pin these versions; this line makes a mismatch visible in the run log.
    print(f"runtime: spark {spark.version}, python {sys.version.split()[0]}")
    rows = run_bronze(spark, settings)
    print(f"bronze: appended {rows} row(s) to {settings.bronze_table(BRONZE_TABLE)}")


if __name__ == "__main__":
    main()
