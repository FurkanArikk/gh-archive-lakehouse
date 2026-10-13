"""Databricks job task: new bronze rows -> typed silver tables and quarantine.

Thin entry point: parse settings, call gh_lakehouse.silver. No sys.exit: serverless runs
this file in an IPython kernel, where any SystemExit fails the task.
"""

import argparse

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

from gh_lakehouse.config import add_settings_args, settings_from_args
from gh_lakehouse.silver import QUARANTINE_TABLE, SPECS, run_silver


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    add_settings_args(parser)
    settings = settings_from_args(parser.parse_args(argv))

    spark = SparkSession.builder.getOrCreate()
    run_silver(spark, settings)
    for spec in SPECS:
        table = settings.silver_table(spec.table)
        print(f"silver: {table} has {spark.table(table).count()} row(s)")
    # Quarantine is created inside foreachBatch, in a cloned session; this session's
    # catalog.tableExists() can miss it, so read the table and handle "not found".
    quarantine = settings.silver_table(QUARANTINE_TABLE)
    try:
        print(f"silver: {quarantine} has {spark.table(quarantine).count()} row(s)")
    except AnalysisException:
        print(f"silver: {quarantine} does not exist (nothing quarantined yet)")


if __name__ == "__main__":
    main()
