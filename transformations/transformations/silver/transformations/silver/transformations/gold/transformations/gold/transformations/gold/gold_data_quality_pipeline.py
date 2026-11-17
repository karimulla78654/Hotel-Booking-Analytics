"""Gold layer: data quality counts and row reconciliation.

Single-row summary that reconciles the bronze source against the pipeline
outputs: bronze_rows must equal duplicate_rows + rejected_rows +
clean_rows. Rejection counts are taken from the actual published silver
tables so the reconciliation reflects real output, not re-derived logic.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformations.common import (
    BRONZE_SOURCE,
    SILVER_CLEAN,
    SILVER_REJECTED,
    rank_bookings_by_id,
)


@dp.materialized_view(
    name="gold_data_quality_pipeline",
    comment=(
        "Data quality counts and reconciliation for the bookings pipeline: "
        "bronze rows vs duplicates + rejected + clean rows, with rejection "
        "reason breakdown and a PASS/FAIL status."
    ),
)
def gold_data_quality_pipeline():
    ranked = rank_bookings_by_id(spark.read.table(BRONZE_SOURCE))
    bronze_stats = ranked.agg(
        F.count(F.lit(1)).alias("bronze_rows"),
        F.count(F.when(F.col("_rn") == 1, F.lit(1))).alias("unique_bookings"),
        F.count(F.when(F.col("_rn") > 1, F.lit(1))).alias("duplicate_rows"),
    )

    clean_stats = spark.read.table(SILVER_CLEAN).agg(
        F.count(F.lit(1)).alias("clean_rows")
    )

    rejected_stats = spark.read.table(SILVER_REJECTED).agg(
        F.count(F.lit(1)).alias("rejected_rows"),
        F.count(
            F.when(F.col("rejection_reason") == "Invalid checkout date", F.lit(1))
        ).alias("invalid_checkout_rows"),
        F.count(
            F.when(F.col("rejection_reason") == "Negative room rate", F.lit(1))
        ).alias("negative_rate_rows"),
    )

    stats = (
        bronze_stats.join(clean_stats, how="cross")
        .join(rejected_stats, how="cross")
    )

    rows_accounted = (
        F.col("duplicate_rows") + F.col("rejected_rows") + F.col("clean_rows")
    )
    is_reconciled = rows_accounted == F.col("bronze_rows")

    return stats.select(
        "bronze_rows",
        "unique_bookings",
        "duplicate_rows",
        "rejected_rows",
        "invalid_checkout_rows",
        "negative_rate_rows",
        "clean_rows",
        rows_accounted.alias("rows_accounted"),
        is_reconciled.alias("is_reconciled"),
        F.when(is_reconciled, F.lit("PASS"))
        .otherwise(F.lit("FAIL"))
        .alias("reconciliation_status"),
    )
