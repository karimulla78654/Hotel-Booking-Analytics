"""Shared bronze-to-silver rules and gold metric helpers.

Keeps every dataset in this pipeline consistent with the project's validated
Hotel Booking Analytics recipe:

* Dedup: keep one row per booking_id, ordered by
  (booking_date, check_in_date, check_out_date).
* Reject ("Invalid checkout date"): check_in_date or check_out_date is NULL,
  or check_out_date <= check_in_date.
* Reject ("Negative room rate"): room_rate < 0. NULL room_rate is kept.
* Silver normalization: TRIM/INITCAP text columns; nights falls back to
  DATEDIFF(check_out_date, check_in_date); total_amount falls back to
  room_rate * nights - discount_amount.
* Gold paid_revenue: SUM(total_amount) over rows with
  booking_status = 'Completed' AND payment_status = 'Paid'.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Existing bronze source table (read-only input, owned outside this pipeline).
BRONZE_SOURCE = "workspace.bookings.bronze_bookings"

# Pipeline-managed silver clean table (dataset name, resolved by the pipeline).
SILVER_CLEAN = "silver_bookings_clean_pipeline"
SILVER_REJECTED = "silver_bookings_rejected_pipeline"

# Text columns normalized with TRIM/INITCAP in silver.
TEXT_COLUMNS_TO_NORMALIZE = [
    "hotel_name",
    "city",
    "region",
    "room_type",
    "booking_channel",
    "meal_plan",
    "payment_method",
    "payment_status",
    "booking_status",
]


def rank_bookings_by_id(df: DataFrame) -> DataFrame:
    """Add _rn: row number within each booking_id, ordered so the earliest
    record (by booking_date, check_in_date, check_out_date) ranks first."""
    w = Window.partitionBy("booking_id").orderBy(
        "booking_date", "check_in_date", "check_out_date"
    )
    return df.withColumn("_rn", F.row_number().over(w))


def dedupe_bookings(df: DataFrame) -> DataFrame:
    """Keep one row per booking_id (the earliest record).

    Duplicate booking_ids are handled here, consistently for every dataset
    in the pipeline: only the _rn = 1 row survives; later copies are counted
    as duplicate_rows in gold_data_quality_pipeline.
    """
    return rank_bookings_by_id(df).filter(F.col("_rn") == 1).drop("_rn")


def is_invalid_checkout() -> Column:
    """Rows with a missing check_in/check_out date or check_out <= check_in.

    Never returns NULL because the isNull() branches cover NULL dates.
    """
    return (
        F.col("check_in_date").isNull()
        | F.col("check_out_date").isNull()
        | (F.col("check_out_date") <= F.col("check_in_date"))
    )


def is_negative_rate() -> Column:
    """Rows with room_rate < 0. NULL room_rate is NOT rejected."""
    return F.col("room_rate") < 0


def is_acceptable_rate() -> Column:
    """NULL or non-negative room_rate (NULL is kept, per project rules)."""
    return F.col("room_rate").isNull() | (F.col("room_rate") >= 0)


def normalize_bookings(df: DataFrame) -> DataFrame:
    """Apply silver normalization: TRIM/INITCAP text columns, derive nights
    and total_amount where missing."""
    result = df
    for column_name in TEXT_COLUMNS_TO_NORMALIZE:
        result = result.withColumn(column_name, F.trim(F.initcap(F.col(column_name))))
    result = result.withColumn(
        "nights",
        F.coalesce(
            F.col("nights"),
            F.datediff(F.col("check_out_date"), F.col("check_in_date")),
        ),
    )
    return result.withColumn(
        "total_amount",
        F.coalesce(
            F.col("total_amount"),
            F.col("room_rate") * F.col("nights")
            - F.coalesce(F.col("discount_amount"), F.lit(0.0)),
        ),
    )


def booking_aggregations() -> list:
    """Standard booking metrics shared by every gold performance table."""
    completed = F.col("booking_status") == "Completed"
    cancelled = F.col("booking_status") == "Cancelled"
    paid = F.col("payment_status") == "Paid"
    return [
        F.count(F.lit(1)).alias("total_bookings"),
        F.count(F.when(completed, F.lit(1))).alias("completed_bookings"),
        F.count(F.when(cancelled, F.lit(1))).alias("cancelled_bookings"),
        F.sum(
            F.when(completed & paid, F.col("total_amount")).otherwise(F.lit(0.0))
        ).alias("paid_revenue"),
        F.sum(F.when(completed, F.col("nights")).otherwise(F.lit(0))).alias(
            "completed_room_nights"
        ),
        F.avg(F.col("room_rate")).alias("average_room_rate"),
        F.avg(F.col("guest_rating")).alias("average_guest_rating"),
    ]
