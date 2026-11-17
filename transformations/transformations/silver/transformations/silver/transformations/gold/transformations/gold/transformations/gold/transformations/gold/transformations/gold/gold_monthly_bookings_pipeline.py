"""Gold layer: monthly booking summary."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformations.common import SILVER_CLEAN, booking_aggregations


@dp.materialized_view(
    name="gold_monthly_bookings_pipeline",
    comment=(
        "Monthly bookings, paid revenue (INR, no conversion), completed room "
        "nights and guest rating per booking month."
    ),
)
def gold_monthly_bookings_pipeline():
    clean = spark.read.table(SILVER_CLEAN)
    return clean.groupBy(
        F.date_trunc("month", F.col("booking_date")).alias("booking_month")
    ).agg(*booking_aggregations())
