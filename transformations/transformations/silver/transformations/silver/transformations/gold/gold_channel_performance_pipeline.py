"""Gold layer: per-booking-channel performance summary."""

from pyspark import pipelines as dp

from transformations.common import SILVER_CLEAN, booking_aggregations


@dp.materialized_view(
    name="gold_channel_performance_pipeline",
    comment=(
        "Bookings, paid revenue (INR, no conversion), completed room nights, "
        "average room rate and guest rating per booking channel."
    ),
)
def gold_channel_performance_pipeline():
    clean = spark.read.table(SILVER_CLEAN)
    return clean.groupBy("booking_channel").agg(*booking_aggregations())
