"""Gold layer: per-room-type performance summary."""

from pyspark import pipelines as dp

from transformations.common import SILVER_CLEAN, booking_aggregations


@dp.materialized_view(
    name="gold_room_performance_pipeline",
    comment=(
        "Bookings, paid revenue (INR, no conversion), completed room nights, "
        "average room rate and guest rating per room type."
    ),
)
def gold_room_performance_pipeline():
    clean = spark.read.table(SILVER_CLEAN)
    return clean.groupBy("room_type").agg(*booking_aggregations())
