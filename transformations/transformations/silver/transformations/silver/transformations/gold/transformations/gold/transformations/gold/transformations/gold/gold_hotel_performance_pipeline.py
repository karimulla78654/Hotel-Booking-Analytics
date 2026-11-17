"""Gold layer: per-hotel performance summary."""

from pyspark import pipelines as dp

from transformations.common import SILVER_CLEAN, booking_aggregations


@dp.materialized_view(
    name="gold_hotel_performance_pipeline",
    comment=(
        "Bookings, paid revenue (INR, no conversion), completed room nights, "
        "average room rate and guest rating per hotel (with city and region)."
    ),
)
def gold_hotel_performance_pipeline():
    clean = spark.read.table(SILVER_CLEAN)
    return clean.groupBy("hotel_name", "city", "region").agg(
        *booking_aggregations()
    )
