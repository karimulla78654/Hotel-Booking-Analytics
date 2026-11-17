"""Gold layer: per-city performance summary."""

from pyspark import pipelines as dp

from transformations.common import SILVER_CLEAN, booking_aggregations


@dp.materialized_view(
    name="gold_city_performance_pipeline",
    comment=(
        "Bookings, paid revenue (INR, no conversion), completed room nights, "
        "average room rate and guest rating per city (with region)."
    ),
)
def gold_city_performance_pipeline():
    clean = spark.read.table(SILVER_CLEAN)
    return clean.groupBy("city", "region").agg(*booking_aggregations())
