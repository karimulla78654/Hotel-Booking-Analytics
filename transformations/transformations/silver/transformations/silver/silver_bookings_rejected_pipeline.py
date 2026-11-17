"""Silver layer: bookings rejected during cleaning.

Raw (unmodified) bronze rows that fail cleaning, each tagged with a
rejection reason: 'Invalid checkout date' (missing check_in/check_out date
or check_out_date <= check_in_date) or 'Negative room rate' (room_rate < 0).
NULL room_rate rows are NOT rejected. Duplicate booking_ids are resolved
before the rejection checks, so only one copy of a rejected booking
appears here.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformations.common import (
    BRONZE_SOURCE,
    dedupe_bookings,
    is_invalid_checkout,
    is_negative_rate,
)


@dp.materialized_view(
    name="silver_bookings_rejected_pipeline",
    comment=(
        "Bookings rejected during silver cleaning, with rejection reason: "
        "'Invalid checkout date' or 'Negative room rate'."
    ),
)
def silver_bookings_rejected_pipeline():
    bronze = spark.read.table(BRONZE_SOURCE)
    rejected = dedupe_bookings(bronze).filter(
        is_invalid_checkout() | is_negative_rate()
    )
    return rejected.withColumn(
        "rejection_reason",
        F.when(is_invalid_checkout(), F.lit("Invalid checkout date"))
        .when(is_negative_rate(), F.lit("Negative room rate")),
    )
