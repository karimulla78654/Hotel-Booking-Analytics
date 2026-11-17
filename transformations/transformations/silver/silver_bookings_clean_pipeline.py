"""Silver layer: cleaned hotel bookings.

One row per booking_id (duplicates resolved by keeping the earliest record
by booking_date, check_in_date, check_out_date). Rows with invalid checkout
dates (missing check_in/check_out or check_out <= check_in) or negative
room_rate are excluded (they land in silver_bookings_rejected_pipeline);
rows with NULL room_rate are kept. Text fields are trimmed/initial-capsed,
nights falls back to DATEDIFF(check_out_date, check_in_date) and
total_amount falls back to room_rate * nights - discount_amount.
"""

from pyspark import pipelines as dp

from transformations.common import (
    BRONZE_SOURCE,
    dedupe_bookings,
    is_acceptable_rate,
    is_invalid_checkout,
    normalize_bookings,
)


@dp.materialized_view(
    name="silver_bookings_clean_pipeline",
    comment=(
        "Cleaned hotel bookings: deduplicated by booking_id (earliest record "
        "kept), invalid checkout dates and negative room rates excluded, text "
        "normalized, nights and total_amount derived where missing."
    ),
)
def silver_bookings_clean_pipeline():
    bronze = spark.read.table(BRONZE_SOURCE)
    clean = dedupe_bookings(bronze).filter(
        ~is_invalid_checkout() & is_acceptable_rate()
    )
    return normalize_bookings(clean)
