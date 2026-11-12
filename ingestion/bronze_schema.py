"""Spark schemas for the bronze-layer SEC EDGAR tables."""

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# One row per company in company_tickers.json (top-level keys "0", "1", ...).
COMPANIES_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("cik", LongType(), False),
        StructField(
            "payload",
            StructType(
                [
                    StructField("cik_str", LongType(), True),
                    StructField("ticker", StringType(), True),
                    StructField("title", StringType(), True),
                ]
            ),
            False,
        ),
        StructField("ingested_at", TimestampType(), False),
        StructField("source_url", StringType(), False),
    ]
)

# One row per company in submissions/CIK##########.json (payload = raw JSON string).
SUBMISSIONS_SCHEMA = StructType(
    [
        StructField("cik", LongType(), False),
        StructField("payload", StringType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("source_url", StringType(), False),
    ]
)

# One row per company in companyfacts/CIK##########.json (payload = raw JSON string).
FACTS_SCHEMA = StructType(
    [
        StructField("cik", LongType(), False),
        StructField("payload", StringType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("source_url", StringType(), False),
    ]
)

# One row per taxonomy/tag/unit/period combination from the frames API
# (payload = raw JSON string).
FRAMES_SCHEMA = StructType(
    [
        StructField("taxonomy", StringType(), False),
        StructField("tag", StringType(), False),
        StructField("unit", StringType(), False),
        StructField("period", StringType(), False),
        StructField("payload", StringType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("source_url", StringType(), False),
    ]
)