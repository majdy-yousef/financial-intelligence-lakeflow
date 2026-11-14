"""Silver layer: `company_tickers` materialized view.

Lakeflow Spark Declarative Pipeline dataset. Explodes the parallel
`tickers[]` / `exchanges[]` arrays of the raw submissions JSON payload
of `bronze.company_submissions` into one row per ticker/exchange pair,
published as `financial_intelligence.silver.company_tickers` by the
silver pipeline.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

# Partial schema for from_json: only the payload fields used by the silver table.
_TICKERS_SCHEMA = StructType(
    [
        StructField("tickers", ArrayType(StringType())),
        StructField("exchanges", ArrayType(StringType())),
    ]
)


@dp.materialized_view(
    name="company_tickers",
    comment="Ticker dimension: one row per ticker/exchange pair, exploded from the submissions payload.",
    schema="""
        cik BIGINT,
        ticker STRING,
        exchange STRING
    """,
)
def company_tickers():
    parsed = (
        spark.read.table("financial_intelligence.bronze.company_submissions")
        .select(F.col("cik"), F.from_json(F.col("payload"), _TICKERS_SCHEMA).alias("e"))
    )
    return (
        parsed.select(
            F.col("cik"),
            F.explode(
                F.arrays_zip(
                    F.col("e.tickers").alias("ticker"),
                    F.col("e.exchanges").alias("exchange"),
                )
            ).alias("t"),
        )
        .select(F.col("cik"), F.col("t.ticker"), F.col("t.exchange"))
        .dropDuplicates(["cik", "ticker", "exchange"])
    )
