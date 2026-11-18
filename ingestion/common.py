"""Shared configuration and helpers for the SEC EDGAR ingestion modules."""

CATALOG = "financial_intelligence"
BRONZE = f"{CATALOG}.bronze"

USER_AGENT = "Financial Intelligence Data Platform majdy.yousef@portfolio.com"

TAXONOMY = "us-gaap"
UNIT = "USD"
YEARS = range(2024, 2026)

# Duration concepts: full-year periods (CY{year}).
ANNUAL_TAGS = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "NetIncomeLoss",
    "OperatingIncomeLoss",
    "NetCashProvidedByUsedInOperatingActivities",
]

# Instant (balance-sheet) concepts: year-end periods (CY{year}Q4I).
INSTANT_TAGS = [
    "Assets",
    "Liabilities",
    "StockholdersEquity",
    "AssetsCurrent",
]


def upsert(spark, df, table, on):
    """Idempotent upsert: create the target table if it does not exist,
    otherwise MERGE by the given ON condition (update matched rows,
    insert new ones).

    Args:
        spark: SparkSession used for the write.
        df: source DataFrame (must be unique on the ON keys).
        table: fully-qualified target table name.
        on: SQL join condition between target alias `t` and source `s`.
    """
    if not spark.catalog.tableExists(table):
        df.write.mode("overwrite").saveAsTable(table)
        return df
    df.createOrReplaceTempView("upsert_source")
    spark.sql(
        f"""
        MERGE INTO {table} AS t
        USING upsert_source AS s
        ON {on}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.catalog.dropTempView("upsert_source")
    return df
