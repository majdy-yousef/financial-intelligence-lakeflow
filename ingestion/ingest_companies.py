"""Ingestion step: companies from SEC EDGAR company_tickers.json.

Runs standalone (`spark` must be in scope), e.g. as its own job task.
"""

from datetime import datetime, timezone

from bronze_schema import COMPANIES_SCHEMA
from common import BRONZE, USER_AGENT, upsert
from sec_client import SECClient


def ingest_companies(spark, client=None):
    """Fetch company_tickers.json and write one row per company to bronze.companies."""
    if client is None:
        client = SECClient(user_agent=USER_AGENT)
    data = client.get_companies()
    ingested_at = datetime.now(timezone.utc)
    rows = [
        (int(key), entry["cik_str"], entry, ingested_at, client.TICKERS_URL)
        for key, entry in data.items()
    ]
    df = spark.createDataFrame(rows, schema=COMPANIES_SCHEMA)
    # company_tickers.json lists one entry per ticker, so the same CIK
    # can appear multiple times; keep one row per CIK so the MERGE key
    # is unique in the source.
    df = df.dropDuplicates(["cik"])
    return upsert(spark, df, f"{BRONZE}.companies", "t.cik = s.cik")


if __name__ == "__main__":
    ingest_companies(spark)
