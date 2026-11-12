"""Ingestion step: company submissions from SEC EDGAR.

Runs standalone (`spark` must be in scope), e.g. as its own job task.
"""

import json
from datetime import datetime, timezone

from bronze_schema import SUBMISSIONS_SCHEMA
from common import BRONZE, USER_AGENT, upsert
from sec_client import SECClient


def ingest_company_submissions(spark, ciks=None, client=None):
    """Fetch the submissions JSON per company and append one row per company to bronze.

    The payload column stores the raw, unmodified JSON string.

    Args:
        ciks: companies to fetch; defaults to all CIKs in bronze.companies.
    """
    if client is None:
        client = SECClient(user_agent=USER_AGENT)
    if ciks is None:
        ciks = [
            row.cik
            for row in spark.table(f"{BRONZE}.companies")
            .select("cik")
            .distinct()
            .collect()
        ]
    ingested_at = datetime.now(timezone.utc)
    rows = []
    for cik in ciks:
        payload = client.get_company_submissions(cik)
        rows.append(
            (
                int(cik),
                json.dumps(payload),
                ingested_at,
                client.SUBMISSIONS_URL.format(cik=client.format_cik(cik)),
            )
        )
    df = spark.createDataFrame(rows, schema=SUBMISSIONS_SCHEMA)
    return upsert(spark, df, f"{BRONZE}.company_submissions", "t.cik = s.cik")


if __name__ == "__main__":
    ingest_company_submissions(spark)
