"""Ingestion step: company facts (XBRL) from SEC EDGAR.

Runs standalone (`spark` must be in scope), e.g. as its own job task.
"""

import json
from datetime import datetime, timezone

import requests

from bronze_schema import FACTS_SCHEMA
from common import BRONZE, USER_AGENT, upsert
from sec_client import SECClient, SECClientError


def ingest_company_facts(spark, ciks=None, client=None):
    """Fetch the companyfacts JSON per company and append one row per company to bronze.

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
        # Not every company in company_tickers.json has XBRL facts
        # (e.g. foreign private issuers); the API returns 404 for those.
        try:
            payload = client.get_company_facts(cik)
        except (SECClientError, requests.HTTPError) as exc:
            print(f"WARN: skipping companyfacts for CIK {cik}: {exc}")
            continue
        rows.append(
            (
                int(cik),
                json.dumps(payload),
                ingested_at,
                client.COMPANYFACTS_URL.format(cik=client.format_cik(cik)),
            )
        )
    df = spark.createDataFrame(rows, schema=FACTS_SCHEMA)
    return upsert(spark, df, f"{BRONZE}.company_facts", "t.cik = s.cik")


if __name__ == "__main__":
    ingest_company_facts(spark)
