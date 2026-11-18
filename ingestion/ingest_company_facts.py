"""Ingestion step: company facts (XBRL) from SEC EDGAR.

Runs standalone (`spark` must be in scope), e.g. as its own job task.

Data is pulled and written in batches: each batch of CIKs is fetched,
then immediately upserted into bronze.company_facts, so progress is
visible mid-run and a cancelled run keeps the already-written batches.
"""

import json
from datetime import datetime, timezone

import requests

from bronze_schema import FACTS_SCHEMA
from common import BRONZE, USER_AGENT, upsert
from sec_client import SECClient, SECClientError


def ingest_company_facts(spark, ciks=None, client=None, batch_size=50):
    """Fetch the companyfacts JSON per company and upsert it to bronze in batches.

    The payload column stores the raw, unmodified JSON string.

    Args:
        ciks: companies to fetch; defaults to all CIKs in bronze.companies.
        client: pre-built SECClient; defaults to a throttled client.
        batch_size: number of companies fetched (and upserted) per batch.
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

    total_batches = (len(ciks) + batch_size - 1) // batch_size
    upserted = 0
    for batch_no, start in enumerate(range(0, len(ciks), batch_size), start=1):
        batch = ciks[start : start + batch_size]
        # One ingestion timestamp per batch.
        ingested_at = datetime.now(timezone.utc)
        rows = []
        skipped = 0
        for cik in batch:
            # Not every company in company_tickers.json has XBRL facts
            # (e.g. foreign private issuers); the API returns 404 for those.
            try:
                payload = client.get_company_facts(cik)
            except (SECClientError, requests.HTTPError) as exc:
                skipped += 1
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
        if rows:
            df = spark.createDataFrame(rows, schema=FACTS_SCHEMA)
            # Idempotent per-CIK MERGE: batches can be re-run without duplicates.
            upsert(spark, df, f"{BRONZE}.company_facts", "t.cik = s.cik")
            upserted += len(rows)
        print(
            f"Batch {batch_no}/{total_batches}: fetched {len(rows)} facts "
            f"({skipped} skipped), {upserted} rows in bronze so far."
        )
    return upserted


if __name__ == "__main__":
    ingest_company_facts(spark)
