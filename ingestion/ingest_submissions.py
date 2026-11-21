"""Ingestion step: company submissions from SEC EDGAR.

Runs standalone (`spark` must be in scope), e.g. as its own job task.
"""

import json
import requests
from datetime import datetime, timezone

from bronze_schema import SUBMISSIONS_SCHEMA
from common import BRONZE, USER_AGENT, upsert
from sec_client import SECClient, SECClientError


def ingest_company_submissions(spark):
    """Fetch the submissions JSON per company and append one row per company to bronze.

    The payload column stores the raw, unmodified JSON string.

    The companies to fetch are always taken from bronze.companies.
    """
    client = SECClient(user_agent=USER_AGENT)

    # Source the CIKs from bronze.companies; this task runs after ingest_companies.
    ciks = [
        row.cik
        for row in spark.table(f"{BRONZE}.companies")
        .select("cik")
        .distinct()
        .collect()
    ]

    # One ingestion timestamp for every row of this run.
    ingested_at = datetime.now(timezone.utc)

    rows = []      # successfully fetched companies -> written to bronze
    failures = []  # failed CIKs -> only logged, do not abort the run
    for cik in ciks:
        try:
            payload = client.get_company_submissions(cik)

        except (requests.HTTPError, SECClientError) as exc:
            # Log and skip a failing CIK so the rest of the run continues.
            failures.append((int(cik), str(exc)))
            continue

        # Tuple order must match SUBMISSIONS_SCHEMA.
        rows.append(
            (
                int(cik),
                json.dumps(payload),
                ingested_at,
                client.SUBMISSIONS_URL.format(cik=client.format_cik(cik)),
            )
        )
    df = spark.createDataFrame(rows, schema=SUBMISSIONS_SCHEMA)
    if failures:
        # Log failed fetches to the task output; retried on the next run.
        print(f"Skipped {len(failures)} CIKs after errors: {[f[0] for f in failures]}")
        for cik, err in failures:
            print(f"  cik={cik}: {err}")

    # Idempotent upsert into bronze.company_submissions (MERGE on cik).
    return upsert(spark, df, f"{BRONZE}.company_submissions", "t.cik = s.cik")


if __name__ == "__main__":
    ingest_company_submissions(spark)
