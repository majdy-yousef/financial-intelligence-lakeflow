"""Ingestion step: XBRL frames from SEC EDGAR.

Runs standalone (`spark` must be in scope), e.g. as its own job task.
"""

import json
from datetime import datetime, timezone

import requests

from bronze_schema import FRAMES_SCHEMA
from common import (
    ANNUAL_TAGS,
    BRONZE,
    INSTANT_TAGS,
    TAXONOMY,
    UNIT,
    USER_AGENT,
    YEARS,
    upsert,
)
from sec_client import SECClient, SECClientError


def ingest_frames(spark, client=None):
    """Fetch the frames JSON for every tag/unit/period combination and
    upsert one row per combination into bronze.frames.

    Annual (duration) tags are fetched as CY{year}, instant
    (balance-sheet) tags as CY{year}Q4I. The payload column stores the
    raw, unmodified JSON string.
    """
    if client is None:
        client = SECClient(user_agent=USER_AGENT)
    combinations = [
        (TAXONOMY, tag, UNIT, f"CY{year}")
        for year in YEARS
        for tag in ANNUAL_TAGS
    ] + [
        (TAXONOMY, tag, UNIT, f"CY{year}Q4I")
        for year in YEARS
        for tag in INSTANT_TAGS
    ]
    ingested_at = datetime.now(timezone.utc)
    rows = []
    for taxonomy, tag, unit, period in combinations:
        try:
            payload = client.get_company_frames(taxonomy, tag, unit, period)
        except (SECClientError, requests.HTTPError) as exc:
            print(f"WARN: skipping {taxonomy}/{tag}/{unit}/{period}: {exc}")
            continue
        rows.append(
            (
                taxonomy,
                tag,
                unit,
                period,
                json.dumps(payload),
                ingested_at,
                client.FRAMES_URL.format(
                    taxonomy=taxonomy, tag=tag, unit=unit, period=period
                ),
            )
        )
    df = spark.createDataFrame(rows, schema=FRAMES_SCHEMA)
    return upsert(
        spark,
        df,
        f"{BRONZE}.frames",
        "t.taxonomy = s.taxonomy AND t.tag = s.tag "
        "AND t.unit = s.unit AND t.period = s.period",
    )


if __name__ == "__main__":
    ingest_frames(spark)
