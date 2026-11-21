"""Gold layer: `company_financial_summary` materialized view.

One row per company (CIK) and fiscal year (2021-2025) with the annual USD
figures revenue, net_income, assets, liabilities and equity (each
DECIMAL(20,2)), published as
`financial_intelligence.gold.company_financial_summary`. Sources are the
silver companies dimension and the five IFRS USD fact MVs (silver already
guarantees USD and fiscal years 2021-2025). Missing facts stay NULL; all
joins are LEFT joins, so no company is dropped.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

_SILVER = "financial_intelligence.silver"

# Full fiscal-year duration window in days.
_MIN_FY_DAYS = 300
_MAX_FY_DAYS = 400


def _pick_annual_value(table, value_alias, duration):
    """One row per (cik, fiscal_year) with the valid annual value."""
    facts = spark.read.table(f"{_SILVER}.{table}")

    if duration:
        # Duration facts: only complete fiscal years (300-400 days);
        # fiscal_year = year(period_end), never the XBRL fy label.
        facts = facts.filter(
            F.col("period_start").isNotNull()
            & F.col("period_end").isNotNull()
            & F.datediff(F.col("period_end"), F.col("period_start")).between(
                _MIN_FY_DAYS, _MAX_FY_DAYS
            )
        )
    else:
        # Instant facts: balance-sheet value at the fiscal-year-end date.
        facts = facts.filter(F.col("period_end").isNotNull())

    # Duplicates and amended reports: the latest filing wins
    # (filed desc, tie-break accn desc, then value desc for determinism).
    latest_filing = Window.partitionBy("cik", "fiscal_year").orderBy(
        F.col("filed").desc(), F.col("accn").desc(), F.col("value").desc()
    )
    return (
        facts.withColumn("_rank", F.row_number().over(latest_filing))
        .filter(F.col("_rank") == 1)
        .select(
            F.col("cik"),
            F.col("fiscal_year"),
            F.col("value").alias(value_alias),
        )
    )


@dp.materialized_view(
    name="financial_intelligence.gold.company_financial_summary",
    comment=(
        """Annual USD financial summary per company and fiscal year (2021-2025): one row 
        per (cik, fiscal_year) with revenue, net_income, assets, liabilities, equity. 
        Selection: complete fiscal-year durations (300-400 days) or fiscal-year-end 
        instants, latest filing wins (filed, accn, value desc), fiscal year from the 
        period end (never XBRL fy); missing facts stay NULL."""
    ),
    schema="""
        cik BIGINT,
        company_name STRING,
        fiscal_year INT,
        revenue DECIMAL(20,2),
        net_income DECIMAL(20,2),
        assets DECIMAL(20,2),
        liabilities DECIMAL(20,2),
        equity DECIMAL(20,2)
    """,
)
@dp.expect_or_drop(
    "valid_balance_sheet",
    """
    assets IS NULL
    OR liabilities IS NULL
    OR equity IS NULL
    OR (assets - liabilities - equity) = 0
    """
)
def company_financial_summary():
    companies = spark.read.table(f"{_SILVER}.companies").select(
        F.col("cik"),
        F.col("name").alias("company_name"),
    )

    revenue = _pick_annual_value("company_revenue", "revenue", duration=True)
    net_income = _pick_annual_value("company_net_income", "net_income", duration=True)
    assets = _pick_annual_value("company_assets", "assets", duration=False)
    liabilities = _pick_annual_value("company_liabilities", "liabilities", duration=False)
    equity = _pick_annual_value("company_equity", "equity", duration=False)

    # (cik, fiscal_year) keys that have at least one of the five facts.
    keys = (
        revenue.select("cik", "fiscal_year")
        .unionByName(net_income.select("cik", "fiscal_year"))
        .unionByName(assets.select("cik", "fiscal_year"))
        .unionByName(liabilities.select("cik", "fiscal_year"))
        .unionByName(equity.select("cik", "fiscal_year"))
        .distinct()
    )

    return (
        keys.join(companies, on="cik", how="left")
        .join(revenue, on=["cik", "fiscal_year"], how="left")
        .join(net_income, on=["cik", "fiscal_year"], how="left")
        .join(assets, on=["cik", "fiscal_year"], how="left")
        .join(liabilities, on=["cik", "fiscal_year"], how="left")
        .join(equity, on=["cik", "fiscal_year"], how="left")
        .select(
            "cik",
            "company_name",
            "fiscal_year",
            # DECIMAL(20,2) — silver USD values carry at most 2 decimal
            # places, so this cast is lossless for every value.
            F.col("revenue").cast("DECIMAL(20,2)"),
            F.col("net_income").cast("DECIMAL(20,2)"),
            F.col("assets").cast("DECIMAL(20,2)"),
            F.col("liabilities").cast("DECIMAL(20,2)"),
            F.col("equity").cast("DECIMAL(20,2)"),
        )
    )
