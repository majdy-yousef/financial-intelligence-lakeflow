"""Gold layer: `company_financial_ratios` materialized view.

One row per company (CIK) and fiscal year (2021-2025) with the four key
financial ratios, derived 1:1 from `financial_intelligence.gold.company_financial_summary`:
net_profit_margin (net_income/revenue), equity_ratio (equity/assets),
debt_ratio (liabilities/assets) and return_on_assets (net_income/assets),
each stored as DECIMAL(20,6).
A ratio stays NULL whenever numerator or denominator is NULL, or the
denominator is 0 (no division errors or infinities).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

_GOLD = "financial_intelligence.gold"


def _safe_ratio(numerator, denominator):
    """numerator / denominator as DECIMAL(20,6); NULL if a side is NULL or the
    denominator is 0."""
    return F.when(
        F.col(numerator).isNotNull()
        & F.col(denominator).isNotNull()
        & (F.col(denominator) != 0),
        F.col(numerator) / F.col(denominator),
    ).cast("DECIMAL(20,6)")


@dp.materialized_view(
    name="financial_intelligence.gold.company_financial_ratios",
    comment=(
        """Annual financial ratios per company and fiscal year (2021-2025), derived 1:1 
        from gold.company_financial_summary: net_profit_margin = net_income/revenue, 
        equity_ratio = equity/assets, debt_ratio = liabilities/assets, 
        return_on_assets = net_income/assets. Ratios stay NULL when an input is NULL 
        or the denominator is 0."""
    ),
    schema="""
        cik BIGINT,
        company_name STRING,
        fiscal_year INT,
        net_profit_margin DECIMAL(20,6),
        equity_ratio DECIMAL(20,6),
        debt_ratio DECIMAL(20,6),
        return_on_assets DECIMAL(20,6)
    """,
)
def company_financial_ratios():
    summary = spark.read.table(f"{_GOLD}.company_financial_summary")
    return summary.select(
        "cik",
        "company_name",
        "fiscal_year",
        _safe_ratio("net_income", "revenue").alias("net_profit_margin"),
        _safe_ratio("equity", "assets").alias("equity_ratio"),
        _safe_ratio("liabilities", "assets").alias("debt_ratio"),
        _safe_ratio("net_income", "assets").alias("return_on_assets"),
    )
