"""Silver layer: `company_equity` materialized view.

Lakeflow Spark Declarative Pipeline dataset. Extracts the IFRS concept
`Equity` (taxonomy `ifrs-full`, unit `USD`) from the raw companyfacts
JSON payload of `bronze.company_facts`, one row per reported filing
entry, published as `financial_intelligence.silver.company_equity`.

All original filings are kept, including amended reports (e.g. 40-F/A,
20-F/A, 6-K/A) and multiple filings for the same period — no arbitrary
selection of a single annual value. `fiscal_year` is derived from the
period end date and limited to the business years 2021-2025.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

_CONCEPT = "Equity"
_MEASURE_TYPE = "instant"

# Partial schema for from_json: only the payload fields used by the silver table.
_ENTRY_SCHEMA = StructType(
    [
        StructField("start", StringType()),
        StructField("end", StringType()),
        StructField("val", DoubleType()),
        StructField("accn", StringType()),
        StructField("form", StringType()),
        StructField("filed", StringType()),
    ]
)

_FACTS_SCHEMA = StructType(
    [
        StructField(
            "facts",
            StructType(
                [
                    StructField(
                        "ifrs-full",
                        StructType(
                            [
                                StructField(
                                    _CONCEPT,
                                    StructType(
                                        [
                                            StructField(
                                                "units",
                                                StructType(
                                                    [StructField("USD", ArrayType(_ENTRY_SCHEMA))]
                                                ),
                                            )
                                        ]
                                    ),
                                )
                            ]
                        ),
                    )
                ]
            ),
        )
    ]
)


@dp.materialized_view(
    name="company_equity",
    comment=(
        "IFRS total equity facts (ifrs-full/Equity, USD, fiscal years 2021-2025): one row per "
        "original filing entry, keeping all reports and corrections; instant/balance-sheet "
        "measure (period_end only, period_start is null), parsed from the companyfacts payload."
    ),
    schema="""
        cik BIGINT,
        concept STRING,
        value DOUBLE,
        unit STRING,
        measure_type STRING,
        period_start DATE,
        period_end DATE,
        fiscal_year INT,
        form STRING,
        filed DATE,
        accn STRING
    """,
)
def company_equity():
    bronze = spark.read.table("financial_intelligence.bronze.company_facts").filter(
        F.col("payload").contains('"ifrs-full"')
    )
    parsed = bronze.select(F.col("cik"), F.from_json(F.col("payload"), _FACTS_SCHEMA).alias("f"))
    facts = parsed.select(
        F.col("cik"),
        F.explode(F.col(f"f.facts.`ifrs-full`.{_CONCEPT}.units.USD")).alias("e"),
    )
    return (
        facts.select(
            F.col("cik"),
            F.lit(_CONCEPT).alias("concept"),
            F.col("e.val").alias("value"),
            F.lit("USD").alias("unit"),
            F.lit(_MEASURE_TYPE).alias("measure_type"),
            F.to_date(F.col("e.start")).alias("period_start"),
            F.to_date(F.col("e.end")).alias("period_end"),
            F.year(F.to_date(F.col("e.end"))).alias("fiscal_year"),
            F.col("e.form").alias("form"),
            F.to_date(F.col("e.filed")).alias("filed"),
            F.col("e.accn").alias("accn"),
        )
        .filter(F.col("value").isNotNull())
        .filter(F.col("period_end").isNotNull())
        .filter(F.col("fiscal_year").between(2021, 2025))
        .dropDuplicates()
    )
