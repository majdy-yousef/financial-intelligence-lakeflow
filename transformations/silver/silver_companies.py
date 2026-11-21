"""Silver layer: `companies` materialized view.

Lakeflow Spark Declarative Pipeline dataset. Parses the raw submissions
JSON payload of `bronze.company_submissions` into a clean company
dimension (one row per CIK), published as
`financial_intelligence.silver.companies` by the silver pipeline.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Partial schema for from_json: only the payload fields used by the silver table.
_ADDRESS_SCHEMA = StructType(
    [
        StructField("street1", StringType()),
        StructField("street2", StringType()),
        StructField("city", StringType()),
        StructField("stateOrCountry", StringType()),
        StructField("stateOrCountryDescription", StringType()),
        StructField("zipCode", StringType()),
        StructField("isForeignLocation", IntegerType()),
        StructField("foreignStateTerritory", StringType()),
        StructField("country", StringType()),
        StructField("countryCode", StringType()),
    ]
)

_ENTITY_SCHEMA = StructType(
    [
        StructField("entityType", StringType()),
        StructField("sic", StringType()),
        StructField("sicDescription", StringType()),
        StructField("ownerOrg", StringType()),
        StructField("name", StringType()),
        StructField("ein", StringType()),
        StructField("description", StringType()),
        StructField("website", StringType()),
        StructField("investorWebsite", StringType()),
        StructField("category", StringType()),
        StructField("fiscalYearEnd", StringType()),
        StructField("stateOfIncorporation", StringType()),
        StructField("stateOfIncorporationDescription", StringType()),
        StructField("addresses", StructType([StructField("business", _ADDRESS_SCHEMA)])),
        StructField("phone", StringType()),
    ]
)


@dp.materialized_view(
    name="companies",
    comment="Company dimension: one row per CIK, parsed from the raw submissions payload.",
    schema="""
        cik BIGINT PRIMARY KEY,
        entityType STRING,
        sic STRING,
        sicDescription STRING,
        ownerOrg STRING,
        name STRING,
        ein STRING,
        description STRING,
        website STRING,
        investorWebsite STRING,
        category STRING,
        fiscal_month INT,
        fiscal_day INT,
        stateOfIncorporation STRING,
        stateOfIncorporationDescription STRING,
        address STRING,
        city STRING,
        stateOrCountry STRING,
        stateOrCountryDescription STRING,
        zipCode STRING,
        isForeignLocation INT,
        foreignStateTerritory STRING,
        country STRING,
        countryCode STRING,
        phone STRING
    """,
)
def companies():
    parsed = (
        spark.read.table("financial_intelligence.bronze.company_submissions")
        .select(F.col("cik"), F.from_json(F.col("payload"), _ENTITY_SCHEMA).alias("e"))
    )
    return parsed.select(
        F.col("cik"),
        F.col("e.entityType"),
        F.col("e.sic"),
        F.col("e.sicDescription"),
        F.col("e.ownerOrg"),
        F.col("e.name"),
        F.col("e.ein"),
        F.col("e.description"),
        F.col("e.website"),
        F.col("e.investorWebsite"),
        F.col("e.category"),
        # fiscalYearEnd is "MMDD" (e.g. "1231"); split into month/day ints.
        F.substring(F.col("e.fiscalYearEnd"), 1, 2).cast("int").alias("fiscal_month"),
        F.substring(F.col("e.fiscalYearEnd"), 3, 2).cast("int").alias("fiscal_day"),
        F.col("e.stateOfIncorporation"),
        F.col("e.stateOfIncorporationDescription"),
        F.concat_ws(
            " ",
            F.col("e.addresses.business.street1"),
            F.col("e.addresses.business.street2"),
        ).alias("address"),
        F.col("e.addresses.business.city"),
        F.col("e.addresses.business.stateOrCountry"),
        F.col("e.addresses.business.stateOrCountryDescription"),
        F.col("e.addresses.business.zipCode"),
        F.col("e.addresses.business.isForeignLocation"),
        F.col("e.addresses.business.foreignStateTerritory"),
        F.col("e.addresses.business.country"),
        F.col("e.addresses.business.countryCode"),
        F.col("e.phone"),
    ).dropDuplicates(["cik"])  # cik is PRIMARY KEY: dedup defensively

