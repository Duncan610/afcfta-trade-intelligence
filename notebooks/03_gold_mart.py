# Databricks notebook source
from pyspark.sql import functions as F

trade_flows = spark.table("afcfta_trade.silver.trade_flows")
tariffs = spark.table("afcfta_trade.silver.tariffs")

# Pivot AHS (applied, incl. preferential) vs MFN into side-by-side columns
# per reporter/hs_chapter, so gold consumers can compare them directly.
tariffs_pivoted = (
    tariffs
    .groupBy("reporter_iso3", "hs_chapter", "product_name")
    .pivot("duty_type", ["AHS", "MFN"])
    .agg(F.first("simple_avg_tariff_pct"))
    .withColumnRenamed("AHS", "applied_tariff_pct")
    .withColumnRenamed("MFN", "mfn_tariff_pct")
    .withColumn("tariff_year", F.lit(2023))  # only year available, see README
)

# Aggregate trade flow value per reporter/hs_chapter/year/flow direction
trade_agg = (
    trade_flows
    .groupBy("reporter_iso3", "hs_chapter", "year", "flow_code")
    .agg(F.sum("trade_value_usd").alias("total_trade_value_usd"))
)

from pyspark.sql.types import DecimalType

gold_market_opportunity = (
    trade_agg
    .join(tariffs_pivoted, on=["reporter_iso3", "hs_chapter"], how="left")
    .withColumn("tariff_data_year_mismatch", F.col("year") != F.col("tariff_year"))
    .withColumn("applied_tariff_pct", F.round(F.col("applied_tariff_pct"), 2))
    .withColumn("mfn_tariff_pct", F.round(F.col("mfn_tariff_pct"), 2))
    .withColumn(
        "preferential_discount_pct",
        F.round(F.col("mfn_tariff_pct") - F.col("applied_tariff_pct"), 2)
    )
    .withColumn(
        "total_trade_value_usd",
        F.col("total_trade_value_usd").cast(DecimalType(18, 2))
    )
    .select(
        "reporter_iso3", "hs_chapter", "product_name", "year", "flow_code",
        "total_trade_value_usd", "applied_tariff_pct", "mfn_tariff_pct",
        "preferential_discount_pct", "tariff_year", "tariff_data_year_mismatch",
    )
)


gold_market_opportunity.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "afcfta_trade.gold.market_opportunity"
)

print(f"gold.market_opportunity: {gold_market_opportunity.count()} rows")
gold_market_opportunity.orderBy("reporter_iso3", "hs_chapter", "year", "flow_code").show(60, truncate=False)
#gold_market_opportunity.filter(F.col("reporter_iso3") == "KEN").show(20, truncate=False)

# COMMAND ----------

print("silver.trade_flows total:", trade_flows.count())
#print("silver.trade_flows KEN:", trade_flows.filter(F.col("reporter_iso3") == "KEN").count())

trade_flows.select("reporter_iso3").distinct().show()

print("trade_agg total:", trade_agg.count())
#trade_agg.filter(F.col("reporter_iso3") == "KEN").show()

print("tariffs_pivoted total:", tariffs_pivoted.count())
tariffs_pivoted.select("reporter_iso3").distinct().show()

# COMMAND ----------

display(gold_market_opportunity.orderBy("reporter_iso3", "hs_chapter", "year", "flow_code"))