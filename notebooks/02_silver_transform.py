# Databricks notebook source
from pyspark.sql import functions as F

# Silver: trade flows (from Comtrade)
comtrade_bronze = spark.table("afcfta_trade.bronze.comtrade_raw")


M49_TO_ISO3 = {"404": "KEN", "566": "NGA", "818": "EGY", "710": "ZAF", "288": "GHA"}
map_expr = F.create_map([F.lit(x) for pair in M49_TO_ISO3.items() for x in pair])

trade_flows_silver = (
    comtrade_bronze
    .withColumn("reporter_iso3", map_expr[F.col("reporterCode").cast("string")])
    .select(
        F.col("reporter_iso3"),
        F.col("partnerISO").alias("partner_iso3"),
        F.lpad(F.col("cmdCode"), 2, "0").alias("hs_chapter"),
        F.col("period").cast("int").alias("year"),
        F.col("flowCode").alias("flow_code"),
        F.col("primaryValue").alias("trade_value_usd"),
        F.col("reporterDesc").alias("reporter_name"),
        F.col("cmdDesc").alias("product_name"),
        F.col("_ingested_at"),
        F.col("_source"),
    )
    .withColumn("trade_value_usd_is_null", F.col("trade_value_usd").isNull())
)

COUNTRY_NAMES = {"KEN": "Kenya", "NGA": "Nigeria", "EGY": "Egypt", "ZAF": "South Africa", "GHA": "Ghana"}
CHAPTER_NAMES = {"08": "Edible fruit and nuts", "09": "Coffee, tea, mate and spices", "52": "Cotton"}
name_map = F.create_map([F.lit(x) for pair in COUNTRY_NAMES.items() for x in pair])
chapter_map = F.create_map([F.lit(x) for pair in CHAPTER_NAMES.items() for x in pair])

trade_flows_silver = (
    trade_flows_silver
    .withColumn("reporter_name", name_map[F.col("reporter_iso3")])
    .withColumn("product_name", chapter_map[F.col("hs_chapter")])
)

trade_flows_silver.write.format("delta").mode("overwrite").saveAsTable("afcfta_trade.silver.trade_flows")
print(f"silver.trade_flows: {trade_flows_silver.count()} rows")
trade_flows_silver.select("reporter_iso3").distinct().show()

# Silver: tariffs (from WITS)
M49_TO_ISO3 = {"404": "KEN", "566": "NGA", "818": "EGY", "710": "ZAF", "288": "GHA"}
map_expr = F.create_map([F.lit(x) for pair in M49_TO_ISO3.items() for x in pair])

wits_bronze = spark.table("afcfta_trade.bronze.wits_tariffs_raw")

tariffs_silver = (
    wits_bronze
    .withColumn("reporter_iso3", map_expr[F.col("Reporter").cast("string")])
    .select(
        F.col("reporter_iso3"),
        F.lpad(F.col("Product").cast("string"), 2, "0").alias("hs_chapter"),
        F.col("Tariff_Year").alias("year"),
        F.col("DutyType").alias("duty_type"),          # AHS = applied incl. preferential, MFN = most-favored-nation
        F.col("Simple_Average").alias("simple_avg_tariff_pct"),
        F.col("Reporter_Name").alias("reporter_name"),
        F.col("Product_Name").alias("product_name"),
    )
)

tariffs_silver.write.format("delta").mode("overwrite").saveAsTable("afcfta_trade.silver.tariffs")
print(f"silver.tariffs: {tariffs_silver.count()} rows")

# COMMAND ----------

comtrade_bronze.select("reporterCode", "reporterDesc", "reporterISO").distinct().show()