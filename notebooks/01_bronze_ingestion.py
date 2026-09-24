# Databricks notebook source
# Comtrade: newline-delimited JSON -> Delta
comtrade_df = spark.read.json("/Volumes/afcfta_trade/bronze/raw_files/comtrade_raw.json")
comtrade_df.write.format("delta").mode("overwrite").saveAsTable("afcfta_trade.bronze.comtrade_raw")
print(f"comtrade_raw: {comtrade_df.count()} rows")

# WITS: 4 CSVs -> single Delta table
wits_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("encoding", "windows-1252")
    .csv("/Volumes/afcfta_trade/bronze/raw_files/wits_tariffs_*.csv")
)
wits_df = wits_df.toDF(*[c.replace(" ", "_") for c in wits_df.columns])
wits_df.write.format("delta").mode("overwrite").saveAsTable("afcfta_trade.bronze.wits_tariffs_raw")
print(f"wits_tariffs_raw: {wits_df.count()} rows")

# COMMAND ----------

spark.table("afcfta_trade.bronze.wits_tariffs_raw").printSchema()

# COMMAND ----------

comtrade_df.printSchema()