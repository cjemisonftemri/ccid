# Databricks notebook source
# MAGIC %run /Workspace/Shared/util/storage_account_access 

# COMMAND ----------

now = datetime.datetime.now()
date_str = now.strftime("%Y/%m/%d")

aiq_hem_table_name =  "bronze_alwayson.aiq_full_population_hem_april24"
aiq_hem_path = analyticsiq_root + "AIQ_HEM_20240715/*.parquet"

df = spark.read.parquet(aiq_hem_path)
spark.sql(f"drop table if exists {aiq_hem_table_name}")
df.write.format("parquet") \
  .mode("overwrite") \
  .option("compression", "snappy") \
  .option("path", bronze_root +f"bronze_schema/analyticsiq/hem/{date_str}/0") \
  .saveAsTable(aiq_hem_table_name)
