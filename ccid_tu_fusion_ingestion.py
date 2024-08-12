# Databricks notebook source
# MAGIC %run /Workspace/Shared/util/storage_account_access 

# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import uuid
import datetime

files_list = [
    "silver_schema/consumer_canvas/fusion/2024-07-31-13/tu_dma_Match_Final.csv",
    "silver_schema/consumer_canvas/fusion/2024-08-01-04/tu_dma_Match_Final.csv",
    "silver_schema/consumer_canvas/fusion/2024-08-06-18/tu_dma_Match_Final.csv",
    "silver_schema/consumer_canvas/fusion/2024-08-11-11/tu_dma_Match_Final.csv",
]

bronze_table_path = (
    bronze_root + "bronze_schema/consumer_canvas/fusion/tu_fusion_dma_match"
)
bronze_table_name = "bronze_alwayson.tu_fusion_dma_match"

silver_table_path = (
    silver_root + "silver_schema/consumer_canvas/tu_fusion_dma_match"
)
silver_table_name = "silver_alwayson.tu_fusion_dma_match"

for i, filename in enumerate(files_list):
    df = spark.read.csv(silver_root + filename)

    if i == 0:
        spark.sql(f"drop table if exists {silver_table_name}")
        df.write.format("delta").option("compression", "snappy").option(
            "path", silver_table_path
        ).saveAsTable(silver_table_name)
    else:
        df.write.format("delta").mode("overwrite").option(
            "compression", "snappy"
        ).option("overwriteSchema", True).save(silver_table_path)

# COMMAND ----------

# MAGIC %sql select * from bronze_alwayson.tu_fusion_dma_match@v3
