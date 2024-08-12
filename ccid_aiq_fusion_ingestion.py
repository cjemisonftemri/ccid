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
    "silver_schema/consumer_canvas/fusion/AIQ-2024-08-12-12/tu_dma_Match_Final.csv"
]

bronze_table_path = (
    bronze_root + "bronze_schema/consumer_canvas/fusion/aiq_fusion_dma_match"
)
bronze_table_name = "bronze_alwayson.aiq_fusion_dma_match"

silver_table_path = (
    silver_root + "silver_schema/consumer_canvas/aiq_fusion_dma_match"
)
silver_table_name = "silver_alwayson.aiq_fusion_dma_match"

for i, filename in enumerate(files_list):
    df = spark.read.csv(silver_root + filename, header=True)

    df.write.format("delta").mode("overwrite").option(
            "compression", "snappy"
    ).option("overwriteSchema", True).save(bronze_table_path)

# COMMAND ----------

# MAGIC %sql select * from bronze_alwayson.tu_fusion_dma_match@v3
