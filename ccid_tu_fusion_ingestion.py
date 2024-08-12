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
]

bronze_table_path = (
    bronze_root + "bronze_schema/consumer_canvas/fusion/tu_fusion_dma_match"
)
table_name = "bronze_alwayson.tu_fusion_dma_match"

# for i, filename in enumerate(files_list):

filename = "silver_schema/consumer_canvas/fusion/2024-08-11-11/tu_dma_Match_Final.csv"

df = spark.read.csv(silver_root + filename, header=True)
df.write.format("delta").mode("overwrite").option("compression", "snappy").option(
    "overwriteSchema", True
).save(bronze_table_path)

# if True:
#    df.write.format("delta").option("compression", "snappy").option(
#        "path", bronze_table_path
#    ).saveAsTable(table_name)
# else:
#    df.write.format("delta").mode("overwrite").option(
#        "compression", "snappy"
#    ).option("overwriteSchema", True).save(bronze_table_path)

# COMMAND ----------

# MAGIC %sql select * from bronze_alwayson.tu_fusion_dma_match@v3
