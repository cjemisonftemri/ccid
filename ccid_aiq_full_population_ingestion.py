# Databricks notebook source
# MAGIC %run /Workspace/Shared/util/storage_account_access 

# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql import SparkSession 
from pyspark.sql.types import *
         
now = datetime.now()

 date_str = now.strftime("%Y/%m/%d")

full_population_table_name = "bronze_alwayson.aiq_full_population_april24"
full_population_path = analyticsiq_root +"AIQ_250MFullPopulationFile_MRI_extract_dyn100_HH_W_MRIID_April2024_*"

full_population_output_path = bronze_root + f"bronze_schema/analyticsiq/full_population/{date_str}"
full_population_output_path = full_population_output_path + "/" + str(get_version(full_population_output_path))

# COMMAND ----------

df = spark.read.csv(full_population_path, header=True, sep="|")

spark.sql(f"drop table if exists {full_population_table_name}")
df.write\
    .format("delta")\
    .option("compression", "snappy")\
    .option("path", full_population_output_path)\
    .saveAsTable(full_population_table_name)

# COMMAND ----------

df = spark.sql(f"Select * from {full_population_table_name}")
df.printSchema()
print(df.count())
