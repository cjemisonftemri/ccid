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
current_quarter = "april2024"

id_res_table_name = f"bronze_alwayson.tu_id_resolution_{current_quarter}"
id_res_path = transunion_root + "Resolution"

id_res_output_path = bronze_root + f"bronze_schema/transunion/id_resolution/{date_str}"
id_res_output_path = id_res_output_path + "/" + str(get_version(id_res_output_path))

# COMMAND ----------

df = spark.read.csv(id_res_path, header=True, sep="|")

spark.sql(f"drop table if exists {id_res_table_name}")
df.write\
    .format("delta")\
    .option("compression", "snappy")\
    .option("path", id_res_output_path)\
    .saveAsTable(id_res_table_name)

# COMMAND ----------

df = spark.sql(f"Select * from {id_res_table_name}")
df.printSchema()
df.show(1, truncate=False)
