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
current_quarter = "july2024"

full_population_table_name = f"bronze_alwayson.tu_full_population_{current_quarter}"
#full_population_path = transunion_root+"FullPopulation/Attribute"
#full_population_path = transunion_root +"TU_Spring2024_Q32024Refresh_July2024"
full_population_path = sandbox_root + "TU_Fall23_Demo_Race"

full_population_output_path = bronze_root + f"bronze_schema/transunion/full_population/{date_str}"
full_population_output_path = full_population_output_path + "/" + str(get_version(full_population_output_path))

# COMMAND ----------

df = spark.read.parquet(sandbox_root + "TU_Fall23_Demo_Race/parquet_file")


# COMMAND ----------

spark.sql(f"drop table if exists {full_population_table_name}")
df.write\
    .format("delta")\
    .option("compression", "snappy")\
    .option("path", full_population_output_path)\
    .saveAsTable(full_population_table_name)

# COMMAND ----------

# MAGIC %sql select count(*) from bronze_alwayson.tu_full_population_july2024

# COMMAND ----------

path = transunion_root +"TU_Spring2024_Q32024Refresh_July2024"

df = spark.read.parquet(path)
display(df)

# COMMAND ----------

# DBTITLE 1,column_fix
import collections

path = transunion_root +"TU_Spring2024_Q32024Refresh_July2024"
df = spark.read.parquet(path)
display(df)

def has_duplicates(s: str) -> bool:
    l = s.split('_')
    c = collections.Counter(l)
    for k,v in c.items():
        if v > 1:
            return True
    return False

d = {
    x: x.split('_')[0] for x in df.columns if '_' in x and has_duplicates(x)
}

d['IN1_MRI_ID'] = 'MRI_ID'
d['MA1900_TU_Extern_ID'] = 'TU_Extern_ID'
d['MA3890_TU_Extern_HHID'] = 'TU_Extern_HHID'

df = df.withColumnsRenamed(d)
df.printSchema()

full_population_output_path = bronze_root + "bronze_schema/transunion/full_population/2024/08/01/0"
df.write\
    .mode("overwrite")\
    .format("delta")\
    .option("overwriteSchema", "True")\
    .option("compression", "snappy")\
    .option("path", full_population_output_path)\
    .saveAsTable(full_population_table_name)

# COMMAND ----------

# MAGIC %sql select * from bronze_alwayson.tu_full_population_july2024
