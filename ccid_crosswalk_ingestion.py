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

crosswalk_table_name = "bronze_alwayson.tu_crosswalk_070724"
crosswalk_path = transunion_root + "FullPopulation/Attribute/"

crosswalk_output_path = bronze_root + f"bronze_schema/transunion/crosswalk/{date_str}"
crosswalk_output_path = crosswalk_output_path + "/" + str(get_version(crosswalk_output_path))

# COMMAND ----------



# COMMAND ----------

aiq_table_name = "bronze_alwayson.aiq_full_population_april24"
hhid = "100000585"

query = f"Select AIQ_HHID, AIQ_INDID from {aiq_table_name} where AIQ_HHID = '{hhid}'"
df = spark.sql(query)
df.printSchema()
df.show()


# COMMAND ----------

from pyspark.sql import functions as F

col = "CCUID"

# Record was found.... this is data that aiq sent to tu.

df = spark.read.parquet(transunion_root + "AIQ_TO_TU_PII_20240425/*.psv")
df = df.withColumnRenamed("Customer Unique ID", col)
df = df.filter(F.col(col) == F.lit("100000585_1")).select(col)
df.show(1, vertical=True, truncate=True)

# Record was not found.... this is data aiq sent to mri (us)

aiq_table_name = "bronze_alwayson.aiq_full_population_april24"
hhid = "100000585"
df1 = spark.sql(f"select * from {aiq_table_name}")
df1 = df1.filter(F.col("AIQ_HHID") == F.lit(hhid))
df1.show(1, vertical=True, truncate=True)
df1 = df1.withColumn(col, F.concat(F.col("AIQ_HHID"), F.lit("_"), F.col("AIQ_INDID")))

# this record shows up as missing again

df = spark.read.parquet(transunion_root + "AIQ_TO_TU_PII_20240425/*.psv")
df = df.withColumnRenamed("Customer Unique ID", col)

df1 = spark.sql(f"select * from {aiq_table_name}")
df1 = df1.withColumn(col, F.concat(F.col("AIQ_HHID"), F.lit("_"), F.col("AIQ_INDID")))

diff = df.alias("a").join(
    df1.alias("b"), F.col("a.CCUID") == F.col("b.CCUID"), "leftanti"
).select("a.CCUID")
print(f"Diff Count = {diff.count()}")

diff.show(vertical=True, truncate=True)



# COMMAND ----------

df = spark.read.csv(analyticsiq_root + "AIQ_250MFullPopulationFile_MRI_extract_dyn100_HH_W_MRIID_April2024_*",
                    header=True, 
                    sep="|") 
print(df.count())
df = df.filter(F.col("AIQ_HHID") == F.lit(hhid))
df.show(vertical=True, truncate=True)

# COMMAND ----------

col = "KEY"

# this is the dataset going to transunion from aiq. 263787913 rows/records
df = spark.read.parquet(transunion_root + "AIQ_TO_TU_PII_20240425/*.psv")
df = df.withColumnRenamed("Customer Unique ID", col)
print(df.count())

# this is the april 2024 dataset sent to mri by aiq. 263796981 rows/records
aiq_table_name = "bronze_alwayson.aiq_full_population_april24"
df1 = spark.sql(f"select * from {aiq_table_name}")
df1 = df1.withColumn(col, F.concat(F.col("AIQ_HHID"), F.lit("_"), F.col("AIQ_INDID")))
print(df1.count())

# caching both keys from each data for faster comparison operations.
df = df.select(col).cache()
df1 = df1.select(col).cache()

# 1 delta
diff = df.subtract(df1)
print(f"Diff Count = {diff.count()}")
diff.coalesce(1).write.mode("overwrite").csv(sandbox_root + "records_missing_from_april_2024_dataset.csv", header=True)

# 9069 deltas
diff = df1.subtract(df)
print(f"Diff Count = {diff.count()}")
diff.coalesce(1).write.mode("overwrite").csv(sandbox_root + "records_missing_from_tu_dataset.csv", header=True)

