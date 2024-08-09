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

tu_table_name = "bronze_alwayson.tu_ccid_july24"
aiq_table_name = "bronze_alwayson.aiq_ccid_april2024"

previous_crosswalk_table_name = None
crosswalk_table_name = "bronze_alwayson.crosswalk_july24"

crosswalk_data_path = bronze_root + f"bronze_schema/crosswalk/ccid/{date_str}"
crosswalk_data_path = crosswalk_data_path + "/" + str(get_version(crosswalk_data_path))

core_attributes_list = [
    "MRI_ID",
    "HHID",
    "INDID",
    "MALE",
    "FEMALE",
    "MARRIED",
    "SINGLE",
    "HISPANIC",
    "BLACK",
    "FULL_TIME_EMPLOYMENT",
    "PART_TIME_EMPLOYMENT",
    "SOME_HIGH_SCHOOL",
    "SOME_COLLEGE",
    "BACHELORS_DEGREE",
    "GRADUATE_DEGREE",
    "BLOCK_GROUP",
    "AGE",
    "DATA_SOURCE"
]

# COMMAND ----------

# DBTITLE 1,Create initial crosswalk
import uuid

tu_data_df = spark.sql(f"Select * from {tu_table_name}")
tu_data_df = tu_data_df.withColumnRenamed("TU_EXTERN_HHID", "HHID")
tu_data_df = tu_data_df.withColumnRenamed("TU_EXTERN_ID", "INDID")

aiq_data_df = spark.sql(f"Select * from {aiq_table_name}")
aiq_data_df = aiq_data_df.withColumnRenamed("AIQ_HHID", "HHID")
aiq_data_df = aiq_data_df.withColumnRenamed("AIQ_INDID", "INDID")

# replace with crosswalk file.
init_crosswalk_df = tu_data_df.alias("a").join(
    aiq_data_df.alias("b"),
    F.col("a.MRI_id") == F.col("b.MRI_id"),
    "inner"
).select("a.MRI_ID",
         F.col("b.HHID").alias("TU_HHID"),
         F.col("b.INDID").alias("TU_INDID"),
         F.col("a.HHID").alias("AIQ_HHID"),
         F.col("a.INDID").alias("AIQ_INDID"),
         F.lit(str(uuid.uuid4())).alias("CCID")
)

if previous_crosswalk_table_name:
    # NEEDS WORK!
    previous_crosswalk_df = spark.sql(f"SELECT * from {previous_crosswalk_table_name}")
    crosswalk_df = init_crosswalk_df.union(previous_crosswalk_df)
else:
    crosswalk_df = init_crosswalk_df

crosswalk_df = crosswalk_df.dropDuplicates()

everyone_df = tu_data_df.select(*core_attributes_list).union(
    aiq_data_df.select(*core_attributes_list)
)

# Remove crosswalk entities
everyone_df = everyone_df.alias("a").join(
    crosswalk_df.alias("b"),
    (
        (
            (F.col("a.HHID") == F.col("b.TU_HHID"))
            & (F.col("a.INDID") == F.col("b.TU_INDID"))
        )
        | (
            (F.col("a.HHID") == F.col("b.AIQ_HHID"))
            & (F.col("a.INDID") == F.col("b.AIQ_INDID"))
        )
    ),
    "leftanti",
)
display(everyone_df)


# COMMAND ----------

# DBTITLE 1,attribute count
from pyspark.sql.types import *

@F.udf(returnType=IntegerType())
def get_attribute_count(row) -> int:
    cnt = 4

    if row.MALE == "N" and row.FEMALE == "N":
        cnt -= 1

    if row.MARRIED == "N" and row.SINGLE == "N":
        cnt -= 1
    
    if row.HISPANIC == "N" and row.BLACK == "N":
        cnt -= 1
    
    if row.FULL_TIME_EMPLOYMENT == "N" and row.PART_TIME_EMPLOYMENT == "N":
        cnt -= 1

    return cnt

@F.udf(returnType=StringType())
def get_guid() -> str:
    return str(uuid.uuid4())

everyone_df = everyone_df.withColumn(
    "ATTRIBUTE_COUNT",
    get_attribute_count(F.struct([ F.col(x) for x in core_attributes_list]))
)
everyone_df = everyone_df.withColumn("CCID", get_guid())

# COMMAND ----------

# DBTITLE 1,Ranking
from pyspark.sql.window import Window

window_list = [
    "BLOCK_GROUP",
    "ATTRIBUTE_COUNT"
]
w = Window.partitionBy(window_list).orderBy("ATTRIBUTE_COUNT")
everyone_df = everyone_df.withColumn("DENSE_RANK", F.dense_rank().over(w)

# COMMAND ----------

# DBTITLE 1,Join to Claritas population.


# COMMAND ----------

# DBTITLE 1,Join to BG File

