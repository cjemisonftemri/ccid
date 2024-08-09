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

now = datetime.datetime.now()
date_str = now.strftime("%Y/%m/%d")
current_quarter = "april2024"

tu_full_population_table =  "bronze_alwayson.tu_fullpopulation_april24"
tu_id_resolution_table =  f"bronze_alwayson.tu_id_resolution_{current_quarter}"
crosswalk_table =  f"silver_alwayson.crosswalk_{current_quarter}"

crosswalk_csv_path = bronze_root + f"bronze_schema/consumer_canvas/crosswalk/{date_str}"
crosswalk_csv_path = crosswalk_csv_path + "/" + str(get_version(crosswalk_csv_path))

crosswalk_delta_path = silver_root + f"silver_schema/consumer_canvas/crosswalk/{date_str}"
crosswalk_delta_path = crosswalk_delta_path + "/" + str(get_version(crosswalk_delta_path))

def upcase_columns(df):
    return df.withColumnsRenamed(
        {x: x.upper() for x in df.columns}
    )

tu_fp_data_df = spark.sql(f"select * from {tu_full_population_table}")
tu_fp_data_df = upcase_columns(tu_fp_data_df)
tu_fp_data_df = (
    tu_fp_data_df.withColumnRenamed("TU_EXTERN_ID", "TU_INDID")
    .withColumnRenamed("TU_EXTERN_HHID", "TU_HHID")
)

tu_mri_id_res_df = spark.sql(f"select * from {tu_id_resolution_table}")
tu_mri_id_res_df = upcase_columns(tu_mri_id_res_df)
tu_mri_id_res_df = (
    tu_mri_id_res_df.withColumnRenamed("TU_EXTERN_ID", "TU_INDID")
    .withColumnRenamed("TU_EXTERN_HHID", "TU_HHID")
)

crosswalk_df = tu_fp_data_df.alias("a").join(
    tu_mri_id_res_df.alias("b"),
    (
        (F.col("a.TU_HHID") == F.col("b.TU_HHID"))
        & (F.col("a.TU_INDID") == F.col("b.TU_INDID"))
    ),
    "left",
).select(
    "b.MRI_ID", "a.TU_HHID", "a.TU_INDID"
).withColumn("CCID", F.expr("uuid()")) \
.withColumn("VENDOR_QUARTER_CREATED", F.lit("Q2"))\
.withColumn("CREATED_DATE", F.expr("current_timestamp()"))

crosswalk_df.write.csv(crosswalk_csv_path, header=True)

spark.sql(f"drop table if exists {crosswalk_table}")
crosswalk_df = spark.read.csv(crosswalk_csv_path, header=True)
crosswalk_df.printSchema()
crosswalk_df.write.format("delta")\
    .option("compression", "snappy")\
    .option("path", crosswalk_delta_path)\
    .saveAsTable(crosswalk_table)


# COMMAND ----------

# MAGIC %sql select * from silver_alwayson.crosswalk_april2024
