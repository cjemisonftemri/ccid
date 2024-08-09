# Databricks notebook source
# MAGIC %run /Workspace/Shared/util/storage_account_access

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

def upcase_columns(df):
    for column in df.columns:
        df = df.withColumnRenamed(column, column.upper())
    return df

now = datetime.now()
date_str = now.strftime("%Y/%m/%d")
current_quarter = "april2024"

tu_full_population_table = "bronze_alwayson.tu_fullpopulation_april24"
tu_id_resolution_table = f"bronze_alwayson.tu_id_resolution_{current_quarter}"
crosswalk_table = f"bronze_alwayson.crosswalk_{current_quarter}"

tu_fp_data_df = spark.sql(f"SELECT * FROM {tu_full_population_table}")
tu_fp_data_df = upcase_columns(tu_fp_data_df).withColumnRenamed("TU_EXTERN_ID", "TU_INDID").withColumnRenamed("TU_EXTERN_HHID", "TU_HHID")

tu_mri_id_res_df = spark.sql(f"SELECT * FROM {tu_id_resolution_table}")
tu_mri_id_res_df = upcase_columns(tu_mri_id_res_df).withColumnRenamed("TU_EXTERN_ID", "TU_INDID").withColumnRenamed("TU_EXTERN_HHID", "TU_HHID")

crosswalk_df = tu_fp_data_df.alias("a").join(
    tu_mri_id_res_df.alias("b"),
    (F.col("a.TU_HHID") == F.col("b.TU_HHID")) & (F.col("a.TU_INDID") == F.col("b.TU_INDID")),
    "left"
).select(
    "b.MRI_ID", "a.TU_HHID", "a.TU_INDID"
).withColumn(
    "CCID", F.expr("uuid()")
).withColumn(
    "VENDOR_QUARTER_CREATED", F.lit("Q2")
).withColumn(
    "CREATED_DATE", F.expr("current_timestamp()")
)

df = spark.read.load(f'{bronze_root}/bronze_schema/tu_resolution/')

final_df = df.join(
    tu_fp_data_df,
    df["Extern_TUID"] == tu_fp_data_df["TU_INDID"],
    "left"
).select(df["*"], tu_fp_data_df["*"])

final_df.write.mode("overwrite").saveAsTable("silver_alwayson.MRI_Appended_TU_Vars_0424")
