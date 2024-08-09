# Databricks notebook source
# MAGIC %run /Workspace/Shared/util/storage_account_access 

# COMMAND ----------

from pyspark.sql import functions as F

aiq_tu_mri_path = transunion_root + "Crosswalk/MRI_TU_AIQ_Crosswalk_August2024_08.05.2024"
aiq_tu_mri_df = spark.read.parquet(aiq_tu_mri_path)

aiq_tu_mri_table_name = "bronze_alwayson.tu_crosswalk_april2024"

aiq_tu_mri_table_path = (
    bronze_root + "bronze_schema/transunion/crosswalk"
)

aiq_tu_mri_df.write.format("delta").option(
        "compression", "snappy"
    ).option("path", aiq_tu_mri_table_path).saveAsTable(aiq_tu_mri_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC from
# MAGIC   bronze_alwayson.tu_crosswalk_april2024

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select a.* from
# MAGIC
# MAGIC (select
# MAGIC   MRI_Extern_TU_HHID,
# MAGIC   MRI_Extern_TU_INDID,
# MAGIC   count(*) as `count`
# MAGIC from
# MAGIC   bronze_alwayson.tu_crosswalk_april2024
# MAGIC group by
# MAGIC   MRI_Extern_TU_HHID,
# MAGIC   MRI_Extern_TU_INDID
# MAGIC having
# MAGIC   count(*) > 1) x,
# MAGIC bronze_alwayson.tu_crosswalk_april2024 a
# MAGIC where x.MRI_Extern_TU_HHID = a.MRI_Extern_TU_HHID
# MAGIC and x.MRI_Extern_TU_INDID = a.MRI_Extern_TU_INDID
# MAGIC
