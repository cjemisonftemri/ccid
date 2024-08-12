# Databricks notebook source
# DBTITLE 1,Init
# MAGIC %run /Workspace/Shared/util/storage_account_access 

# COMMAND ----------

# DBTITLE 1,Run
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import uuid
import datetime
from delta.tables import *
import json

spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.selfDestruct.enabled", "true")

now = datetime.datetime.now()
date_str = now.strftime("%Y/%m/%d")


# crosswalk_table_name = "silver_alwayson.crosswalk_db23"
crosswalk_table_name = "silver_alwayson.combined_crosswalk"
fusion_filename = "tu_dma_Match_Final.csv"
drop_tables: bool = False

#### Run ####
bronze_table_path = (
    bronze_root + "bronze_schema/consumer_canvas/fusion/fusion_dma_match"
)

crosswalk_path = silver_root + "silver_schema/consumer_canvas/crosswalk/"

attributes = [
    "MRI_ID",
    "TU_HHID",
    "TU_INDID",
    "CCID",
    "FIPS_CODE",
    "FinalWgt",
    "FinalHhldWgt",
    "VENDOR_QUARTER_CREATED",
    "CREATED_DATE",
    "META"
]

if drop_tables:
    fusion_df = spark.sql(
        """SELECT * FROM bronze_alwayson.tu_fusion_dma_match@v0"""
    )
    current_quarter = "Q32023"
    meta = {
        "MRI_DATASET": "FALLDB23",
        "TU_DATASET": "Fall 2023",
        "DMA_MATCH": "bronze_alwayson.fusion_dma_match@v3"
    }
    meta = json.dumps(meta)

else:
    #### BACKUP ####
    backup_path = silver_root + "silver_schema/consumer_canvas/crosswalk_backup/"
    backup_path = backup_path + str(get_version(backup_path))

    crosswalk_df = spark.sql(f""" SELECT * FROM {crosswalk_table_name}""")
    crosswalk_df.write.parquet(backup_path)

    fusion_df = spark.sql(
        """SELECT * FROM bronze_alwayson.tu_fusion_dma_match@v3"""
    )
    current_quarter = "Q22024"
    meta = {
        "MRI_DATASET": "FALLDB23",
        "TU_DATASET": "July 2024",
        "DMA_MATCH": "bronze_alwayson.fusion_dma_match@v2"
    }
    meta = json.dumps(meta)

fusion_df = (
    fusion_df.withColumnRenamed("MRIRespID", "MRI_ID")
    .withColumnRenamed("AlwaysOnHHID", "TU_HHID")
    .withColumnRenamed("AlwaysOnRecID", "TU_INDID")
    .withColumnRenamed("FIPS_Code", "FIPS_CODE")
)

if drop_tables:
    spark.sql(f"drop view if exists {crosswalk_table_name}_latest")
    spark.sql(f"drop table if exists {crosswalk_table_name}")

    crosswalk_df = (
        fusion_df.withColumn("CCID", F.expr("uuid()"))
        .withColumn("VENDOR_QUARTER_CREATED", F.lit(current_quarter))
        .withColumn("CREATED_DATE", F.expr("current_timestamp()"))
        .withColumn("META", F.lit(meta))
        .select(*attributes)
        .cache()
    )

    crosswalk_df.write.format("delta").mode("overwrite").option(
        "compression", "snappy"
    ).option("path", crosswalk_path).option("overwriteSchema", "true").saveAsTable(
        crosswalk_table_name
    )

else:
    # New Records
    crosswalk_df = spark.sql(f""" SELECT * FROM {crosswalk_table_name}""")
    new_records_df = (
        fusion_df.alias("a")
        .join(
            crosswalk_df.alias("b"),
            (
                (F.col("a.TU_HHID") == F.col("b.TU_HHID"))
                & (F.col("a.TU_INDID") == F.col("b.TU_INDID"))
            ),
            "left_anti",
        )
        .withColumn("CCID", F.expr("uuid()"))
        .withColumn("VENDOR_QUARTER_CREATED", F.lit(current_quarter))
        .withColumn("CREATED_DATE", F.expr("current_timestamp()"))
        .withColumn("META", F.lit(meta))
    )

    unique_attributes = ["TU_HHID", "TU_INDID"]
    new_records_df = new_records_df.drop_duplicates(unique_attributes)
    new_records_df.write.mode("append").format("delta").save(crosswalk_path)

    # Insert Records - MRI_ID not static and changes every fusion run.
    crosswalk_df = spark.sql(f""" SELECT * FROM {crosswalk_table_name}""")
    updated_records_df = (
        fusion_df.alias("a")
        .join(
            crosswalk_df.alias("b"),
            (
                (
                    (F.col("a.TU_HHID") == F.col("b.TU_HHID"))
                    & (F.col("a.TU_INDID") == F.col("b.TU_INDID"))
                )
                & (F.col("a.MRI_ID") != F.col("b.MRI_ID"))
            ),
            "inner",
        )
        .select(
            "a.MRI_ID",
            "a.TU_HHID",
            "a.TU_INDID",
            "b.CCID",
            "a.FIPS_CODE",
            "a.FinalWgt",
            "a.FinalHhldWgt",
        )
        .withColumn("VENDOR_QUARTER_CREATED", F.lit(current_quarter))
        .withColumn("CREATED_DATE", F.expr("current_timestamp()"))
        .withColumn("META", F.lit(meta))
        .join(
            crosswalk_df.alias("b"),
            (
                (F.col("a.TU_HHID") == F.col("b.TU_HHID"))
                & (F.col("a.TU_INDID") == F.col("b.TU_INDID"))
                & (F.col("a.MRI_ID") == F.col("b.MRI_ID"))
            ),
            "left_anti",
        )
    )
    updated_records_df.write.mode("append").format("delta").save(crosswalk_path)

# COMMAND ----------

# DBTITLE 1,CROSSWALK  LATEST VIEW
# MAGIC %sql create or replace view silver_alwayson.combined_crosswalk_latest
# MAGIC as
# MAGIC Select a.* from silver_alwayson.combined_crosswalk a
# MAGIC where a.CREATED_DATE IN (
# MAGIC   Select max(a1.CREATED_DATE) from silver_alwayson.combined_crosswalk a1
# MAGIC   where a.TU_HHID = a1.TU_HHID
# MAGIC   and a.TU_INDID = a1.TU_INDID
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Fall DB 2023 VIEW
# MAGIC %sql
# MAGIC create
# MAGIC or replace view silver_alwayson.tu_mri_crosswalk_fall23 as
# MAGIC SELECT
# MAGIC   a.*
# MAGIC FROM
# MAGIC   silver_alwayson.combined_crosswalk a
# MAGIC where
# MAGIC   (a.tu_hhid, a.tu_indid) IN (
# MAGIC     SELECT
# MAGIC       AlwaysOnHHID,
# MAGIC       AlwaysOnRecID
# MAGIC     FROM
# MAGIC       bronze_alwayson.tu_fusion_dma_match @v1
# MAGIC   )

# COMMAND ----------

# DBTITLE 1,SPRING DB 2024 VIEW
# MAGIC %sql
# MAGIC create
# MAGIC or replace view silver_alwayson.tu_mri_crosswalk_july24 as
# MAGIC SELECT
# MAGIC   a.*
# MAGIC FROM
# MAGIC   silver_alwayson.combined_crosswalk a
# MAGIC where
# MAGIC   (a.tu_hhid, a.tu_indid) IN (
# MAGIC     SELECT
# MAGIC       TU_HHID,
# MAGIC       TU_INDID
# MAGIC     FROM
# MAGIC       bronze_alwayson.tu_fusion_dma_match @v2
# MAGIC   )

# COMMAND ----------

# DBTITLE 1,SPRING DB 2024 - 2 VIEW
# MAGIC %sql
# MAGIC create
# MAGIC or replace view silver_alwayson.tu_mri_crosswalk_july_2_24 as
# MAGIC SELECT
# MAGIC   a.*
# MAGIC FROM
# MAGIC   silver_alwayson.combined_crosswalk a
# MAGIC where
# MAGIC   (a.tu_hhid, a.tu_indid) IN (
# MAGIC     SELECT
# MAGIC       TU_HHID,
# MAGIC       TU_INDID
# MAGIC     FROM
# MAGIC       bronze_alwayson.tu_fusion_dma_match @v3
# MAGIC   )

# COMMAND ----------

# DBTITLE 1,VERSIONS OF THE CCID
# MAGIC
# MAGIC %sql SELECT * FROM silver_alwayson.combined_crosswalk where TU_HHID = '00f4+++oaMOuwwaLXBR1aSSvxg==' 
# MAGIC and TU_INDID = '00f4ATvSXheG3a1h53qPYyhD8Q=='

# COMMAND ----------

# MAGIC %sql SELECT count(*) FROM silver_alwayson.combined_crosswalk

# COMMAND ----------

# MAGIC %sql SELECT count(*) FROM silver_alwayson.combined_crosswalk_latest
