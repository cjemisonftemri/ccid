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

aiq_fusion_table_name = "bronze_alwayson.aiq_fusion_dma_match@v1"

fusion_df = spark.sql(f"select * from {aiq_fusion_table_name}")
initial_run = False
current_quarter = "Q32024"

crosswalk_view_name = "silver_alwayson.aiq_mri_crosswalk_latest"
crosswalk_table_name = "silver_alwayson.aiq_mri_crosswalk"
crosswalk_path = silver_root + "silver_schema/consumer_canvas/aiq_mri_crosswalk"

attributes = [
    "MRI_ID",
    "AIQ_HHID",
    "AIQ_INDID",
    "CCID",
    "FIPS_CODE",
    "FinalWgt",
    "FinalHhldWgt",
    "VENDOR_QUARTER_CREATED",
    "CREATED_DATE",
    "META",
]
fusion_df = spark.sql(F"""SELECT * FROM {aiq_fusion_table_name}""")

current_quarter = "Q32024"
meta = {
    "MRI_DATASET": "FALLDB23",
    "AIQ_DATASET": "JULY 2024",
    "DMA_MATCH": "bronze_alwayson.aiq_fusion_dma_match@v1",
}
meta = json.dumps(meta)

if initial_run:
    spark.sql(f""" drop table if exists {crosswalk_table_name} """)
    spark.sql(f"drop view if exists {crosswalk_view_name}")

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
    crosswalk_df = spark.sql(f"SELECT * FROM {crosswalk_table_name}")
    new_records_df = (
        fusion_df.alias("a")
        .join(
            crosswalk_df.alias("b"),
            (
                (F.col("a.AIQ_HHID") == F.col("b.AIQ_HHID"))
                & (F.col("a.AIQ_INDID") == F.col("b.AIQ_INDID"))
            ),
            "left_anti",
        )
        .withColumn("CCID", F.expr("uuid()"))
        .withColumn("VENDOR_QUARTER_CREATED", F.lit(current_quarter))
        .withColumn("CREATED_DATE", F.expr("current_timestamp()"))
        .withColumn("META", F.lit(meta))
    )

    unique_attributes = ["AIQ_HHID", "AIQ_INDID"]
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
                    (F.col("a.AIQ_HHID") == F.col("b.AIQ_HHID"))
                    & (F.col("a.AIQ_INDID") == F.col("b.AIQ_INDID"))
                )
                & (F.col("a.MRI_ID") != F.col("b.MRI_ID"))
            ),
            "inner",
        )
        .select(
            "a.MRI_ID",
            "a.AIQ_HHID",
            "a.AIQ_INDID",
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
                (F.col("a.AIQ_HHID") == F.col("b.AIQ_HHID"))
                & (F.col("a.AIQ_INDID") == F.col("b.AIQ_INDID"))
                & (F.col("a.MRI_ID") == F.col("b.MRI_ID"))
            ),
            "left_anti",
        )
    )
    updated_records_df.write.mode("append").format("delta").save(crosswalk_path)

# COMMAND ----------

# MAGIC %sql create or replace view silver_alwayson.aiq_mri_crosswalk_latest
# MAGIC as
# MAGIC Select a.* from silver_alwayson.aiq_mri_crosswalk a
# MAGIC where a.CREATED_DATE IN (
# MAGIC   Select max(a1.CREATED_DATE) from silver_alwayson.aiq_mri_crosswalk a1
# MAGIC   where a.AIQ_HHID = a1.AIQ_HHID
# MAGIC   and a.AIQ_INDID = a1.AIQ_INDID
# MAGIC )

# COMMAND ----------

# MAGIC %sql select * from silver_alwayson.aiq_mri_crosswalk

# COMMAND ----------

# MAGIC %sql select count(*) from silver_alwayson.aiq_mri_crosswalk
