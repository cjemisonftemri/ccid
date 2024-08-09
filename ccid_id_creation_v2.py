# Databricks notebook source
# MAGIC %run /Workspace/Shared/util/storage_account_access 

# COMMAND ----------

# DBTITLE 1,Init
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import uuid
import datetime

spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.selfDestruct.enabled", "true")


def upcase_columns(df):
    return df.withColumnsRenamed({x: x.upper() for x in df.columns})


now = datetime.datetime.now()
date_str = now.strftime("%Y/%m/%d")
current_quarter = "april2024"
# crosswalk_table_name = "silver_alwayson.crosswalk_db23"
crosswalk_table_name = "silver_alwayson.crosswalk"
crosswalk_history_table_name = "silver_alwayson.crosswalk_history"
drop_tables: bool = False

fusion_filename = "tu_dma_Match_Final.csv"
# silver_schema/consumer_canvas/fusion/2024-07-31-13/tu_dma_Match_Final.csv
# silver_schema/consumer_canvas/fusion/2024-08-01-04/tu_dma_Match_Final.csv
fusion_file_path = (
    silver_root
    + f"silver_schema/consumer_canvas/fusion/2024-08-01-04/{fusion_filename}"
)

crosswalk_path = silver_root + "silver_schema/consumer_canvas/crosswalk/"
crosswalk_history_path = (
    silver_root + "silver_schema/consumer_canvas/crosswalk_history"
)

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
]

fusion_df = spark.read.csv(fusion_file_path, header=True)
fusion_df = (
    fusion_df.withColumnRenamed("MRIRespID", "MRI_ID")
    .withColumnRenamed("AlwaysOnHHID", "TU_HHID")
    .withColumnRenamed("AlwaysOnRecID", "TU_INDID")
    .withColumnRenamed("FIPS_Code", "FIPS_CODE")
)

if drop_tables:
    spark.sql(f"drop table if exists {crosswalk_table_name}")
    spark.sql(f"drop table if exists {crosswalk_history_table_name}")

    crosswalk_df = (
        fusion_df.withColumn("CCID", F.expr("uuid()"))
        .withColumn("VENDOR_QUARTER_CREATED", F.lit("Q22024"))
        .withColumn("CREATED_DATE", F.expr("current_timestamp()"))
        .select(*attributes)
        .cache()
    )

    crosswalk_df.write.format("delta").option(
        "compression", "snappy"
    ).option("path", crosswalk_path).saveAsTable(crosswalk_table_name)

    crosswalk_df.write.format("delta").option(
        "compression", "snappy"
    ).option("path", crosswalk_history_path).saveAsTable(crosswalk_history_table_name)

else:
    changes = False
    # handle new records.
    crosswalk_history_df = spark.sql(
        f""" SELECT * FROM {crosswalk_history_table_name} """
    )

    new_fusion_df = (
        fusion_df.alias("a")
        .join(
            crosswalk_history_df.alias("b"),
            (F.col("a.TU_HHID") == F.col("b.TU_HHID"))
            & (F.col("a.TU_INDID") == F.col("b.TU_INDID")),
            "leftanti",
        )
        .select(
            "a.MRI_ID",
            "a.TU_HHID",
            "a.TU_INDID",
            "a.FIPS_CODE",
            "a.FinalWgt",
            "a.FinalHhldWgt",
        )
        .withColumn("CCID", F.expr("uuid()"))
        .withColumn("VENDOR_QUARTER_CREATED", F.lit("Q22024"))
        .withColumn("CREATED_DATE", F.expr("current_timestamp()"))
        .select(*attributes)
    )

    if new_fusion_df and new_fusion_df.count() > 0:
        changes = True
        print("Appending: ", new_fusion_df.count())
        crosswalk_history_df = crosswalk_history_df.union(new_fusion_df)
        crosswalk_history_df.write.format("delta").mode("append").save(
            crosswalk_history_path
        )

    mri_id_diff_df = (
        fusion_df.alias("a")
        .join(
            crosswalk_history_df.alias("b"),
            (F.col("a.TU_HHID") == F.col("b.TU_HHID"))
            & (F.col("a.TU_INDID") == F.col("b.TU_INDID"))
            & (F.col("a.MRI_ID") != F.col("b.MRI_ID")),
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
        .withColumn("VENDOR_QUARTER_CREATED", F.lit("Q22024"))
        .withColumn("CREATED_DATE", F.expr("current_timestamp()"))
        .select(*attributes)
    )

    if mri_id_diff_df and mri_id_diff_df.count() > 0:
        changes = True
        print("MRI ID Change Appending: ", mri_id_diff_df.count())
        crosswalk_history_df = crosswalk_history_df.union(mri_id_diff_df)
        crosswalk_history_df.write.format("delta").mode("append").save(
            crosswalk_history_path
        )

    if changes:
        # trucate crosswalk table
        spark.sql(f"truncate table {crosswalk_table_name}")
        sql = f"""
        insert into {crosswalk_table_name}
        Select a.* from {crosswalk_history_table_name} a
        where a.CREATED_DATE = (
            Select max(b.CREATED_DATE) from {crosswalk_history_table_name} b
            where a.TU_HHID = b.TU_HHID and b.TU_INDID = b.TU_INDID
        )
        """
        crosswalk_df = spark.sql(sql)



# COMMAND ----------

# MAGIC %sql select count(*) from silver_alwayson.crosswalk

# COMMAND ----------

# MAGIC %sql select count(*) from silver_alwayson.crosswalk_history

# COMMAND ----------

df 
