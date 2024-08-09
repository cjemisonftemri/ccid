# Databricks notebook source
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

spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.selfDestruct.enabled", "true")

now = datetime.datetime.now()
date_str = now.strftime("%Y/%m/%d")
current_quarter = "april2024"

# crosswalk_table_name = "silver_alwayson.crosswalk_db23"
crosswalk_table_name = "silver_alwayson.combined_crosswalk"
fusion_filename = "tu_dma_Match_Final.csv"
drop_tables: bool = False

#### BACKUP ####
backup_path = silver_root + "silver_schema/consumer_canvas/crosswalk_backup/"
backup_path = backup_path + str(get_version(backup_path))

# crosswalk_df = spark.sql(f""" SELECT * FROM {crosswalk_table_name}""")
# crosswalk_df.write.parquet(backup_path)

# silver_schema/consumer_canvas/fusion/2024-07-31-13/tu_dma_Match_Final.csv
# silver_schema/consumer_canvas/fusion/2024-08-01-04/tu_dma_Match_Final.csv
# silver_schema/consumer_canvas/fusion/2024-08-06-18/tu_dma_Match_Final.csv
data_set = "2024-08-06-18"

#### Run ###
fusion_file_path = (
    silver_root + f"silver_schema/consumer_canvas/fusion/{data_set}/{fusion_filename}"
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
]

fusion_df = spark.read.csv(fusion_file_path, header=True)
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
        .withColumn("VENDOR_QUARTER_CREATED", F.lit("Q22024"))
        .withColumn("CREATED_DATE", F.expr("current_timestamp()"))
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
        .withColumn("VENDOR_QUARTER_CREATED", F.lit("Q22024"))
        .withColumn("CREATED_DATE", F.expr("current_timestamp()"))
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
        .withColumn("VENDOR_QUARTER_CREATED", F.lit("Q22024"))
        .withColumn("CREATED_DATE", F.expr("current_timestamp()"))
        .join(
            crosswalk_df.alias("b"),
            (
                (F.col("a.TU_HHID") == F.col("b.TU_HHID"))
                & (F.col("a.TU_INDID") == F.col("b.TU_INDID"))
                & (F.col("a.MRI_ID") == F.col("b.MRI_ID"))
            ),
            "left_anti"
        )
    )
    updated_records_df.write.mode("append").format("delta").save(crosswalk_path)

# COMMAND ----------

# MAGIC %sql create or replace view silver_alwayson.combined_crosswalk_latest
# MAGIC as
# MAGIC Select a.* from silver_alwayson.combined_crosswalk a
# MAGIC where a.CREATED_DATE IN (
# MAGIC   Select max(a1.CREATED_DATE) from silver_alwayson.combined_crosswalk a1
# MAGIC   where a.TU_HHID = a1.TU_HHID
# MAGIC   and a.TU_INDID = a1.TU_INDID
# MAGIC )

# COMMAND ----------

# MAGIC
# MAGIC %sql SELECT * FROM silver_alwayson.combined_crosswalk where TU_HHID = '00f4+++oaMOuwwaLXBR1aSSvxg==' 
# MAGIC and TU_INDID = '00f4ATvSXheG3a1h53qPYyhD8Q=='

# COMMAND ----------


fusion_file_path = silver_root + "silver_schema/consumer_canvas/fusion/2024-08-01-04/tu_dma_Match_Final.csv"
df = spark.read.csv(fusion_file_path, header=True)
df = (
    df.withColumnRenamed("MRIRespID", "MRI_ID")
    .withColumnRenamed("AlwaysOnHHID", "TU_HHID")
    .withColumnRenamed("AlwaysOnRecID", "TU_INDID")
    .withColumnRenamed("FIPS_Code", "FIPS_CODE")
)

df = df.filter(F.col("TU_HHID") == F.lit("00f4+++oaMOuwwaLXBR1aSSvxg=="))\
  .filter(F.col("TU_INDID") == F.lit("00f4ATvSXheG3a1h53qPYyhD8Q=="))

display(df)

fusion_file_path =  silver_root + "silver_schema/consumer_canvas/fusion/2024-08-06-18/tu_dma_Match_Final.csv"
df1 = spark.read.csv(fusion_file_path, header=True)
df1 = (
    df1.withColumnRenamed("MRIRespID", "MRI_ID")
    .withColumnRenamed("AlwaysOnHHID", "TU_HHID")
    .withColumnRenamed("AlwaysOnRecID", "TU_INDID")
    .withColumnRenamed("FIPS_Code", "FIPS_CODE")
)
df1= df1.filter(F.col("TU_HHID") == F.lit("00f4+++oaMOuwwaLXBR1aSSvxg=="))\
  .filter(F.col("TU_INDID") == F.lit("00f4ATvSXheG3a1h53qPYyhD8Q=="))

display(df1)

# COMMAND ----------

# MAGIC %sql SELECT * FROM silver_alwayson.combined_crosswalk where TU_HHID = '00f4+3NlTf1RfmrOPBO5CrEkQA==' 
# MAGIC and TU_INDID = '00f4KUXbWPpXLorMPky+xKvA8g=='

# COMMAND ----------

# MAGIC %sql SELECT count(*) FROM silver_alwayson.combined_crosswalk
