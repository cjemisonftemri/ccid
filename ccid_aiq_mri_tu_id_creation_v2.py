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
from delta.tables import *
import json

spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.selfDestruct.enabled", "true")

now = datetime.datetime.now()
date_str = now.strftime("%Y/%m/%d")

crosswalk_view_name = "silver_alwayson.combined_crosswalk_latest"
crosswalk_table_name = "silver_alwayson.combined_crosswalk"
crosswalk_path = silver_root + "silver_schema/consumer_canvas/combined_crosswalk"

@F.udf(returnType=StringType())
def split_id(s: str, x: int)-> str:
    tmp = None
    if s:
        l = s.split("_")
        if l:
            tmp = l[x]
    return tmp

# COMMAND ----------

# DBTITLE 1,Ingestion TU  - Initial
spark.sql("drop view if exists silver_alwayson.combined_crosswalk_latest")
spark.sql("drop table if exists silver_alwayson.combined_crosswalk")

current_quarter = "Q42023"
meta_dict = {
    "MRI_DATASET": "FALL DB 2023",
    "TU_DATASET": "Fall 2023",
    "DMA_MATCH": "silver_alwayson.tu_fusion_dma_match@v1",
}
meta = json.dumps(meta_dict)

fusion_table_name = meta_dict["DMA_MATCH"]

query = f"select * from {fusion_table_name}"

fusion_df = spark.sql(query)
fusion_df = (
    fusion_df.withColumnRenamed("MRIRespID", "MRI_ID")
    .withColumnRenamed("AlwaysOnRecID", "TU_INDID")
    .withColumnRenamed("AlwaysOnHHID", "TU_HHID")
    .withColumnRenamed("FIPS_Code", "TU_FIPS_CODE")
    .withColumnRenamed("FinalWgt", "TU_FINAL_WGT")
    .withColumnRenamed("FinalHhldWgt", "TU_FINAL_HH_WGT")
)

crosswalk_df = (
    fusion_df.withColumn("CCID", F.expr("uuid()"))
    .withColumn("TU_VENDOR_QUARTER_CREATED", F.lit(current_quarter))
    .withColumn("TU_CREATED_DATE", F.expr("current_timestamp()"))
    .withColumn("TU_META", F.lit(meta))
    .select(
        [
            "MRI_ID",
            "TU_HHID",
            "TU_INDID",
            "CCID",
            "TU_FIPS_CODE",
            "TU_FINAL_WGT",
            "TU_FINAL_HH_WGT",
            "TU_VENDOR_QUARTER_CREATED",
            "TU_CREATED_DATE",
            "TU_META",
        ]
    )
    .cache()
)

crosswalk_df.write.format("delta").mode("overwrite").option(
    "compression", "snappy"
).option("path", crosswalk_path).option("overwriteSchema", "true").saveAsTable(
    crosswalk_table_name
)

# COMMAND ----------

# DBTITLE 1,Ingestion TU - April 2024
current_quarter = "Q22024"
meta_dict = {
    "MRI_DATASET": "FALL DB 2023",
    "TU_DATASET": "APRIL 2024",
    "DMA_MATCH": "silver_alwayson.tu_fusion_dma_match@v2",
}
meta = json.dumps(meta_dict)

fusion_table_name = meta_dict["DMA_MATCH"]

query = f"select * from {fusion_table_name}"

fusion_df = spark.sql(query)
fusion_df = (
    fusion_df.withColumnRenamed("MRIRespID", "MRI_ID")
    .withColumnRenamed("AlwaysOnRecID", "TU_INDID")
    .withColumnRenamed("AlwaysOnHHID", "TU_HHID")
    .withColumnRenamed("FIPS_Code", "TU_FIPS_CODE")
    .withColumnRenamed("FinalWgt", "TU_FINAL_WGT")
    .withColumnRenamed("FinalHhldWgt", "TU_FINAL_HH_WGT")
)

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
    .withColumn("TU_VENDOR_QUARTER_CREATED", F.lit(current_quarter))
    .withColumn("TU_CREATED_DATE", F.expr("current_timestamp()"))
    .withColumn("TU_META", F.lit(meta))
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
        "a.TU_FIPS_CODE",
        "a.TU_FINAL_WGT",
        "a.TU_FINAL_HH_WGT",
    )
    .withColumn("TU_VENDOR_QUARTER_CREATED", F.lit(current_quarter))
    .withColumn("TU_CREATED_DATE", F.expr("current_timestamp()"))
    .withColumn("TU_META", F.lit(meta))
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

# DBTITLE 1,Ingestion TU - July 2024
current_quarter = "Q32024"
meta_dict = {
    "MRI_DATASET": "FALL DB 2023",
    "TU_DATASET": "JULY 2024",
    "DMA_MATCH": "silver_alwayson.tu_fusion_dma_match@v3",
}
meta = json.dumps(meta_dict)

fusion_table_name = meta_dict["DMA_MATCH"]

query = f"select * from {fusion_table_name}"

fusion_df = spark.sql(query)
fusion_df = (
    fusion_df.withColumnRenamed("MRIRespID", "MRI_ID")
    .withColumnRenamed("AlwaysOnRecID", "TU_INDID")
    .withColumnRenamed("AlwaysOnHHID", "TU_HHID")
    .withColumnRenamed("FIPS_Code", "TU_FIPS_CODE")
    .withColumnRenamed("FinalWgt", "TU_FINAL_WGT")
    .withColumnRenamed("FinalHhldWgt", "TU_FINAL_HH_WGT")
)

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
    .withColumn("TU_VENDOR_QUARTER_CREATED", F.lit(current_quarter))
    .withColumn("TU_CREATED_DATE", F.expr("current_timestamp()"))
    .withColumn("TU_META", F.lit(meta))
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
        "a.TU_FIPS_CODE",
        "a.TU_FINAL_WGT",
        "a.TU_FINAL_HH_WGT",
    )
    .withColumn("TU_VENDOR_QUARTER_CREATED", F.lit(current_quarter))
    .withColumn("TU_CREATED_DATE", F.expr("current_timestamp()"))
    .withColumn("TU_META", F.lit(meta))
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

# DBTITLE 1,Crosswalk join -- NON EMAIL MATCH
aiq_mri_tu_crosswalk_table_name = "bronze_alwayson.tu_crosswalk_april2024"

current_quarter = "Q32024"
meta_dict = {
    "MRI_DATASET": "FALL DB 2023",
    "TU_DATASET": "JULY 2024",
    "crosswalk_table": aiq_mri_tu_crosswalk_table_name,
}
meta = json.dumps(meta_dict)

query = f""" 
Select 
a.MRI_ID,
a.TU_HHID,
a.TU_INDID,
b.AIQI_INDHHID,
a.CCID,
a.TU_FIPS_CODE,
a.TU_FINAL_WGT,
a.TU_FINAL_HH_WGT,
a.TU_VENDOR_QUARTER_CREATED,
a.TU_CREATED_DATE
from {crosswalk_table_name} a left join (
select distinct MRI_Extern_TU_HHID as TU_HHID, 
MRI_Extern_TU_INDID AS TU_INDID,
AIQI_INDHHID
from {aiq_mri_tu_crosswalk_table_name} where Email_Match_Flag <> 'Y') b
on a.TU_HHID = b.TU_HHID and a.TU_INDID = b.TU_INDID
"""

crosswalk_df = spark.sql(query)

crosswalk_df = (
  crosswalk_df.withColumn("AIQ_HHID", split_id(F.col("b.AIQI_INDHHID"), F.lit(0)))
  .withColumn("AIQ_INDID", split_id(F.col("b.AIQI_INDHHID"), F.lit(1)))
  .withColumn("META", F.lit(meta))
)

display(crosswalk_df)
crosswalk_df.write.mode("overwrite").format("delta").option("overwriteSchema", True).save(crosswalk_path)


# COMMAND ----------

# DBTITLE 1,Crosswalk join -- Email Match
aiq_mri_tu_crosswalk_table_name = "bronze_alwayson.tu_crosswalk_april2024"

current_quarter = "Q32024"
meta_dict = {
    "MRI_DATASET": "FALL DB 2023",
    "TU_DATASET": "JULY 2024",
    "crosswalk_table": aiq_mri_tu_crosswalk_table_name,
}
meta = json.dumps(meta_dict)

crosswalk_df = spark.sql(f""" SELECT * FROM {crosswalk_table_name}""")

aiq_tu_mri_df = spark.sql(
    f"""select distinct MRI_Extern_TU_HHID as TU_HHID, 
        MRI_Extern_TU_INDID AS TU_INDID, AIQI_INDHHID
     from {aiq_mri_tu_crosswalk_table_name} where Email_Match_Flag = 'Y'
    """
)

delta_df = (
    aiq_tu_mri_df.alias("a")
    .join(
        crosswalk_df.alias("b"),
        (
            (F.col("a.TU_HHID") == F.col("b.TU_HHID"))
            & (F.col("a.TU_INDID") == F.col("b.TU_INDID"))
        ),
        "left_anti",
    )
    .withColumn("MRI_ID", F.lit(""))
    .withColumn("TU_HHID", F.lit(""))
    .withColumn("TU_INDID", F.lit(""))
    .withColumn("AIQ_HHID", split_id(F.col("a.AIQI_INDHHID"), F.lit(0)))
    .withColumn("AIQ_INDID", split_id(F.col("a.AIQI_INDHHID"), F.lit(1)))
    .withColumn("CCID", F.expr("uuid()"))
    .withColumn("TU_FINAL_WGT", F.lit("0"))
    .withColumn("TU_FINAL_HH_WGT", F.lit("0"))
    .withColumn("TU_VENDOR_QUARTER_CREATED", F.lit(current_quarter))
    .withColumn("TU_CREATED_DATE", F.expr("current_timestamp()"))
    .withColumn("META", F.lit(meta))
)
delta_df.write.mode("append").format("delta").option("overwriteSchema", True).save(crosswalk_path)

# COMMAND ----------

# MAGIC %sql create or replace view silver_alwayson.combined_crosswalk_latest
# MAGIC as
# MAGIC Select a.* from silver_alwayson.combined_crosswalk a
# MAGIC where a.CREATED_DATE IN (
# MAGIC   Select max(a1.CREATED_DATE) from silver_alwayson.combined_crosswalk a1
# MAGIC   where a.HHID = a1.HHID
# MAGIC   and a.HH_INDID = a1.HH_INDID
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_alwayson.combined_crosswalk where MRI_ID ='87001W87JR5MGRS'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_alwayson.aiq_fusion_dma_match a,
# MAGIC silver_alwayson.combined_crosswalk b where
# MAGIC a.MRI_ID = b.MRI_ID
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from bronze_alwayson.tu_crosswalk_april2024;
