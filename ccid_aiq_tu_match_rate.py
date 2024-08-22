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

crosswalk_table_name = "bronze_alwayson.tu_aiq_mri_crosswalk"

crosswalk_df = spark.sql(F"""
Select distinct MRI_Extern_TU_HHID as HHID,
MRI_Extern_TU_INDID as INDID,
AIQI_INDHHID
from {crosswalk_table_name}
where trim(MRI_Extern_TU_HHID) != ""
and trim(MRI_Extern_TU_INDID) != ""
and trim(AIQI_INDHHID) != ""
""")

crosswalk_cnt = crosswalk_df.count()
print("Crosswalk Population Matches: ", crosswalk_cnt)
print("---")

tu_april_fp_df = spark.sql("""select distinct TU_Extern_HHID as HHID, TU_Extern_ID as INDID from bronze_alwayson.tu_full_population_april2024""")

tu_july_fp_df = spark.sql("""select distinct TU_Extern_HHID as HHID, TU_Extern_ID as INDID from bronze_alwayson.tu_full_population_july2024""")

tu_fp_df = tu_april_fp_df.union(tu_july_fp_df).distinct()

tu_unique_attributes = ["HHID", "INDID"]

aiq_fp_df = spark.sql(""" 
select distinct concat(AIQ_HHID, "_", AIQ_INDID) as AIQI_INDHHID from bronze_alwayson.aiq_full_population_july24                  
""")

aiq_unique_attributes = ["AIQI_INDHHID"]

def evaluate(total_population_df, unique_attributes) -> None:
    total_population_cnt = total_population_df.count()
    print("Total Full Population: ", total_population_cnt)

    unique_ids_df = crosswalk_df.select(unique_attributes).distinct()
    missing_from_fp_df = unique_ids_df.subtract(total_population_df)
    missing_from_cw_df = total_population_df.subtract(unique_ids_df)

    unique_ids_cnt = unique_ids_df.count()
    print("Unique Ids: ", unique_ids_cnt)

    missing_from_fp_cnt = missing_from_fp_df.count()
    print("Missing from full population: ", missing_from_fp_cnt, 
          "(records that exist in crosswalk, not in full population)")
    print("Within in crosswalk: ", (total_population_cnt -  missing_from_fp_cnt))

    missing_from_cw_cnt = missing_from_cw_df.count()
    print("Missing from crosswalk: ", missing_from_cw_cnt, 
          "(records that exists in full population, not in crosswalk)")

    match_rate = (total_population_cnt - missing_from_cw_cnt) / total_population_cnt
    print("Match Rate: ", match_rate)

print("\nAIQ (APRIL)")
evaluate(aiq_fp_df, aiq_unique_attributes)
print("----")
print("TU (APRIL)")
evaluate(tu_fp_df, tu_unique_attributes)
print("----")
print("TU (APRIL AND JULY)")
evaluate(tu_april_fp_df.union(tu_july_fp_df).distinct(), tu_unique_attributes)




# COMMAND ----------

# MAGIC %sql select count(*) from bronze_alwayson.aiq_full_population_april24

# COMMAND ----------

df = spark.read.parquet(analyticsiq_root + "hh")
df.count()


# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark.sql import functions as F

spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.selfDestruct.enabled", "true")


@F.udf(returnType=StringType())
def split_aiq_id(s: str, indx: int)-> str:
    tmp = ""
    if s:
        l = s.split("_")
        if l:
            tmp =l[indx]
    return tmp

spark.udf.register("aiq_split", split_aiq_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   distinct y.STATE
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       distinct aiq_split((AIQI_INDHHID), 0) as HHID,
# MAGIC       aiq_split((AIQI_INDHHID), 1) as INDID
# MAGIC     from
# MAGIC       bronze_alwayson.tu_aiq_mri_crosswalk minus
# MAGIC     Select
# MAGIC       distinct AIQ_HHID as HHID,
# MAGIC       AIQ_INDID as INDID
# MAGIC     from
# MAGIC       bronze_alwayson.aiq_full_population_july24
# MAGIC   ) x, bronze_alwayson.aiq_full_population_april24 y
# MAGIC   where x.HHID = y.AIQ_HHID and x.INDID = y.AIQ_INDID
