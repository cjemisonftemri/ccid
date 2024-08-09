# Databricks notebook source
# MAGIC %run /Workspace/Shared/util/storage_account_access 

# COMMAND ----------

# MAGIC %run ./ccid_util

# COMMAND ----------

# DBTITLE 1,Variables
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql import SparkSession 
from pyspark.sql.types import *

now = datetime.now()
date_str = now.strftime("%Y/%m/%d")

current_year = int(now.strftime("%Y"))

core_attributes_list = [
    "C_CustomerUniqueID",
    "AIQ_HHID",
    "AIQ_INDID",
    "AIQID",
    "city",
    "state",
    "COUNTY",
    "Zip5",
    "Zip4",
    "AIQ_GENDER",
    "AIQ_AGE",
    "COUNTY_CODE",
    "ethnic_code",
    "ethnic_group",
    "Presence_of_Children_v2",
    "AIQ_Adults_in_HH_v2",
    "AIQ_Children_in_HH_v2",
    "AIQ_People_in_HH_v2",
    "AIQ_MARITAL_V2",
    "AIQ_EDUCATION_V2",
    "AIQ_Employment",
    "WEALTH_PROFILES",
    "INCOMEIQ_PLUS_v3",
    "AIQ_BIRTH_YEAR",
    "AIQ_BIRTH_MONTH",
]

aiq_table_name = "bronze_alwayson.aiq_full_population_april24"
claritas_table_name = "bronze_alwayson.claritas_april24"
id_resolution_table_name = "bronze_alwayson.aiq_id_resolution_april24"
aiq_ccid_table_name = "bronze_alwayson.aiq_ccid_april2024"

aiq_data_path = bronze_root + f"bronze_schema/analyticsiq/ccid/{date_str}"
aiq_data_path = aiq_data_path + "/" + str(get_version(aiq_data_path))


# COMMAND ----------

cols = ", ".join(core_attributes_list)

query = f"Select {cols} from {aiq_table_name}"

subset_df = spark.sql(query)
subset_df.createOrReplaceTempView("aiq_april24_tbl")

sclaritas_df = (
    spark.sql(f"Select ST_ABBR, STATE_NAME, FIPS_STATE_CODE from {claritas_table_name}")
    .distinct()
    .cache()
)
sclaritas_df.createOrReplaceTempView("claritas_tbl")

join_query = """
    Select a.*, b.FIPS_STATE_CODE from aiq_april24_tbl a left join claritas_tbl b
    on a.state = b.ST_ABBR
"""

join_result = spark.sql(join_query)

join_result = upcase_columns(join_result)
join_result.createOrReplaceTempView("aiq_tbl")

join_result = spark.sql(
    """
                         SELECT a.*, b.BLOCK, b.BLOCK_GROUP 
                         from aiq_tbl a LEFT JOIN bronze_alwayson.aiq_block_groups_april2024 b
                         on a.AIQ_HHID = b.AIQ_HHID and a.AIQ_INDID = b.AIQ_INDID 
                         """
)
join_result.createOrReplaceTempView("aiq_tbl")

# COMMAND ----------

# DBTITLE 1,Claritas
join_result = (
    join_result.withColumn(
        "MRI_BLOCK",
        F.when(
            F.col("BLOCK").isNotNull(),
            F.concat(
                F.col("FIPS_STATE_CODE").cast(StringType()),
                F.substring(F.col("BLOCK").cast(StringType()), 3, 15),
            ),
        ).otherwise(None),
    )
    .withColumn(
        "MRI_BLOCK_GROUP",
        F.when(
            F.col("BLOCK_GROUP").isNotNull(),
            F.concat(
                F.col("FIPS_STATE_CODE").cast(StringType()),
                F.substring(F.col("BLOCK_GROUP").cast(StringType()), 3, 12),
            ),
        ).otherwise(None),
    )
    .withColumn(
        "INCOME",
        F.when(
            F.col("INCOMEIQ_PLUS_V3").isNotNull(),
            F.concat(F.col("INCOMEIQ_PLUS_V3"), F.lit("000"))
        ).otherwise(0),
    ).withColumn(
        "AGE",
        F.when(
            F.col("AIQ_BIRTH_YEAR").isNotNull(),
            F.lit(current_year - F.col("AIQ_BIRTH_YEAR"))
        ).otherwise(0),
    )
)
join_result.createOrReplaceTempView("aiq_tbl")

# COMMAND ----------

display(join_result)

# COMMAND ----------

# DBTITLE 1,Normalize
@F.udf(returnType=StringType())
def zfill_county_code(val: int) -> str:
    tmp = None
    if val:
        tmp = str(val).zfill(3)
    return tmp

upcase_columns(join_result)
join_result = join_result.alias("a").select(
    "a.*",
    F.col("AIQ_GENDER").alias("GENDER"),
    F.when(F.col("GENDER") == "Male", "M")
    .when(F.col("GENDER") == "Female", "F")
    .otherwise("U")
    .alias("GENDER_VALUE"),
    F.col("AIQ_MARITAL_V2").alias("MARITAL_STATUS"),
    F.when(F.col("AIQ_MARITAL_V2") == "Married", "M")
    .when(F.col("AIQ_MARITAL_V2") != "Married", "S")
    .otherwise("U")
    .alias("MARITAL_STATUS_VALUE"),
    F.when(F.col("ETHNIC_GROUP") == "Y", "Y").otherwise("N").alias("HISPANIC"),
    F.when(F.col("ETHNIC_GROUP") == "F", "Y").otherwise("N").alias("BLACK"),
    F.col("AIQ_EMPLOYMENT").alias("EMPLOYMENT"),
    F.when(F.col("AIQ_EMPLOYMENT").cast(IntegerType()) >= 5, "F")
    .when(F.col("AIQ_EMPLOYMENT").cast(IntegerType()) <= 3, "P")
    .otherwise("U").alias("EMPLOYMENT_STATUS_VALUE"),
    F.col("AIQ_People_in_HH_v2").alias("HOUSEHOLD_SIZE"),
    F.col("AIQ_ADULTS_IN_HH_V2").alias("NUMBER_OF_ADULTS_IN_HOUSEHOLD"),
    F.col("AIQ_CHILDREN_IN_HH_V2").alias("NUMBER_OF_CHILDREN_IN_HOUSEHOLD"),
    F.col("AIQ_Education_v2").alias("EDUCATION"),
    F.when(F.col("GENDER_VALUE") == "M", "Y").otherwise("N").alias("MALE"),
    F.when(F.col("GENDER_VALUE") == "F", "Y").otherwise("N").alias("FEMALE"),
    F.when(F.col("MARITAL_STATUS_VALUE") == "M", "Y").otherwise("N").alias("MARRIED"),
    F.when(F.col("MARITAL_STATUS_VALUE") == "S", "Y").otherwise("N").alias("SINGLE"),
    F.when(F.col("EMPLOYMENT_STATUS_VALUE") == "F", "Y")
    .otherwise("N")
    .alias("FULL_TIME_EMPLOYMENT"),
    F.when(F.col("EMPLOYMENT_STATUS_VALUE") == "P", "Y")
    .otherwise("N")
    .alias("PART_TIME_EMPLOYMENT"),
    F.when(F.col("EDUCATION") == "HS Degree", "Y")
    .when(F.col("EDUCATION") == "Less Than HS", "Y")
    .otherwise("N")
    .alias("SOME_HIGH_SCHOOL"),
    F.when(F.col("EDUCATION") == "HS Degree", "Y")
    .otherwise("N")
    .alias("HIGH_SCHOOL_DIPLOMA"),
    F.when(F.col("EDUCATION") == "Bachelor Degree", "Y")
    .otherwise("N")
    .alias("BACHELORS_DEGREE"),
    F.when(F.col("EDUCATION") == "Bachelor Degree", "Y")
    .otherwise("N")
    .alias("SOME_COLLEGE"),
    F.when(F.col("EDUCATION") == "Graduate Degree", "Y")
    .otherwise("N")
    .alias("GRADUATE_DEGREE"),
    F.when((F.col("NUMBER_OF_CHILDREN_IN_HOUSEHOLD") > 0), "Y")
    .otherwise("N")
    .alias("PRESENCE_OF_CHILDREN"),
    zfill_county_code(F.col("COUNTY_CODE")).alias("FIPS_COUNTY_CODE"),
    F.substring(F.col("MRI_BLOCK_GROUP"), 5, 11).alias("FIPS_CENSUS_TRACT"),
    F.substring(F.col("MRI_BLOCK_GROUP"), 11, 11).alias("FIPS_BLOCK_GROUP"),
    F.col("AIQ_BIRTH_YEAR").alias("BIRTH_YEAR"),
    F.col("AIQ_BIRTH_MONTH").alias("BIRTH_MONTH"),
    F.lit("AIQ").alias("DATA_SOURCE"),
)

# COMMAND ----------

# DBTITLE 1,ID Resolution
id_resolution_df = spark.sql(f"select * from {id_resolution_table_name}")
id_resolution_df = id_resolution_df.withColumnRenamed("C_CustomerUniqueID", "MRI_ID")
id_resolution_df = upcase_columns(id_resolution_df)

join_result = (
    join_result.alias("a")
    .join(
        id_resolution_df.alias("b"),
        (
            (F.col("a.AIQID") == F.col("b.AIQID"))
            & (F.col("a.AIQ_HHID") == F.col("b.AIQ_HHID"))
        ),
        "left",
    )
    .select("b.MRI_ID", "a.*")
)

# COMMAND ----------

# DBTITLE 1,Save

spark.sql(f"drop table if exists {aiq_ccid_table_name}")
join_result.write\
   .format("delta")\
   .option("path", aiq_data_path)\
   .option("compression", "snappy")\
   .saveAsTable(aiq_ccid_table_name)
