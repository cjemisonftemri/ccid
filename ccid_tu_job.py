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
current_quarter = "april2024"

tu_full_population_table =  f"bronze_alwayson.tu_fullpopulation_{current_quarter}"
tu_id_resolution_table =  f"bronze_alwayson.tu_id_resolution_{current_quarter}"

attributes = ["TU_Extern_ID",
             "TU_Extern_HHID"
]

with open("./tu_household_incomes.txt", "r") as file:
    l = file.readlines()
    l = [x.strip() for x in l]
    l = [x.replace(" ", "") for x in l]
    l = [x.replace("$", "") for x in l]
    l = [x.replace("-", "_") for x in l]
    l = [x.replace(",", "") for x in l] 

rename_dict = {
    "MA1693": "Gender",
    "MA4249": "Ethnic_Group",
    "MA1699": "Ethnic_Race_Code",
    "MA2929": "Married_",
    "MA2930": "Single_",
    "MA1698": "Marital Status",
    "MA4348": "Household Income",
    "MA2676": "Fips County Code",
    "MA4507": "Fips State Code",
    "MA1783": "Census Tract",
    "MA1784": "Census Block Group",
    "MA1899": "Has Children",
    "MA1884": "Number of people in household.",
    "MA2783": "College Graduate",
    "MA2784": "Graduate School",
    "MA2781": "High School",
    "MA1694": "Age",
    "MA2782": "Some College_"
}

attributes.extend([x for x in rename_dict.keys()])

tu_ccid_table_name = "bronze_alwayson.tu_ccid_july24"

tu_data_path = bronze_root + f"bronze_schema/transunion/ccid/{date_str}"
tu_data_path = tu_data_path + "/" + str(get_version(tu_data_path))

# COMMAND ----------

tu_data_df = spark.sql(f"select * from {tu_full_population_table}")
tu_data_df = upcase_columns(tu_data_df)


tu_data_df = tu_data_df.select(*attributes).withColumnsRenamed(rename_dict).alias("a")
tu_data_df = tu_data_df.select(
    "a.*",
    F.concat(
        F.col("Fips State Code"),
        F.col("Fips County Code"),
        F.col("Census Tract"),
        F.col("Census Block Group"),
    ).alias("FIPS"),
)

for x in tu_data_df.columns:
    tu_data_df = tu_data_df.withColumnRenamed(x, x.replace(" ", "_"))
tu_data_df = upcase_columns(tu_data_df)

tu_data_df = tu_data_df.withColumnRenamed(
    "NUMBER_OF_PEOPLE_IN_HOUSEHOLD.", "NUMBER_OF_PEOPLE_IN_HOUSEHOLD"
)

tu_data_df = tu_data_df.alias("a").select(
    "a.*",
    F.when(F.col("GENDER") == "M", "M")
    .when(F.col("GENDER") == "F", "F")
    .otherwise("U")
    .alias("GENDER_VALUE"),
    F.when(F.col("GENDER_VALUE") == "M", "Y").otherwise("N").alias("MALE"),
    F.when(F.col("GENDER_VALUE") == "F", "Y").otherwise("N").alias("FEMALE"),
    F.when(F.col("MARITAL_STATUS") == "S", "S")
    .when(F.col("MARITAL_STATUS") == "M", "M")
    .otherwise("U")
    .alias("MARITAL_STATUS_VALUE"),
    F.when(F.col("MARITAL_STATUS_VALUE") == "M", "Y").otherwise("N").alias("MARRIED"),
    F.when(F.col("MARITAL_STATUS_VALUE") == "S", "Y").otherwise("N").alias("SINGLE"),
    F.when(F.col("ETHNIC_GROUP") == "O", "Y")
    .when(F.col("ETHNIC_GROUP") != "O", "N")
    .otherwise("N").alias("HISPANIC"),
    F.when(F.col("ETHNIC_GROUP") == "B", "Y")
    .when(F.col("ETHNIC_GROUP") != "B", "N")
    .otherwise("N").alias("BLACK"),
    F.lit(None).alias("EMPLOYMENT"),
    F.lit("U").alias("EMPLOYMENT_STATUS_VALUE"),
    F.when(F.col("EMPLOYMENT_STATUS_VALUE") == "F", "Y")
    .otherwise("N")
    .alias("FULL_TIME_EMPLOYMENT"),
    F.when(F.col("EMPLOYMENT_STATUS_VALUE") == "P", "Y")
    .otherwise("N")
    .alias("PART_TIME_EMPLOYMENT"),
    F.col("NUMBER_OF_PEOPLE_IN_HOUSEHOLD").cast(IntegerType()).alias("HOUSEHOLD_SIZE"),
    F.lit(0).alias("NUMBER_OF_ADULTS_IN_HOUSEHOLD"),
    F.lit(0).alias("NUMBER_OF_CHILDREN_IN_HOUSEHOLD"),
    F.when(F.col("HIGH_SCHOOL") == "1", "Y").otherwise("N").alias("SOME_HIGH_SCHOOL"),
    F.when(F.col("HIGH_SCHOOL") == "1", "Y")
    .otherwise("N")
    .alias("HIGH_SCHOOL_DIPLOMA"),
    F.when(F.col("COLLEGE_GRADUATE") == "1", "Y").otherwise("N").alias("SOME_COLLEGE"),
    F.when(F.col("COLLEGE_GRADUATE") == "1", "Y")
    .otherwise("N")
    .alias("BACHELORS_DEGREE"),
    F.when(F.col("GRADUATE_SCHOOL") == "1", "Y")
    .otherwise("N")
    .alias("GRADUATE_DEGREE"),
    F.when(F.col("HAS_CHILDREN") == "1", "Y")
    .otherwise("N")
    .alias("PRESENCE_OF_CHILDREN"),
    F.lit(None).alias("EDUCATION"),
    F.lit("TU").alias("DATA_SOURCE"),
    F.lit(None).alias("BLOCK"),
    F.col("FIPS").alias("BLOCK_GROUP")
)

# COMMAND ----------

# DBTITLE 1,ID Resolution
id_resolution_df = spark.sql(f"select * from {tu_id_resolution_table}")
id_resolution_df = upcase_columns(id_resolution_df)

tu_data_df = (
    tu_data_df.alias("a")
    .join(
        id_resolution_df.alias("b"),
        (
            (F.col("a.TU_EXTERN_ID") == F.col("b.TU_EXTERN_ID"))
            & (F.col("a.TU_EXTERN_HHID") == F.col("b.TU_EXTERN_HHID"))
        ),
        "left",
    )
    .select("b.MRI_ID", "a.*")
)

# COMMAND ----------

# DBTITLE 1,Save
spark.sql(f"drop table if exists {tu_ccid_table_name}")
tu_data_df.write\
   .format("delta")\
   .option("path", tu_data_path)\
   .option("compression", "snappy")\
   .saveAsTable(tu_ccid_table_name)
