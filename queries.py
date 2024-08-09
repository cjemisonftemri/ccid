# Databricks notebook source
# MAGIC %run /Workspace/Shared/util/storage_account_access

# COMMAND ----------

# MAGIC %sql select * from bronze_alwayson.tu_fullpopulation_april24;

# COMMAND ----------

# MAGIC %sql SELECT * from bronze_alwayson.cr
