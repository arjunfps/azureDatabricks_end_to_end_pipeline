# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Streaming Table**

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# Expectations
my_rules = {
    "rule1" : "product_id IS NOT NULL",
    "rule1" : "product_name IS NOT NULL",

}

# COMMAND ----------

@dlt.table()
@dlt.expect_all_or_drop(my_rules)
def DimProducts_Stage():
    df = spark.readStream.table("databricks_cata.silver.products_silver")
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Streaming View**

# COMMAND ----------

@dlt.view()
def DimProducts_View():
    df = spark.readStream.table("Live.DimProducts_Stage")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimProducts**

# COMMAND ----------

dlt.create_streaming_table("DimProducts")

# COMMAND ----------

dlt.apply_changes(
  target = "DimProducts",
  source = "Live.DimProducts_View",
  keys = ["product_id"],
  sequence_by = "product_id",
  stored_as_scd_type = 2,
)

# COMMAND ----------

