# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@azuredatabrickse2e.dfs.core.windows.net/products")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

df.createOrReplaceGlobalTempView("products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Functions**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION  databricks_cata.bronze.discount_func(p_price DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN p_price * 0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_id, price, databricks_cata.bronze.discount_func(price) AS discounted_price 
# MAGIC FROM global_temp.products

# COMMAND ----------

df = df.withColumn("dicounted_price", expr("databricks_cata.bronze.discount_func(price)"))
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_cata.bronze.upper_fucntion(p_brand STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC AS $$
# MAGIC     return p_brand.upper()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_id, brand, databricks_cata.bronze.upper_fucntion(brand) AS brand_upper
# MAGIC FROM global_temp.products

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("path", "abfss://silver@azuredatabrickse2e.dfs.core.windows.net/products")\
    .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.products_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@azuredatabrickse2e.dfs.core.windows.net/products'