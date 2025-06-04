# Databricks notebook source
# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.table("databricks_cata.bronze.regions")
display(df)

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.write.format("delta")\
.mode("overwrite")\
.save("abfss://silver@azuredatabrickse2e.dfs.core.windows.net/regions")

# COMMAND ----------

df = spark.read.format("delta")\
    .load("abfss://silver@azuredatabrickse2e.dfs.core.windows.net/products")


df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.regions_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@azuredatabrickse2e.dfs.core.windows.net/regions'