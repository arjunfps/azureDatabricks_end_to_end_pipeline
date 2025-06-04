# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading from source**

# COMMAND ----------

df = spark.sql("SELECT * FROM databricks_cata.silver.customers_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Removing duplicates**

# COMMAND ----------

df = df.dropDuplicates(subset=['customer_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Diving Old vs New Records**

# COMMAND ----------

if init_load_flag == 0:
    df_old = spark.sql('''SELECT dimCustomerKey, customer_id, create_date, update_date 
                       FROM databricks_cata.gold.DimCustomers''')
    
else:
    df_old = spark.sql('''SELECT 0 dimCustomerKey, 0 customer_id, 0 create_date, 0 update_date 
                       FROM databricks_cata.silver.customers_silver WHERE 1 = 0''')


# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Renaming columns of df_old**

# COMMAND ----------

df_old = df_old.withColumnRenamed("dimCustomerKey", "old_dimCustomerKey")\
    .withColumnRenamed("customer_id", "old_customer_id")\
    .withColumnRenamed("create_date", "old_create_date")\
    .withColumnRenamed("update_date", "old_update_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Applying join with old records**

# COMMAND ----------

df_join = df.join(df_old, df.customer_id == df_old.old_customer_id, 'left')

# COMMAND ----------

df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Seperating New vs Old Records**

# COMMAND ----------

df_new = df_join.filter(df_join.old_dimCustomerKey.isNull())

# COMMAND ----------

df_old = df_join.filter(df_join.old_dimCustomerKey.isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Preparing df_old**

# COMMAND ----------

#dropping all the columns which are not required
df_old = df_old.drop("old_customer_id","old_update_date")

#renaming "old_dimCustomerKey" to "dimCustomerKey"
df_old = df_old.withColumnRenamed("old_dimCustomerKey","dimCustomerKey")

#renaming "old_Create_Date" column to "create_date"
df_old = df_old.withColumnRenamed("old_create_date","create_date")
df_old = df_old.withColumn("create_date", to_timestamp(col("create_date")))

# recreating "update_date" column with current time stamp
df_old = df_old.withColumn("update_date", current_timestamp())

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Preparing df_new**

# COMMAND ----------

#dropping all the columns which are not required
df_new = df_new.drop("old_dimCustomerKey","old_customer_id","old_update_date","old_create_date")


# recreating "update_date","current" column with current time stamp
df_new = df_new.withColumn("update_date", current_timestamp())
df_new = df_new.withColumn("create_date", current_timestamp())

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Surrogate Key - From 1**

# COMMAND ----------

df_new = df_new.withColumn("dimCustomerKey",monotonically_increasing_id()+lit(+1))

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Adding Max surrogate Key**

# COMMAND ----------

if init_load_flag == 1:
    max_surrogate_key = 0

else:
    df_maxsurr = spark.sql("SELECT MAX(dimCustomerKey) AS max_surrogate_key FROM databricks_cata.gold.DimCustomers")
    #Converting df_maxsur to max_surrogate_key variable
    max_surrogate_key = df_maxsurr.collect()[0]['max_surrogate_key']

# COMMAND ----------

df_new = df_new.withColumn("dimCustomerKey",lit(max_surrogate_key)+col("dimCustomerKey"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Union of df_old and df_new**

# COMMAND ----------

df_final = df_new.unionByName(df_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SCD Type 1**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_cata.gold.DimCustomers"):
    dlt_obj = DeltaTable.forPath(spark, "abfss://gold@azuredatabrickse2e.dfs.core.windows.net/DimCustomers")

    dlt_obj.alias("trg").merge(df_final.alias("src"),"trg.dimCustomerKey = src.dimCustomerKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()    

else:
    df_final.write.mode("overwrite")\
    .option("path","abfss://gold@azuredatabrickse2e.dfs.core.windows.net/DimCustomers")\
    .saveAsTable("databricks_cata.gold.DimCustomers")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_cata.gold.dimcustomers

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------