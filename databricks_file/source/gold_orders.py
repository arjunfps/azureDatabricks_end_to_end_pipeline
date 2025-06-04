# Databricks notebook source
# MAGIC %md
# MAGIC # **Fact Orders**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.sql("SELECT * FROM databricks_cata.silver.orders_silver")
df.display()

# COMMAND ----------

df_dimCustomer = spark.sql("SELECT dimCustomerKey, customer_id as dim_customer_id FROM databricks_cata.gold.dimcustomers")

df_dimProducts = spark.sql("SELECT product_id AS dimProductKey, product_id as dim_product_id FROM databricks_cata.gold.dimproducts")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Fact DataFrame**

# COMMAND ----------

df_fact = df.join(df_dimCustomer, df["customer_id"] == df_dimCustomer["dim_customer_id"], how='left').join(df_dimProducts, df["product_id"] == df_dimProducts["dim_product_id"], how="left")

df_fact_new = df_fact.drop("dim_customer_id","dim_product_id","customer_id","product_id")



# COMMAND ----------

df_fact_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Upsert On Fact**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_cata.gold.FactOrders"):
    dlt_obj = DeltaTable.forName(spark, "databricks_cata.gold.FactOrders")

    dlt_obj.alias("trg").merge(df_fact_new.alias("src"), "trg.order_id = src.order_id AND trg.dimCustomerKey = src.dimCustomerKey AND trg.dimProductKey = src.dimProductKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()


else:
    df_fact_new.write.format("delta")\
        .option("path","abfss://gold@azuredatabrickse2e.dfs.core.windows.net/FactOrders")\
        .saveAsTable("databricks_cata.gold.FactOrders")

                             

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_cata.gold.FactOrders