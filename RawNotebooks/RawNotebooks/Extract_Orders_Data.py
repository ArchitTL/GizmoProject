# Databricks notebook source
# MAGIC %md
# MAGIC ### Extract Data From the Orders JSON File
# MAGIC 1. Query Orders File using JSON Format
# MAGIC 2. Query Orders File using TEXT Format
# MAGIC 3. Create Orders View in Bronze Schema

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS access to databricks:

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.sagizmobox.blob.core.windows.net",
  dbutils.secrets.get(scope="mysecretscope", key="storage-account-access-key")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Read data from storage account:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM json.`wasbs://dlgizmobox@sagizmobox.blob.core.windows.net/landing/operational_data/orders/`;

# COMMAND ----------

# MAGIC %md
# MAGIC Read in text format:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM text.`wasbs://dlgizmobox@sagizmobox.blob.core.windows.net/landing/operational_data/orders/`;

# COMMAND ----------

# MAGIC %md
# MAGIC Create a view in bronze layer:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW hive_metastore.gizmobox_bronze.vw_orders_bronze AS
# MAGIC SELECT * FROM text.`wasbs://dlgizmobox@sagizmobox.blob.core.windows.net/landing/operational_data/orders/`;

# COMMAND ----------

spark.sql("SELECT * FROM hive_metastore.gizmobox_bronze.vw_orders_bronze").display()

# COMMAND ----------

