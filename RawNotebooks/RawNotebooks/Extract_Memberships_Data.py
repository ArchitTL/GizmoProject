# Databricks notebook source
# MAGIC %md
# MAGIC ### Extract Data From the Memberships - Image Files
# MAGIC 1. Query Memberships File using binaryFile Format
# MAGIC 2. Create Memberships View in Bronze Schema

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
# MAGIC SELECT * FROM binaryFile.`wasbs://dlgizmobox@sagizmobox.blob.core.windows.net/landing/operational_data/memberships/*/*.png`;

# COMMAND ----------

# MAGIC %md
# MAGIC Create a view in bronze layer:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW hive_metastore.gizmobox_bronze.vw_memberships_bronze 
# MAGIC AS
# MAGIC SELECT * FROM binaryFile.`wasbs://dlgizmobox@sagizmobox.blob.core.windows.net/landing/operational_data/memberships/*/*.png`;

# COMMAND ----------

spark.sql("SELECT * FROM hive_metastore.gizmobox_bronze.vw_memberships_bronze").display()

# COMMAND ----------

