# Databricks notebook source
# MAGIC %md
# MAGIC ### Extract Data From the Address Files
# MAGIC 1. read csv files from storage account
# MAGIC 2. Create Addresses view in the Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.sagizmobox.blob.core.windows.net",
  dbutils.secrets.get(scope="mysecretscope", key="storage-account-access-key")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC   FROM read_files('wasbs://dlgizmobox@sagizmobox.blob.core.windows.net/landing/operational_data/addresses/',
# MAGIC                   format => 'csv',
# MAGIC                   delimiter => '\t',
# MAGIC                   header => true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW hive_metastore.gizmobox_bronze.vw_addresses_bronze
# MAGIC AS
# MAGIC SELECT * 
# MAGIC   FROM read_files('wasbs://dlgizmobox@sagizmobox.blob.core.windows.net/landing/operational_data/addresses/',
# MAGIC                   format => 'csv',
# MAGIC                   delimiter => '\t',
# MAGIC                   header => true);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.gizmobox_bronze.vw_addresses_bronze;

# COMMAND ----------

