# Databricks notebook source
# MAGIC %md
# MAGIC ### Transform Memberships Data
# MAGIC 1. Extract customer_id from the file path
# MAGIC 2. Write transformed data to the Silver schema

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.sagizmobox.blob.core.windows.net",
  dbutils.secrets.get(scope="mysecretscope", key="storage-account-access-key")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.gizmobox_bronze.vw_memberships_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC Extract customer_id frm the file path:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regexp_extract(path, '.*/([0-9]+)\\.png$', 1) AS customer_id,
# MAGIC        content AS membership_card
# MAGIC   FROM hive_metastore.gizmobox_bronze.vw_memberships_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC Write tranformed data to the silver schema:

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS hive_metastore.gizmobox_silver.tbl_memberships;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.gizmobox_silver.tbl_memberships_silver
# MAGIC AS
# MAGIC SELECT regexp_extract(path, '.*/([0-9]+)\\.png$', 1) AS customer_id,
# MAGIC        content AS membership_card
# MAGIC   FROM hive_metastore.gizmobox_bronze.vw_memberships_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.gizmobox_silver.tbl_memberships_silver;

# COMMAND ----------

