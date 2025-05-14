# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract Data From the Payments Files
# MAGIC 1. Read the files from Payment folder
# MAGIC 2. Create External Table

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
# MAGIC Create external table in bronze layer:

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS hive_metastore.gizmobox_bronze.payments;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.gizmobox_bronze.ext_tbl_payments (
# MAGIC   payment_id INT,
# MAGIC   order_id INT,
# MAGIC   payment_timestamp TIMESTAMP,
# MAGIC   payment_status INT,
# MAGIC   payment_method STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true",
# MAGIC   delimiter = ","
# MAGIC )
# MAGIC LOCATION 'wasbs://dlgizmobox@sagizmobox.blob.core.windows.net/landing/external_data/payments/';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.gizmobox_bronze.ext_tbl_payments;

# COMMAND ----------

# MAGIC %md
# MAGIC Describe the table:

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED hive_metastore.gizmobox_bronze.ext_tbl_payments;

# COMMAND ----------

