# Databricks notebook source
# MAGIC %md
# MAGIC ### Transform Orders Data - String to JSON
# MAGIC 1. Pre-process the JSON String to fix the Data Quality Issues
# MAGIC 2. Transform JSON String to JSON Object
# MAGIC 3. Write transformed data to the silver schema

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.sagizmobox.blob.core.windows.net",
  dbutils.secrets.get(scope="mysecretscope", key="storage-account-access-key")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC   FROM hive_metastore.gizmobox_bronze.vw_orders_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC Pre-process the Json string to fix the data quality issues:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW tv_orders_fixed
# MAGIC AS
# MAGIC SELECT value,
# MAGIC        regexp_replace(value, '"order_date": (\\d{4}-\\d{2}-\\d{2})', '"order_date": "\$1"') AS fixed_value 
# MAGIC   FROM hive_metastore.gizmobox_bronze.vw_orders_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC Transform json string to json object:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT schema_of_json(fixed_value) AS schema,
# MAGIC        fixed_value
# MAGIC   FROM tv_orders_fixed
# MAGIC  LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT from_json(fixed_value, 
# MAGIC                  'STRUCT<customer_id: BIGINT, items: ARRAY<STRUCT<category: STRING, details: STRUCT<brand: STRING, color: STRING>, item_id: BIGINT, name: STRING, price: BIGINT, quantity: BIGINT>>, order_date: STRING, order_id: BIGINT, order_status: STRING, payment_method: STRING, total_amount: BIGINT, transaction_timestamp: STRING>') AS json_value,
# MAGIC        fixed_value
# MAGIC   FROM tv_orders_fixed;

# COMMAND ----------

# MAGIC %md
# MAGIC Write transformed data to the silver schema:

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS hive_metastore.gizmobox_silver.tbl_orders_json_silver;
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.gizmobox_silver.tbl_orders_json_silver
# MAGIC AS
# MAGIC SELECT from_json(fixed_value, 
# MAGIC                  'STRUCT<customer_id: BIGINT, items: ARRAY<STRUCT<category: STRING, details: STRUCT<brand: STRING, color: STRING>, item_id: BIGINT, name: STRING, price: BIGINT, quantity: BIGINT>>, order_date: STRING, order_id: BIGINT, order_status: STRING, payment_method: STRING, total_amount: BIGINT, transaction_timestamp: STRING>') AS json_value
# MAGIC   FROM tv_orders_fixed;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.gizmobox_silver.tbl_orders_json_silver;

# COMMAND ----------

