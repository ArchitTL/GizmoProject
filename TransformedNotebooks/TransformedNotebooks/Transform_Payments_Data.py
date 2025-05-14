# Databricks notebook source
# MAGIC %md
# MAGIC ### Transform Payments Data
# MAGIC 1. Extract Date and Time from payment_timestamp and create new columns payment_date and payment_time
# MAGIC 2. Map payment_status to contain descriptive values
# MAGIC (1-Success, 2-Pending, 3-Cancelled, 4-Failed)
# MAGIC 3. Write transformed data to the Silver schema

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS G2 access to databricks:

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.sagizmobox.blob.core.windows.net",
  dbutils.secrets.get(scope="mysecretscope", key="storage-account-access-key")
)


# COMMAND ----------

# MAGIC %md
# MAGIC Read payments table from bronze schema:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT payment_id,
# MAGIC        order_id,
# MAGIC        payment_timestamp,
# MAGIC        payment_status,
# MAGIC        payment_method
# MAGIC   FROM hive_metastore.gizmobox_bronze.ext_tbl_payments;

# COMMAND ----------

# MAGIC %md
# MAGIC Extract  date and time from payment_timestamp: 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT payment_id,
# MAGIC   order_id,
# MAGIC   CAST(date_format(payment_timestamp, 'yyyy-MM-dd') AS DATE) AS payment_date,
# MAGIC   date_format(payment_timestamp, 'HH:mm:ss') AS payment_time,
# MAGIC   payment_status,
# MAGIC   payment_method
# MAGIC FROM hive_metastore.gizmobox_bronze.ext_tbl_payments;

# COMMAND ----------

# MAGIC %md
# MAGIC Make payment_status to have descriptive values:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT payment_id,
# MAGIC        order_id,
# MAGIC        CAST(date_format(payment_timestamp,'yyyy-MM-dd') AS DATE) AS payment_date,
# MAGIC        date_format(payment_timestamp,'HH:mm:ss') AS payment_time,
# MAGIC        CASE payment_status
# MAGIC          WHEN 1 THEN 'Success'
# MAGIC          WHEN 2 THEN 'Pending'
# MAGIC          WHEN 3 THEN 'Cancelled'
# MAGIC          WHEN 4 THEN 'Failed'
# MAGIC        END AS payment_status,  
# MAGIC        payment_method
# MAGIC   FROM hive_metastore.gizmobox_bronze.ext_tbl_payments;

# COMMAND ----------

# MAGIC %md
# MAGIC Write transformed data to silver layer:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.gizmobox_silver.ext_tbl_payments_silver
# MAGIC AS
# MAGIC SELECT payment_id,
# MAGIC        order_id,
# MAGIC        CAST(date_format(payment_timestamp,'yyyy-MM-dd') AS DATE) AS payment_date,
# MAGIC        date_format(payment_timestamp,'HH:mm:ss') AS payment_time,
# MAGIC        CASE payment_status
# MAGIC          WHEN 1 THEN 'Success'
# MAGIC          WHEN 2 THEN 'Pending'
# MAGIC          WHEN 3 THEN 'Cancelled'
# MAGIC          WHEN 4 THEN 'Failed'
# MAGIC        END AS payment_status,  
# MAGIC        payment_method
# MAGIC   FROM hive_metastore.gizmobox_bronze.ext_tbl_payments;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.gizmobox_silver.ext_tbl_payments_silver;

# COMMAND ----------

