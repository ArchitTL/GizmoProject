# Databricks notebook source
# MAGIC %md
# MAGIC ### Transform Addresses Data
# MAGIC 1. Create one record for each customer with 2 sets of address columns, 1 for shipping and 1 for billing address
# MAGIC 2. Write transformed data to the Silver schema

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.sagizmobox.blob.core.windows.net",
  dbutils.secrets.get(scope="mysecretscope", key="storage-account-access-key")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id,
# MAGIC   address_type,
# MAGIC   address_line_1,
# MAGIC   city,
# MAGIC   state,
# MAGIC   postcode
# MAGIC FROM hive_metastore.gizmobox_bronze.vw_addresses_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC Create 1 record for each customer with both addresses, one for each address_type

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC  FROM (SELECT customer_id,
# MAGIC             address_type,
# MAGIC             address_line_1,
# MAGIC             city,
# MAGIC             state,
# MAGIC             postcode
# MAGIC         FROM hive_metastore.gizmobox_bronze.vw_addresses_bronze)
# MAGIC PIVOT (MAX(address_line_1) AS address_line_1,
# MAGIC        MAX(city) AS city,
# MAGIC        MAX(state) AS state,
# MAGIC        MAX(postcode) AS postcode
# MAGIC        FOR address_type IN ('shipping', 'billing')
# MAGIC        );

# COMMAND ----------

# MAGIC %md
# MAGIC Write transformed data to the silver schema: 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.gizmobox_silver.tbl_addresses_silver
# MAGIC AS
# MAGIC SELECT *
# MAGIC  FROM (SELECT customer_id,
# MAGIC             address_type,
# MAGIC             address_line_1,
# MAGIC             city,
# MAGIC             state,
# MAGIC             postcode
# MAGIC         FROM hive_metastore.gizmobox_bronze.vw_addresses_bronze)
# MAGIC PIVOT (MAX(address_line_1) AS address_line_1,
# MAGIC        MAX(city) AS city,
# MAGIC        MAX(state) AS state,
# MAGIC        MAX(postcode) AS postcode
# MAGIC        FOR address_type IN ('shipping', 'billing')
# MAGIC        );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.gizmobox_silver.tbl_addresses_silver;

# COMMAND ----------

