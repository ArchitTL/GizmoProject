# Databricks notebook source
# MAGIC %md
# MAGIC ### Transfrom Customer Data
# MAGIC 1. Remove records with null values in customer_id
# MAGIC 2. Remove exact dupliacte records
# MAGIC 3. Remove duplicates based on created time_stamp
# MAGIC 4. Cast the columns to the corrrect data type
# MAGIC 5. Write the transformed data to the silver schema

# COMMAND ----------

# MAGIC %md
# MAGIC Remove records with null customer_id:

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.sagizmobox.blob.core.windows.net",
  dbutils.secrets.get(scope="mysecretscope", key="storage-account-access-key")
)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Read customers data from the view in bronze layer:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM hive_metastore.gizmobox_bronze.vw_customers_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC Filter nulls on customer id:

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM hive_metastore.gizmobox_bronze.vw_customers_bronze
# MAGIC WHERE customer_id IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC Filter distinct records depending on customer_id:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT *
# MAGIC FROM hive_metastore.gizmobox_bronze.vw_customers_bronze
# MAGIC WHERE customer_id IS NOT NULL
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Create a temporary view to store the distinct records to get rid of the duplicates:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_customers_distinct
# MAGIC AS
# MAGIC SELECT DISTINCT * 
# MAGIC  FROM hive_metastore.gizmobox_bronze.vw_customers_bronze
# MAGIC WHERE customer_id IS NOT NULL
# MAGIC ORDER BY customer_id; 

# COMMAND ----------

# MAGIC %md
# MAGIC Remove duplicates based on created_timestamp:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, MAX(created_timestamp) AS max_created_timestamp
# MAGIC FROM vw_customers_distinct
# MAGIC GROUP BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte_max AS
# MAGIC (
# MAGIC     SELECT customer_id, MAX(created_timestamp) AS max_created_timestamp
# MAGIC     FROM vw_customers_distinct
# MAGIC     GROUP BY customer_id
# MAGIC )
# MAGIC SELECT t. *
# MAGIC FROM vw_customers_distinct t
# MAGIC JOIN cte_max m
# MAGIC     ON t.customer_id = m.customer_id 
# MAGIC     AND t.created_timestamp = m.max_created_timestamp; 

# COMMAND ----------

# MAGIC %md
# MAGIC Type cast the columns to their correct datatypes:

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte_max AS 
# MAGIC (
# MAGIC   SELECT customer_id,
# MAGIC        MAX(created_timestamp) AS max_created_timestamp
# MAGIC   FROM vw_customers_distinct
# MAGIC   GROUP BY customer_id
# MAGIC )
# MAGIC SELECT CAST(t.created_timestamp AS TIMESTAMP) AS created_timestamp,
# MAGIC        t.customer_id,
# MAGIC        t.customer_name,
# MAGIC        CAST(t.date_of_birth AS DATE) AS date_of_birth,
# MAGIC        t.email,
# MAGIC        CAST(t.member_since AS DATE) AS member_since,
# MAGIC        t.telephone
# MAGIC   FROM vw_customers_distinct t
# MAGIC   JOIN cte_max m 
# MAGIC     ON t.customer_id = m.customer_id 
# MAGIC     AND t.created_timestamp = m.max_created_timestamp;

# COMMAND ----------

# MAGIC %md
# MAGIC Create a schema for the silver layer:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS hive_metastore.gizmobox_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC Create customers table in the silver layer:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE hive_metastore.gizmobox_silver.tbl_customers_silver
# MAGIC AS
# MAGIC WITH cte_max AS 
# MAGIC (
# MAGIC   SELECT customer_id,
# MAGIC        MAX(created_timestamp) AS max_created_timestamp
# MAGIC   FROM vw_customers_distinct
# MAGIC   GROUP BY customer_id
# MAGIC )
# MAGIC SELECT CAST(t.created_timestamp AS TIMESTAMP) AS created_timestamp,
# MAGIC        t.customer_id,
# MAGIC        t.customer_name,
# MAGIC        CAST(t.date_of_birth AS DATE) AS date_of_birth,
# MAGIC        t.email,
# MAGIC        CAST(t.member_since AS DATE) AS member_since,
# MAGIC        t.telephone
# MAGIC   FROM vw_customers_distinct t
# MAGIC   JOIN cte_max m 
# MAGIC     ON t.customer_id = m.customer_id 
# MAGIC     AND t.created_timestamp = m.max_created_timestamp;

# COMMAND ----------

# MAGIC %md
# MAGIC Validate if any duplicates present in the silver table:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM hive_metastore.gizmobox_silver.tbl_customers_silver;  

# COMMAND ----------

# MAGIC %md
# MAGIC Describe the table:

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED hive_metastore.gizmobox_silver.tbl_customers_silver;