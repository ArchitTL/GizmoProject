# Databricks notebook source
# MAGIC %md
# MAGIC ## Extract Data From the Customers JSON File
# MAGIC
# MAGIC ##### 1. Read customers data from storage account
# MAGIC ##### 2. Create landing schema
# MAGIC ##### 3. Create customers view in landing schema
# MAGIC ##### 4. Create bronze schema
# MAGIC ##### 5. Create customers view in bronze schema

# COMMAND ----------

# MAGIC %md
# MAGIC access ADLS G2 in databricks:

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.sagizmobox.blob.core.windows.net",
  dbutils.secrets.get(scope="mysecretscope", key="storage-account-access-key")
)


# COMMAND ----------

# MAGIC %md
# MAGIC Read customers data:

# COMMAND ----------

df_customers = spark.read \
    .format("json") \
    .load("wasbs://dlgizmobox@sagizmobox.blob.core.windows.net/landing/operational_data/customers/*")

# COMMAND ----------

# MAGIC %md
# MAGIC display information:

# COMMAND ----------

df_customers.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Ensure we use hivemetastore:

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog();  -- Should return 'hive_metastore'

# COMMAND ----------

# MAGIC %md
# MAGIC Create a landing schema:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gizmobox_landing_db;

# COMMAND ----------

# MAGIC %md
# MAGIC Create a landing view:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gizmobox_landing_db.vw_customers_landing AS
# MAGIC SELECT * FROM json.`wasbs://dlgizmobox@sagizmobox.blob.core.windows.net/landing/operational_data/customers/`;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Create bronze schema:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gizmobox_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC Create the bronze layer view:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW hive_metastore.gizmobox_bronze.vw_customers_bronze AS
# MAGIC SELECT * FROM json.`wasbs://dlgizmobox@sagizmobox.blob.core.windows.net/landing/operational_data/customers/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.gizmobox_bronze.vw_customers_bronze;

# COMMAND ----------

