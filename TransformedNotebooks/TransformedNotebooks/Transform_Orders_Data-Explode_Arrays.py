# Databricks notebook source
# MAGIC %md
# MAGIC ### Transform Orders Data - Explode Arrays
# MAGIC 1. Access elements from the JSON object
# MAGIC 2. Deduplicate Array Elements
# MAGIC 3. Explode Arrays
# MAGIC 4. Write the Transformed Data to Silver Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM hive_metastore.gizmobox_silver.tbl_orders_json_silver; 

# COMMAND ----------

# MAGIC %md
# MAGIC Access elements from the json object:

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT json_value.order_id,
# MAGIC         json_value.order_status,
# MAGIC         json_value.payment_method,
# MAGIC         json_value.total_amount,
# MAGIC         json_value.transaction_timestamp,
# MAGIC         json_value.customer_id,
# MAGIC         json_value.items
# MAGIC FROM hive_metastore.gizmobox_silver.tbl_orders_json_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC Deduplicate array elements:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT json_value.order_id,
# MAGIC         json_value.order_status,
# MAGIC         json_value.payment_method,
# MAGIC         json_value.total_amount,
# MAGIC         json_value.transaction_timestamp,
# MAGIC         json_value.customer_id,
# MAGIC         array_distinct(json_value.items) AS items
# MAGIC FROM hive_metastore.gizmobox_silver.tbl_orders_json_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC Explode arrays:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW tv_orders_exploded
# MAGIC AS
# MAGIC SELECT json_value.order_id,
# MAGIC         json_value.order_status,
# MAGIC         json_value.payment_method,
# MAGIC         json_value.total_amount,
# MAGIC         json_value.transaction_timestamp,
# MAGIC         json_value.customer_id,
# MAGIC         explode(array_distinct(json_value.items)) AS item
# MAGIC FROM hive_metastore.gizmobox_silver.tbl_orders_json_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id,
# MAGIC        order_status,
# MAGIC        payment_method,
# MAGIC        total_amount,
# MAGIC        transaction_timestamp,
# MAGIC        customer_id,
# MAGIC        item.item_id,
# MAGIC        item.name,
# MAGIC        item.price,
# MAGIC        item.quantity,
# MAGIC        item.category,
# MAGIC        item.details.brand,
# MAGIC        item.details.color
# MAGIC   FROM tv_orders_exploded;

# COMMAND ----------

# MAGIC %md
# MAGIC Write transformed data to the silver schema:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.gizmobox_silver.tbl_orders_silver
# MAGIC AS
# MAGIC SELECT order_id,
# MAGIC        order_status,
# MAGIC        payment_method,
# MAGIC        total_amount,
# MAGIC        transaction_timestamp,
# MAGIC        customer_id,
# MAGIC        item.item_id,
# MAGIC        item.name,
# MAGIC        item.price,
# MAGIC        item.quantity,
# MAGIC        item.category,
# MAGIC        item.details.brand,
# MAGIC        item.details.color
# MAGIC   FROM tv_orders_exploded;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.gizmobox_silver.tbl_orders_silver;

# COMMAND ----------

