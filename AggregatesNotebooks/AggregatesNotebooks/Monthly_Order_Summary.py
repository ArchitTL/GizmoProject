# Databricks notebook source
# MAGIC %md
# MAGIC ### Monthly Order Summary
# MAGIC For each of the customer, produce the following summary per month
# MAGIC
# MAGIC 1. total orders
# MAGIC 2. total items bought
# MAGIC 3. total amount spent

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.gizmobox_silver.tbl_orders_silver WHERE customer_id = 5816 ;

# COMMAND ----------

# MAGIC %md
# MAGIC Create a order summary table:

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS hive_metastore.gizmobox_gold.order_summary_monthly;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.gizmobox_gold.tbl_order_summary_monthly_gold
# MAGIC AS
# MAGIC SELECT date_format(transaction_timestamp, 'yyyy-MM') AS order_month,
# MAGIC        customer_id, 
# MAGIC        COUNT(DISTINCT order_id) AS total_orders,
# MAGIC        SUM(quantity) AS total_items_bought,
# MAGIC        SUM(price * quantity) AS total_amount
# MAGIC  FROM hive_metastore.gizmobox_silver.tbl_orders_silver
# MAGIC  GROUP BY order_month, customer_id ;

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Order Summary:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.gizmobox_gold.tbl_order_summary_monthly_gold;

# COMMAND ----------

