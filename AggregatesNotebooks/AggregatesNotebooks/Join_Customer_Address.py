# Databricks notebook source
# MAGIC %md
# MAGIC ### Join Customer and Address
# MAGIC Join customer data with address data to create a customer_address table which contains the address of each customer on the same record

# COMMAND ----------

# MAGIC %md
# MAGIC Customer Table:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hive_metastore.gizmobox_silver.tbl_customers_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC Address Table:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM hive_metastore.gizmobox_silver.tbl_addresses_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC Create gold schema:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS hive_metastore.gizmobox_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC Create tbl_customer_address_gold:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.gizmobox_gold.tbl_customer_address_gold
# MAGIC AS
# MAGIC SELECT c.customer_id,
# MAGIC        c.customer_name,
# MAGIC        c.email,
# MAGIC        c.date_of_birth,
# MAGIC        c.member_since,
# MAGIC        c.telephone,
# MAGIC        a.shipping_address_line_1,
# MAGIC        a.shipping_city,
# MAGIC        a.shipping_state,
# MAGIC        a.shipping_postcode,
# MAGIC        a.billing_address_line_1,
# MAGIC        a.billing_city,
# MAGIC        a.billing_state,
# MAGIC        a.billing_postcode
# MAGIC   FROM hive_metastore.gizmobox_silver.tbl_customers_silver c
# MAGIC   INNER JOIN hive_metastore.gizmobox_silver.tbl_addresses_silver a 
# MAGIC           ON c.customer_id = a.customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.gizmobox_gold.tbl_customer_address_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer Lifetime Cohort by Region

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   YEAR(member_since) AS join_year,
# MAGIC   billing_state,
# MAGIC   COUNT(DISTINCT customer_id) AS customer_count
# MAGIC FROM hive_metastore.gizmobox_gold.tbl_customer_address_gold
# MAGIC GROUP BY YEAR(member_since), billing_state
# MAGIC ORDER BY join_year, billing_state;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers with Mismatched Shipping/Billing Cities:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hive_metastore.gizmobox_gold.tbl_customer_address_gold
# MAGIC WHERE shipping_city <> billing_city;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Age Group Distribution by State:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   billing_state,
# MAGIC   CASE 
# MAGIC     WHEN YEAR(CURRENT_DATE) - YEAR(date_of_birth) < 25 THEN 'Under 25'
# MAGIC     WHEN YEAR(CURRENT_DATE) - YEAR(date_of_birth) BETWEEN 25 AND 40 THEN '25-40'
# MAGIC     WHEN YEAR(CURRENT_DATE) - YEAR(date_of_birth) BETWEEN 41 AND 60 THEN '41-60'
# MAGIC     ELSE '60+'
# MAGIC   END AS age_group,
# MAGIC   COUNT(*) AS total_customers
# MAGIC FROM hive_metastore.gizmobox_gold.tbl_customer_address_gold
# MAGIC GROUP BY billing_state, age_group
# MAGIC ORDER BY billing_state, age_group;
# MAGIC

# COMMAND ----------

