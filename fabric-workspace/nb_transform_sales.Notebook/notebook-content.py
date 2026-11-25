# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4f0f6635-176e-4dfb-b894-c36225dfa9d5",
# META       "default_lakehouse_name": "lh_sales_lab",
# META       "default_lakehouse_workspace_id": "b71bda92-1195-43f9-9cc4-e4c1b0e4a734",
# META       "known_lakehouses": [
# META         {
# META           "id": "4f0f6635-176e-4dfb-b894-c36225dfa9d5"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

run_date = "2026-11-25"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW vw_orders_typed AS
SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    TO_DATE(o.order_date, 'yyyy-MM-dd') AS order_date,
    CAST(o.quantity AS INT) AS quantity,
    CAST(o.discount_pct AS DOUBLE) AS discount_pct,
    o.status,
    c.customer_name,
    c.city,
    c.segment,
    p.product_name,
    p.category,
    CAST(p.unit_price AS DOUBLE) AS unit_price,
    CAST(p.cost_price AS DOUBLE) AS cost_price,
    ROUND(
        CAST(o.quantity AS INT)
        * CAST(p.unit_price AS DOUBLE)
        * (1 - CAST(o.discount_pct AS DOUBLE)),
        2
    ) AS net_revenue,
    ROUND(
        CAST(o.quantity AS INT)
        * CAST(p.cost_price AS DOUBLE),
        2
    )  AS total_cost,
    '{run_date}' AS pipeline_run_date
FROM raw_orders o
INNER JOIN raw_customers c ON o.customer_id = c.customer_id
INNER JOIN raw_products  p ON o.product_id  = p.product_id
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 1. WINDOW FUNCTIONS: Order-level analytics (Completed orders only)
# MAGIC CREATE OR REPLACE TABLE sales_order_analytics AS
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     order_date,
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     city,
# MAGIC     category,
# MAGIC     product_name,
# MAGIC     net_revenue,
# MAGIC     ROUND(net_revenue - total_cost, 2) AS profit,
# MAGIC 
# MAGIC     -- Sequence of orders per customer
# MAGIC     ROW_NUMBER() OVER (
# MAGIC         PARTITION BY customer_id
# MAGIC         ORDER BY order_date, order_id
# MAGIC     ) AS customer_order_seq,
# MAGIC 
# MAGIC     -- Previous order revenue and days elapsed since prior order
# MAGIC     LAG(net_revenue, 1, 0.0) OVER (
# MAGIC         PARTITION BY customer_id
# MAGIC         ORDER BY order_date, order_id
# MAGIC     ) AS prev_order_revenue,
# MAGIC 
# MAGIC     DATEDIFF(
# MAGIC         order_date,
# MAGIC         LAG(order_date) OVER (
# MAGIC             PARTITION BY customer_id
# MAGIC             ORDER BY order_date, order_id
# MAGIC         )
# MAGIC     ) AS days_since_prev_order,
# MAGIC 
# MAGIC     -- Cumulative customer spend over time
# MAGIC     ROUND(SUM(net_revenue) OVER (
# MAGIC         PARTITION BY customer_id
# MAGIC         ORDER BY order_date, order_id
# MAGIC         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ), 2) AS customer_running_spend,
# MAGIC 
# MAGIC     -- Rank orders by revenue within each product category
# MAGIC     DENSE_RANK() OVER (
# MAGIC         PARTITION BY category
# MAGIC         ORDER BY net_revenue DESC
# MAGIC     ) AS category_revenue_rank,
# MAGIC 
# MAGIC     pipeline_run_date
# MAGIC FROM vw_orders_typed
# MAGIC WHERE status = 'Completed';
# MAGIC 
# MAGIC 
# MAGIC -- 2. AGGREGATIONS: City x Category KPI summary (includes conditional counts)
# MAGIC CREATE OR REPLACE TABLE sales_category_city_summary AS
# MAGIC SELECT
# MAGIC     city,
# MAGIC     category,
# MAGIC     COUNT(DISTINCT CASE WHEN status = 'Completed' THEN order_id END) AS completed_orders,
# MAGIC     SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END)            AS cancelled_orders,
# MAGIC     ROUND(SUM(CASE WHEN status = 'Completed' THEN net_revenue ELSE 0 END), 2) AS total_revenue,
# MAGIC     ROUND(SUM(CASE WHEN status = 'Completed' THEN net_revenue - total_cost ELSE 0 END), 2) AS total_profit,
# MAGIC     ROUND(AVG(CASE WHEN status = 'Completed' THEN net_revenue END), 2) AS avg_order_value,
# MAGIC     ROUND(
# MAGIC         100.0 * SUM(CASE WHEN status = 'Completed' THEN net_revenue - total_cost ELSE 0 END)
# MAGIC         / NULLIF(SUM(CASE WHEN status = 'Completed' THEN net_revenue ELSE 0 END), 0),
# MAGIC         2
# MAGIC     ) AS profit_margin_pct
# MAGIC FROM vw_orders_typed
# MAGIC GROUP BY city, category;
# MAGIC 
# MAGIC 
# MAGIC -- 3. LEFT JOIN + AGGREGATION: Preserve customers with zero completed orders (C006)
# MAGIC CREATE OR REPLACE TABLE customer_lifetime_summary AS
# MAGIC SELECT
# MAGIC     c.customer_id,
# MAGIC     c.customer_name,
# MAGIC     c.city,
# MAGIC     c.segment,
# MAGIC     COUNT(o.order_id)   AS completed_order_count,
# MAGIC     COALESCE(ROUND(SUM(o.net_revenue), 2), 0.0)  AS lifetime_spend,
# MAGIC     MAX(o.order_date) AS last_order_date
# MAGIC FROM raw_customers c
# MAGIC LEFT JOIN vw_orders_typed o
# MAGIC     ON c.customer_id = o.customer_id
# MAGIC     AND o.status = 'Completed'
# MAGIC GROUP BY c.customer_id, c.customer_name, c.city, c.segment;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# adding for git
