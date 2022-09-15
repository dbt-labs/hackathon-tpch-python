from snowflake.snowpark import functions as F

def model(dbt, session):

    dbt.config(
        materialized="table",
        tags=['finance']
    )
  
    orders = dbt.ref("stg_tpch_orders")
    order_item = dbt.ref("order_items")

    order_item_grouped = order_item.groupBy(F.col("order_key"))

    order_item_summary = order_item_grouped \
        .agg(F.sum(F.col("gross_item_sales_amount")).alias("gross_item_sales_amount"))
        

    return order_item_summary