def model(dbt, session):
  
    orders = dbt.ref("stg_tpch_orders").to_pandas()
    order_items = dbt.ref("order_items").to_pandas()
    
    order_item_summary = order_items.groupby(
        by='ORDER_KEY'
    ).agg({
        'GROSS_ITEM_SALES_AMOUNT': 'sum',
        'ITEM_DISCOUNT_AMOUNT': 'sum',
        'ITEM_TAX_AMOUNT': 'sum',
        'NET_ITEM_SALES_AMOUNT': 'sum',
        'IS_RETURNED': 'sum',
    }).reset_index()

    order_item_summary = order_item_summary.rename(
        columns={
            "IS_RETURNED" : "RETURN_COUNT"
        }
    )
    orders = orders.merge(
        order_item_summary,
        how="inner",
        on="ORDER_KEY"
    )

    orders["ORDER_COUNT"] = 1

    final_orders = orders[[
        "ORDER_KEY",
        "ORDER_DATE",
        "CUSTOMER_KEY",
        "STATUS_CODE",
        "PRIORITY_CODE",
        "CLERK_NAME",
        "SHIP_PRIORITY",
        "ORDER_COUNT",
        "RETURN_COUNT",
        "GROSS_ITEM_SALES_AMOUNT",
        "ITEM_DISCOUNT_AMOUNT",
        "ITEM_TAX_AMOUNT",
        "NET_ITEM_SALES_AMOUNT"
    ]]

    return final_orders