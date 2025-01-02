insert into ace-mile-446412-j2.SALES.EXT_RAW_SALES (
    category ,
    customer_id ,	
    customer_name ,
    payment_method ,	
    price ,	
    product_id	,
    product_name	,
    quantity	,
    region	,
    sale_date	,
    sales_id	,
    total_amount
)
    select
        category ,
        CAST(customer_id as int64) as customer_id,	
        customer_name ,
        payment_method ,	
        CAST(price as float64) as price ,	
        CAST(product_id as int64) as product_id ,	
        product_name	,
        CAST(quantity as int64) as quantity ,	
        region	,
        CAST(sale_date as timestamp) as sale_date ,	
        CAST(sales_id as int64) as sales_id ,	
        CAST(total_amount as float64) as total_amount 
         
    from ace-mile-446412-j2.SALES.RAW_SALES
    where insert_timestamp >= (
        select last_value
        from ace-mile-446412-j2.SALES.CFG_FLOW_MANAGER
    )