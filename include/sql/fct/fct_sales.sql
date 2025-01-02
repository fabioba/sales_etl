insert into ace-mile-446412-j2.SALES.FCT_SALES 
(
    fct_id,
    sales_id,
    customer_id ,	
    payment_method ,	
    product_id	,
    quantity	,
    sale_date	,
    total_amount 
)
select 
    (select COALESCE(MAX(fct_id), 0) AS max_fct_id from ace-mile-446412-j2.SALES.FCT_SALES) + ROW_NUMBER() OVER (ORDER BY sales_id) AS fct_id,
    sales_id,
    customer_id ,	
    payment_method ,	
    product_id	,
    quantity	,
    sale_date	,
    total_amount 
from ace-mile-446412-j2.SALES.EXT_RAW_SALES rs
where not exists (
    select 1
    from ace-mile-446412-j2.SALES.FCT_SALES fs
    where rs.sales_id = fs.sales_id
)