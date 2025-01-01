insert into ace-mile-446412-j2.SALES.FCT_SALES 
select 
    fct_id,
    sales_id,
    customer_id ,	
    payment_method ,	
    product_id	,
    quantity	,
    sale_date	,
    total_amount 
from ace-mile-446412-j2.SALES.RAW_SALES rs
where sales_id not exists (
    select 1
    from ace-mile-446412-j2.SALES.FCT_SALES fs
    where rs.sales_id = fs.sales_id
)
;