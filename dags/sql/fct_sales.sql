insert into FCT_SALES 
select 
    fct_id,
    sales_id,
    customer_id ,	
    payment_method ,	
    product_id	,
    quantity	,
    sale_date	,
    total_amount 
from RAW_SALES rs
;