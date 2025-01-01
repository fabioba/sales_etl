
insert into ace-mile-446412-j2.SALES.DIM_CUSTOMER

    select 
        customer_id,
        name,
        region
    from ace-mile-446412-j2.SALES.RAW_SALES rs
    left join ace-mile-446412-j2.SALES.DIM_CUSTOMER dc
        on rs.customer_id = dc.customer_id
    where dc.customer_id is null;