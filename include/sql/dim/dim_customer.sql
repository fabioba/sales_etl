
insert into ace-mile-446412-j2.SALES.DIM_CUSTOMER
    (
        customer_id,
        customer_name,
        region
    )

    select distinct
        rs.customer_id,
        rs.customer_name,
        rs.region
    from ace-mile-446412-j2.SALES.EXT_RAW_SALES rs
    left join ace-mile-446412-j2.SALES.DIM_CUSTOMER dc
        on rs.customer_id = dc.customer_id
    where dc.customer_id is null;