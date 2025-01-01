
insert into DIM_COSTUMER

    select 
        customer_id,
        name,
        region
    from RAW_SALE rs
    left join DIM_COSTUMER dc
        on rs.customer_id = dc.customer_id
    where dc.customer_id is null;