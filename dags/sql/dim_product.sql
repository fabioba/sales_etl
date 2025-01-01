
insert into DIM_PRODUCT

    select 
        product_id,
        product_name,
        category,
        price
    from RAW_SALE rs
    left join DIM_PRODUCT dp
        on rs.product_id = dp.product_id
    where dp.product_id is null;