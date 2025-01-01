
insert into ace-mile-446412-j2.SALES.DIM_PRODUCT

    select 
        product_id,
        product_name,
        category,
        price
    from ace-mile-446412-j2.SALES.RAW_SALES rs
    left join ace-mile-446412-j2.SALES.DIM_PRODUCT dp
        on rs.product_id = dp.product_id
    where dp.product_id is null;