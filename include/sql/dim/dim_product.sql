
insert into ace-mile-446412-j2.SALES.DIM_PRODUCT
    (
        product_id,
        product_name,
        category,
        price
    )


    select distinct
        rs.product_id,
        rs.product_name,
        rs.category,
        rs.price
    from ace-mile-446412-j2.SALES.EXT_RAW_SALES rs
    left join ace-mile-446412-j2.SALES.DIM_PRODUCT dp
        on rs.product_id = dp.product_id
    where dp.product_id is null;