-- Create table for marts.receipt_analysis_mart
-- This table is for analyzing product-level sales info based on receipt data.
drop table if exists marts.receipt_analysis_mart;

create table marts.receipt_analysis_mart (
      order_id int8
    , customer_id int8
    , customer_name varchar(255)
    , order_date timestamp
    , product_id int8
    , product_name varchar(255)
    , product_category varchar(100)
    , item_quantity int8
    , item_price numeric(18, 2)
    , item_total numeric(18, 2),
    dbt_dtm timestamp without time zone
);
