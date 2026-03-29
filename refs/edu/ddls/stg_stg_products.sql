CREATE TABLE stg.stg_products (
    product_id integer,
    product_name varchar(255),
    product_category varchar(255),
    price numeric(10,2),
    created_date date,
    dbt_dtm timestamp without time zone
);

COMMENT ON TABLE stg.stg_products IS '상품 데이터의 스테이징 모델';
COMMENT ON COLUMN stg.stg_products.product_id IS '상품의 기본 키';
COMMENT ON COLUMN stg.stg_products.product_name IS '상품 이름';
COMMENT ON COLUMN stg.stg_products.product_category IS '상품 카테고리';
COMMENT ON COLUMN stg.stg_products.price IS '상품 가격';
COMMENT ON COLUMN stg.stg_products.created_date IS '상품 생성일';
COMMENT ON COLUMN stg.stg_products.dbt_dtm IS 'dbt 적재 일시';

