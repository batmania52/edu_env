CREATE TABLE marts.orders_products_mart (
    order_id integer,
    customer_id integer,
    order_date timestamp without time zone,
    total_amount numeric(10,2),
    product_id integer,
    product_name varchar(255),
    product_category varchar(255),
    item_price numeric(10,2),
    product_price numeric(10,2),
    quantity integer,
    product_created_date date
);

COMMENT ON TABLE marts.orders_products_mart IS '주문, 주문 항목 및 상품 데이터 마트 모델';
COMMENT ON COLUMN marts.orders_products_mart.order_id IS '주문의 기본 키';
COMMENT ON COLUMN marts.orders_products_mart.customer_id IS '고객에 대한 외래 키';
COMMENT ON COLUMN marts.orders_products_mart.order_date IS '주문 날짜';
COMMENT ON COLUMN marts.orders_products_mart.total_amount IS '주문 총액';
COMMENT ON COLUMN marts.orders_products_mart.product_id IS '상품에 대한 외래 키';
COMMENT ON COLUMN marts.orders_products_mart.product_name IS '상품 이름';
COMMENT ON COLUMN marts.orders_products_mart.product_category IS '상품 카테고리';
COMMENT ON COLUMN marts.orders_products_mart.item_price IS '주문 항목의 상품 가격';
COMMENT ON COLUMN marts.orders_products_mart.product_price IS '상품의 현재 가격';
COMMENT ON COLUMN marts.orders_products_mart.quantity IS '주문 항목의 상품 수량';
COMMENT ON COLUMN marts.orders_products_mart.product_created_date IS '상품 생성일';
