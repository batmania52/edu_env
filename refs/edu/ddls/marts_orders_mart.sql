CREATE TABLE marts.orders_mart (
    order_id integer,
    customer_id integer,
    order_date timestamp without time zone,
    total_amount numeric(10,2),
    product_id integer,
    quantity integer,
    price numeric(10,2),
    item_total numeric(10,2),
    dbt_dtm timestamp without time zone
);

COMMENT ON TABLE marts.orders_mart IS '주문 데이터 마트 모델';
COMMENT ON COLUMN marts.orders_mart.order_id IS '주문의 기본 키';
COMMENT ON COLUMN marts.orders_mart.customer_id IS '고객에 대한 외래 키';
COMMENT ON COLUMN marts.orders_mart.order_date IS '주문 날짜';
COMMENT ON COLUMN marts.orders_mart.total_amount IS '주문 총액';
COMMENT ON COLUMN marts.orders_mart.product_id IS '상품 ID';
COMMENT ON COLUMN marts.orders_mart.quantity IS '상품 수량';
COMMENT ON COLUMN marts.orders_mart.price IS '상품 가격';
COMMENT ON COLUMN marts.orders_mart.item_total IS '항목별 총액';
COMMENT ON COLUMN marts.orders_mart.dbt_dtm IS 'dbt 적재 일시';

