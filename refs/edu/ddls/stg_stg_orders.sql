CREATE TABLE stg.stg_orders (
    order_id integer PRIMARY KEY,
    customer_id integer,
    order_date timestamp without time zone,
    total_amount numeric(10,2),
    dbt_dtm timestamp without time zone
);

COMMENT ON TABLE stg.stg_orders IS '주문 데이터의 스테이징 모델';
COMMENT ON COLUMN stg.stg_orders.order_id IS '주문의 기본 키';
COMMENT ON COLUMN stg.stg_orders.customer_id IS '고객에 대한 외래 키';
COMMENT ON COLUMN stg.stg_orders.order_date IS '주문 날짜';
COMMENT ON COLUMN stg.stg_orders.total_amount IS '주문 총액';
COMMENT ON COLUMN stg.stg_orders.dbt_dtm IS 'dbt 적재 일시';

