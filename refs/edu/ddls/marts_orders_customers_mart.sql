CREATE TABLE marts.orders_customers_mart (
    order_id integer PRIMARY KEY,
    customer_id integer,
    customer_name varchar(255),
    customer_email varchar(255),
    order_date timestamp without time zone,
    total_amount numeric(10,2),
    registration_date date,
    dbt_dtm timestamp without time zone
);

COMMENT ON TABLE marts.orders_customers_mart IS '주문 및 고객 데이터 마트 모델';
COMMENT ON COLUMN marts.orders_customers_mart.order_id IS '주문의 기본 키';
COMMENT ON COLUMN marts.orders_customers_mart.customer_id IS '고객에 대한 외래 키';
COMMENT ON COLUMN marts.orders_customers_mart.customer_name IS '고객 이름';
COMMENT ON COLUMN marts.orders_customers_mart.customer_email IS '고객 이메일';
COMMENT ON COLUMN marts.orders_customers_mart.order_date IS '주문 날짜';
COMMENT ON COLUMN marts.orders_customers_mart.total_amount IS '주문 총액';
COMMENT ON COLUMN marts.orders_customers_mart.registration_date IS '고객 등록일';
COMMENT ON COLUMN marts.orders_customers_mart.dbt_dtm IS 'dbt 적재 일시';

