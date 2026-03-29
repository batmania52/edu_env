CREATE TABLE stg.stg_customers (
    customer_id integer PRIMARY KEY,
    customer_name varchar(255),
    customer_email varchar(255),
    registration_date date,
    dbt_dtm timestamp without time zone
);

COMMENT ON TABLE stg.stg_customers IS '고객 데이터의 스테이징 모델';
COMMENT ON COLUMN stg.stg_customers.customer_id IS '고객의 기본 키';
COMMENT ON COLUMN stg.stg_customers.customer_name IS '고객 이름';
COMMENT ON COLUMN stg.stg_customers.customer_email IS '고객 이메일';
COMMENT ON COLUMN stg.stg_customers.registration_date IS '고객 등록일';
COMMENT ON COLUMN stg.stg_customers.dbt_dtm IS 'dbt 적재 일시';

