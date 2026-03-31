CREATE TABLE IF NOT EXISTS marts.orders_churn_mart (
    order_id               integer       NOT NULL,
    customer_id            integer,
    customer_name          character varying(255),
    order_date             timestamp without time zone,
    total_amount           numeric(10,2),
    churn_risk_score       numeric(10,2),
    churn_risk_segment     character varying(255),
    days_since_last_order  integer,
    total_orders           bigint,
    avg_order_value        numeric(10,2),
    dbt_dtm                timestamp without time zone,
    PRIMARY KEY (order_id)
);

COMMENT ON TABLE marts.orders_churn_mart IS '주문별 고객 이탈 위험도 마트';
COMMENT ON COLUMN marts.orders_churn_mart.order_id IS '주문 ID (기본 키)';
COMMENT ON COLUMN marts.orders_churn_mart.customer_id IS '고객 ID';
COMMENT ON COLUMN marts.orders_churn_mart.customer_name IS '고객 이름';
COMMENT ON COLUMN marts.orders_churn_mart.order_date IS '주문 날짜';
COMMENT ON COLUMN marts.orders_churn_mart.total_amount IS '주문 총액';
COMMENT ON COLUMN marts.orders_churn_mart.churn_risk_score IS '이탈 위험 점수';
COMMENT ON COLUMN marts.orders_churn_mart.churn_risk_segment IS '이탈 위험 세그먼트 (Low/Medium/High)';
COMMENT ON COLUMN marts.orders_churn_mart.days_since_last_order IS '마지막 주문 이후 경과일';
COMMENT ON COLUMN marts.orders_churn_mart.total_orders IS '총 주문 건수';
COMMENT ON COLUMN marts.orders_churn_mart.avg_order_value IS '평균 주문 금액';
COMMENT ON COLUMN marts.orders_churn_mart.dbt_dtm IS 'dbt 적재 일시';
