CREATE TABLE marts.customer_churn_risk_mart (
    customer_id integer,
    customer_name varchar(255),
    customer_email varchar(255),
    registration_date date,
    last_order_date timestamp without time zone,
    days_since_last_order integer,
    total_orders bigint,
    avg_order_value numeric(10,2),
    churn_risk_score numeric(10,2),
    churn_risk_segment varchar(255),
    analysis_date date,
    dbt_dtm timestamp without time zone,
    PRIMARY KEY (customer_id, analysis_date)
);

COMMENT ON TABLE marts.customer_churn_risk_mart IS '고객 이탈 위험 분석 마트 모델';
COMMENT ON COLUMN marts.customer_churn_risk_mart.customer_id IS '고객의 기본 키';
COMMENT ON COLUMN marts.customer_churn_risk_mart.customer_name IS '고객 이름';
COMMENT ON COLUMN marts.customer_churn_risk_mart.customer_email IS '고객 이메일';
COMMENT ON COLUMN marts.customer_churn_risk_mart.registration_date IS '고객 등록일';
COMMENT ON COLUMN marts.customer_churn_risk_mart.last_order_date IS '고객의 마지막 주문 날짜';
COMMENT ON COLUMN marts.customer_churn_risk_mart.days_since_last_order IS '고객의 마지막 주문 이후 경과일';
COMMENT ON COLUMN marts.customer_churn_risk_mart.total_orders IS '고객의 총 주문 건수';
COMMENT ON COLUMN marts.customer_churn_risk_mart.avg_order_value IS '고객 주문의 평균 가치';
COMMENT ON COLUMN marts.customer_churn_risk_mart.churn_risk_score IS '계산된 이탈 위험 점수';
COMMENT ON COLUMN marts.customer_churn_risk_mart.churn_risk_segment IS '이탈 위험 세그먼트 (낮음, 중간, 높음)';
COMMENT ON COLUMN marts.customer_churn_risk_mart.analysis_date IS '이탈 위험 분석 수행 날짜';
COMMENT ON COLUMN marts.customer_churn_risk_mart.dbt_dtm IS 'dbt 적재 일시';

