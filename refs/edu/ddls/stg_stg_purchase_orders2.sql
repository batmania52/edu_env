CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.stg_purchase_orders2 (
    purchase_order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date TIMESTAMP WITHOUT TIME ZONE,
    total_amount NUMERIC(10, 2),
    status VARCHAR(50),
    dbt_dtm TIMESTAMP WITHOUT TIME ZONE
);

COMMENT ON TABLE stg.stg_purchase_orders2 IS '발주 데이터 (before_sql 다중 구문 테스트용)';
COMMENT ON COLUMN stg.stg_purchase_orders2.purchase_order_id IS '발주 고유 ID';
COMMENT ON COLUMN stg.stg_purchase_orders2.customer_id IS '발주 고객 ID';
COMMENT ON COLUMN stg.stg_purchase_orders2.order_date IS '발주 날짜 및 시간';
COMMENT ON COLUMN stg.stg_purchase_orders2.total_amount IS '발주 총 금액';
COMMENT ON COLUMN stg.stg_purchase_orders2.status IS '발주 상태 (예: pending, completed, canceled)';
COMMENT ON COLUMN stg.stg_purchase_orders2.dbt_dtm IS 'dbt 적재 일시';
