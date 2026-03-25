CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS marts.purchase_order_summary (
    customer_id INTEGER,
    customer_name VARCHAR(255),
    total_purchase_orders BIGINT,
    total_purchase_amount NUMERIC(10,2),
    pending_orders BIGINT,
    completed_orders BIGINT,
    canceled_orders BIGINT,
    first_purchase_order_date TIMESTAMP WITHOUT TIME ZONE,
    last_purchase_order_date TIMESTAMP WITHOUT TIME ZONE
);

COMMENT ON TABLE marts.purchase_order_summary IS '고객별 발주 요약 정보';
COMMENT ON COLUMN marts.purchase_order_summary.customer_id IS '고객 고유 ID';
COMMENT ON COLUMN marts.purchase_order_summary.customer_name IS '고객 이름';
COMMENT ON COLUMN marts.purchase_order_summary.total_purchase_orders IS '총 발주 횟수';
COMMENT ON COLUMN marts.purchase_order_summary.total_purchase_amount IS '총 발주 금액';
COMMENT ON COLUMN marts.purchase_order_summary.pending_orders IS '대기 중인 발주 횟수';
COMMENT ON COLUMN marts.purchase_order_summary.completed_orders IS '완료된 발주 횟수';
COMMENT ON COLUMN marts.purchase_order_summary.canceled_orders IS '취소된 발주 횟수';
COMMENT ON COLUMN marts.purchase_order_summary.first_purchase_order_date IS '첫 발주 날짜';
COMMENT ON COLUMN marts.purchase_order_summary.last_purchase_order_date IS '마지막 발주 날짜';
