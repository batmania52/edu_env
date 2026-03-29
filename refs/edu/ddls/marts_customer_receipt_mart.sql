CREATE TABLE IF NOT EXISTS marts.customer_receipt_mart (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(255),
    customer_email VARCHAR(255),
    first_order_date DATE,
    last_order_date DATE,
    total_receipt_count BIGINT,
    total_items_purchased BIGINT,
    total_spend_amount NUMERIC(18, 2),
    average_item_price NUMERIC(10, 2),
    dbt_dtm timestamp without time zone
);

COMMENT ON TABLE marts.customer_receipt_mart IS '고객별 영수증 요약 마트 테이블';
COMMENT ON COLUMN marts.customer_receipt_mart.customer_id IS '고객 ID (기본 키)';
COMMENT ON COLUMN marts.customer_receipt_mart.customer_name IS '고객 이름';
COMMENT ON COLUMN marts.customer_receipt_mart.customer_email IS '고객 이메일';
COMMENT ON COLUMN marts.customer_receipt_mart.first_order_date IS '고객의 첫 주문 날짜';
COMMENT ON COLUMN marts.customer_receipt_mart.last_order_date IS '고객의 마지막 주문 날짜';
COMMENT ON COLUMN marts.customer_receipt_mart.total_receipt_count IS '고객의 총 주문 건수';
COMMENT ON COLUMN marts.customer_receipt_mart.total_items_purchased IS '고객이 구매한 총 상품 수량';
COMMENT ON COLUMN marts.customer_receipt_mart.total_spend_amount IS '고객의 총 지출 금액';
COMMENT ON COLUMN marts.customer_receipt_mart.average_item_price IS '고객이 구매한 상품의 평균 가격';
COMMENT ON COLUMN marts.customer_receipt_mart.dbt_dtm IS 'dbt 적재 일시';
