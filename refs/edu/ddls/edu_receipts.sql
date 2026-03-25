CREATE TABLE IF NOT EXISTS edu.receipts (
    receipt_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    customer_name VARCHAR(255),
    customer_email VARCHAR(255),
    order_date TIMESTAMP WITHOUT TIME ZONE,
    total_order_amount NUMERIC(10, 2),
    product_id INTEGER,
    product_name VARCHAR(255),
    product_category VARCHAR(255),
    item_quantity INTEGER,
    item_price NUMERIC(10, 2),
    item_total NUMERIC(10, 2)
);

COMMENT ON TABLE edu.receipts IS '통합 영수증 정보';
COMMENT ON COLUMN edu.receipts.receipt_id IS '영수증 항목 고유 ID';
COMMENT ON COLUMN edu.receipts.order_id IS '주문 ID';
COMMENT ON COLUMN edu.receipts.customer_id IS '고객 ID';
COMMENT ON COLUMN edu.receipts.customer_name IS '고객 이름';
COMMENT ON COLUMN edu.receipts.customer_email IS '고객 이메일';
COMMENT ON COLUMN edu.receipts.order_date IS '주문 일시';
COMMENT ON COLUMN edu.receipts.total_order_amount IS '총 주문 금액';
COMMENT ON COLUMN edu.receipts.product_id IS '상품 ID';
COMMENT ON COLUMN edu.receipts.product_name IS '상품 이름';
COMMENT ON COLUMN edu.receipts.product_category IS '상품 카테고리';
COMMENT ON COLUMN edu.receipts.item_quantity IS '상품 수량';
COMMENT ON COLUMN edu.receipts.item_price IS '개별 상품 가격';
COMMENT ON COLUMN edu.receipts.item_total IS '개별 상품 총액';