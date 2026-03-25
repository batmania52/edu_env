CREATE TABLE IF NOT EXISTS edu.purchase_orders (
    purchase_order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date TIMESTAMP WITHOUT TIME ZONE,
    total_amount NUMERIC(10, 2),
    status VARCHAR(50) DEFAULT 'pending'
);

COMMENT ON TABLE edu.purchase_orders IS '발주 데이터';
COMMENT ON COLUMN edu.purchase_orders.purchase_order_id IS '발주 고유 ID';
COMMENT ON COLUMN edu.purchase_orders.customer_id IS '발주 고객 ID';
COMMENT ON COLUMN edu.purchase_orders.order_date IS '발주 날짜 및 시간';
COMMENT ON COLUMN edu.purchase_orders.total_amount IS '발주 총 금액';
COMMENT ON COLUMN edu.purchase_orders.status IS '발주 상태 (예: pending, completed, canceled)';