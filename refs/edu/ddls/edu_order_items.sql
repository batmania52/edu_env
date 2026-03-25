CREATE TABLE IF NOT EXISTS edu.order_items (
    id INTEGER,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    price NUMERIC(10, 2)
);

COMMENT ON TABLE edu.order_items IS '원본 주문 항목 데이터';
COMMENT ON COLUMN edu.order_items.id IS '주문 항목 ID';
COMMENT ON COLUMN edu.order_items.order_id IS '주문 ID';
COMMENT ON COLUMN edu.order_items.product_id IS '상품 ID';
COMMENT ON COLUMN edu.order_items.quantity IS '수량';
COMMENT ON COLUMN edu.order_items.price IS '가격';