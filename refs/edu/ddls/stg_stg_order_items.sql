CREATE TABLE stg.stg_order_items (
    order_item_id integer,
    order_id integer,
    product_id integer,
    quantity integer,
    price numeric(10,2)
);

COMMENT ON TABLE stg.stg_order_items IS '주문 항목 데이터의 스테이징 모델';
COMMENT ON COLUMN stg.stg_order_items.order_item_id IS '주문 항목의 기본 키';
COMMENT ON COLUMN stg.stg_order_items.order_id IS '주문에 대한 외래 키';
COMMENT ON COLUMN stg.stg_order_items.product_id IS '상품 ID';
COMMENT ON COLUMN stg.stg_order_items.quantity IS '상품 수량';
COMMENT ON COLUMN stg.stg_order_items.price IS '상품 가격';
