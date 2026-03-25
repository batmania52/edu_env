CREATE TABLE edu.order (
    order_id integer,
    customer_id integer,
    order_date timestamp without time zone,
    total_amount numeric(10,2)
);

COMMENT ON TABLE edu.order IS '원본 주문 데이터';
COMMENT ON COLUMN edu.order.order_id IS '주문의 기본 키';
COMMENT ON COLUMN edu.order.customer_id IS '고객의 외래 키';
COMMENT ON COLUMN edu.order.order_date IS '주문이 발생한 날짜와 시간';
COMMENT ON COLUMN edu.order.total_amount IS '주문의 총 금액';
