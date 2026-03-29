CREATE TABLE marts.orders_summary_mart (
    order_id integer,
    customer_id integer,
    customer_name character varying(255),
    customer_email character varying(255),
    registration_date date,
    order_date timestamp without time zone,
    total_amount numeric(10,2),
    product_id integer,
    quantity integer,
    price numeric(10,2),
    item_total numeric(10,2),
    dbt_dtm timestamp without time zone
);

COMMENT ON TABLE marts.orders_summary_mart IS '주문-고객-상품 통합 요약 마트 (mart 간 리니지 테스트용)';
COMMENT ON COLUMN marts.orders_summary_mart.order_id IS '주문 ID (복합 PK)';
COMMENT ON COLUMN marts.orders_summary_mart.customer_id IS '고객 ID';
COMMENT ON COLUMN marts.orders_summary_mart.customer_name IS '고객 이름';
COMMENT ON COLUMN marts.orders_summary_mart.customer_email IS '고객 이메일';
COMMENT ON COLUMN marts.orders_summary_mart.registration_date IS '고객 등록일';
COMMENT ON COLUMN marts.orders_summary_mart.order_date IS '주문 날짜';
COMMENT ON COLUMN marts.orders_summary_mart.total_amount IS '주문 총액';
COMMENT ON COLUMN marts.orders_summary_mart.product_id IS '상품 ID (복합 PK)';
COMMENT ON COLUMN marts.orders_summary_mart.quantity IS '상품 수량';
COMMENT ON COLUMN marts.orders_summary_mart.price IS '상품 단가';
COMMENT ON COLUMN marts.orders_summary_mart.item_total IS '상품별 총액';
COMMENT ON COLUMN marts.orders_summary_mart.dbt_dtm IS 'dbt 적재 일시';
