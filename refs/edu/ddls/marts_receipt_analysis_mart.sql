CREATE TABLE marts.receipt_analysis_mart (
    receipt_id integer,
    order_id integer,
    customer_id integer,
    customer_name character varying(255),
    customer_email character varying(255),
    order_date timestamp without time zone,
    total_order_amount numeric(10,2),
    product_id integer,
    product_name character varying(255),
    product_category character varying(255),
    item_quantity integer,
    item_price numeric(10,2),
    item_total numeric(10,2),
    dbt_dtm timestamp without time zone
);

COMMENT ON TABLE marts.receipt_analysis_mart IS '영수증 데이터 기반 상세 판매 분석 마트';
COMMENT ON COLUMN marts.receipt_analysis_mart.receipt_id IS '영수증 고유 ID';
COMMENT ON COLUMN marts.receipt_analysis_mart.order_id IS '주문 ID';
COMMENT ON COLUMN marts.receipt_analysis_mart.customer_id IS '고객 ID';
COMMENT ON COLUMN marts.receipt_analysis_mart.customer_name IS '고객 이름';
COMMENT ON COLUMN marts.receipt_analysis_mart.customer_email IS '고객 이메일';
COMMENT ON COLUMN marts.receipt_analysis_mart.order_date IS '주문 일시';
COMMENT ON COLUMN marts.receipt_analysis_mart.total_order_amount IS '주문 총액';
COMMENT ON COLUMN marts.receipt_analysis_mart.product_id IS '상품 ID';
COMMENT ON COLUMN marts.receipt_analysis_mart.product_name IS '상품 이름';
COMMENT ON COLUMN marts.receipt_analysis_mart.product_category IS '상품 카테고리';
COMMENT ON COLUMN marts.receipt_analysis_mart.item_quantity IS '상품 수량';
COMMENT ON COLUMN marts.receipt_analysis_mart.item_price IS '상품 단가';
COMMENT ON COLUMN marts.receipt_analysis_mart.item_total IS '상품별 총액';
COMMENT ON COLUMN marts.receipt_analysis_mart.dbt_dtm IS 'dbt 적재 일시';
