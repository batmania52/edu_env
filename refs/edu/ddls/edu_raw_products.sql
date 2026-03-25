CREATE TABLE IF NOT EXISTS edu.raw_products (
    product_id INTEGER,
    product_name varchar(255),
    product_category varchar(255),
    price NUMERIC(10, 2),
    created_date DATE
);

COMMENT ON TABLE edu.raw_products IS '원본 상품 데이터';
COMMENT ON COLUMN edu.raw_products.product_id IS '상품 ID';
COMMENT ON COLUMN edu.raw_products.product_name IS '상품 이름';
COMMENT ON COLUMN edu.raw_products.product_category IS '상품 카테고리';
COMMENT ON COLUMN edu.raw_products.price IS '가격';
COMMENT ON COLUMN edu.raw_products.created_date IS '생성일';