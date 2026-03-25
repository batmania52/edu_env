CREATE TABLE IF NOT EXISTS edu.raw_customers (
    customer_id INTEGER,
    customer_name varchar(255),
    customer_email varchar(255),
    registration_date DATE
);

COMMENT ON TABLE edu.raw_customers IS '원본 고객 데이터';
COMMENT ON COLUMN edu.raw_customers.customer_id IS '고객 ID';
COMMENT ON COLUMN edu.raw_customers.customer_name IS '고객 이름';
COMMENT ON COLUMN edu.raw_customers.customer_email IS '고객 이메일';
COMMENT ON COLUMN edu.raw_customers.registration_date IS '고객 등록일';