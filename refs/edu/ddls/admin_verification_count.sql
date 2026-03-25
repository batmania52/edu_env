CREATE SCHEMA IF NOT EXISTS admin;

CREATE TABLE IF NOT EXISTS admin.verification_count (
    model_name TEXT NOT NULL,
    uuid TEXT NOT NULL REFERENCES admin.verification_summary(uuid),
    source_count_result BIGINT,
    target_count_result BIGINT,
    source_count_sql TEXT,
    target_count_sql TEXT,
    verification_date TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE admin.verification_count IS '검증 레코드 수 정보';
COMMENT ON COLUMN admin.verification_count.model_name IS '모델 이름';
COMMENT ON COLUMN admin.verification_count.uuid IS '검증 실행 고유 ID';
COMMENT ON COLUMN admin.verification_count.source_count_result IS '소스 레코드 수 결과';
COMMENT ON COLUMN admin.verification_count.target_count_result IS '타겟 레코드 수 결과';
COMMENT ON COLUMN admin.verification_count.source_count_sql IS '소스 레코드 수 SQL 쿼리';
COMMENT ON COLUMN admin.verification_count.target_count_sql IS '타겟 레코드 수 SQL 쿼리';
COMMENT ON COLUMN admin.verification_count.verification_date IS '검증 실행 일시';