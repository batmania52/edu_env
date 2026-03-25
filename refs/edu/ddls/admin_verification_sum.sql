CREATE SCHEMA IF NOT EXISTS admin;

CREATE TABLE IF NOT EXISTS admin.verification_sum (
    model_name TEXT NOT NULL,
    uuid TEXT NOT NULL REFERENCES admin.verification_summary(uuid),
    source_sum_result JSONB,
    target_sum_result JSONB,
    source_sum_sql TEXT,
    target_sum_sql TEXT,
    verification_date TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE admin.verification_sum IS '검증 합계 정보';
COMMENT ON COLUMN admin.verification_sum.model_name IS '모델 이름';
COMMENT ON COLUMN admin.verification_sum.uuid IS '검증 실행 고유 ID';
COMMENT ON COLUMN admin.verification_sum.source_sum_result IS '소스 합계 결과 (JSONB)';
COMMENT ON COLUMN admin.verification_sum.target_sum_result IS '타겟 합계 결과 (JSONB)';
COMMENT ON COLUMN admin.verification_sum.source_sum_sql IS '소스 합계 SQL 쿼리';
COMMENT ON COLUMN admin.verification_sum.target_sum_sql IS '타겟 합계 SQL 쿼리';
COMMENT ON COLUMN admin.verification_sum.verification_date IS '검증 실행 일시';