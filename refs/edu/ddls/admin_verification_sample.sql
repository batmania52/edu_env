CREATE SCHEMA IF NOT EXISTS admin;

CREATE TABLE IF NOT EXISTS admin.verification_sample (
    model_name TEXT NOT NULL,
    uuid TEXT NOT NULL REFERENCES admin.verification_summary(uuid),
    source_sample_result JSONB,
    target_sample_result JSONB,
    source_sample_sql TEXT,
    target_sample_sql TEXT,
    verification_date TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE admin.verification_sample IS '검증 샘플 데이터 정보';
COMMENT ON COLUMN admin.verification_sample.model_name IS '모델 이름';
COMMENT ON COLUMN admin.verification_sample.uuid IS '검증 실행 고유 ID';
COMMENT ON COLUMN admin.verification_sample.source_sample_result IS '소스 샘플 데이터 결과 (JSONB)';
COMMENT ON COLUMN admin.verification_sample.target_sample_result IS '타겟 샘플 데이터 결과 (JSONB)';
COMMENT ON COLUMN admin.verification_sample.source_sample_sql IS '소스 샘플 데이터 SQL 쿼리';
COMMENT ON COLUMN admin.verification_sample.target_sample_sql IS '타겟 샘플 데이터 SQL 쿼리';
COMMENT ON COLUMN admin.verification_sample.verification_date IS '검증 실행 일시';