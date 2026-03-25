CREATE SCHEMA IF NOT EXISTS admin;

CREATE TABLE IF NOT EXISTS admin.verification_summary (
    model_name TEXT NOT NULL,
    uuid TEXT PRIMARY KEY,
    query_condition_start TIMESTAMP,
    query_condition_end TIMESTAMP,
    count_status TEXT,
    sum_status TEXT,
    sample_status TEXT,
    compiled_sql TEXT,
    verification_date TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE admin.verification_summary IS '검증 요약 정보';
COMMENT ON COLUMN admin.verification_summary.model_name IS '모델 이름';
COMMENT ON COLUMN admin.verification_summary.uuid IS '검증 실행 고유 ID';
COMMENT ON COLUMN admin.verification_summary.query_condition_start IS '쿼리 조건 시작 시간';
COMMENT ON COLUMN admin.verification_summary.query_condition_end IS '쿼리 조건 종료 시간';
COMMENT ON COLUMN admin.verification_summary.count_status IS '레코드 수 일치 여부 상태';
COMMENT ON COLUMN admin.verification_summary.sum_status IS '합계 일치 여부 상태';
COMMENT ON COLUMN admin.verification_summary.sample_status IS '샘플 데이터 일치 여부 상태';
COMMENT ON COLUMN admin.verification_summary.compiled_sql IS '컴파일된 SQL 쿼리';
COMMENT ON COLUMN admin.verification_summary.verification_date IS '검증 실행 일시';