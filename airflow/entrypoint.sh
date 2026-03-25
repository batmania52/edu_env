#!/bin/bash
set -e

# DB 마이그레이션
airflow db migrate

# 관리자 계정 생성 (이미 있으면 스킵)
airflow users create \
    --username "${_AIRFLOW_WWW_USER_USERNAME:-airflow}" \
    --password "${_AIRFLOW_WWW_USER_PASSWORD:-airflow}" \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email admin@example.com 2>/dev/null || true

# 원래 커맨드 실행
exec airflow "$@"
