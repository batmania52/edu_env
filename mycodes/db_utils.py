# Design Ref: §3.1 — DB 연결풀, 커넥션 관리, 스키마/테이블 조회
# 의존성 없음 (최하위 모듈)
import os
import yaml
import psycopg2
import psycopg2.pool
from psycopg2 import sql as pgsql
import streamlit as st
from contextlib import contextmanager


@st.cache_resource
def _make_pool(host, port, dbname, user, password):
    """앱 전체에서 공유하는 DB 커넥션 풀 (최소 1, 최대 5)"""
    return psycopg2.pool.SimpleConnectionPool(
        1, 5,
        host=host, port=port, dbname=dbname, user=user, password=password
    )

@contextmanager
def get_conn(db_config):
    """풀에서 커넥션을 대여 후 자동 반납하는 컨텍스트 매니저"""
    p = _make_pool(
        db_config['host'], db_config['port'],
        db_config['dbname'], db_config['user'], db_config['password']
    )
    conn = p.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()  # Design Ref: §3.1 — dirty connection 방지
        raise
    finally:
        p.putconn(conn)

def get_db_config(profile_dir, target_name):
    """profiles.yml에서 DB 접속 정보 추출"""
    profile_path = os.path.join(profile_dir, 'profiles.yml')
    if not os.path.exists(profile_path):
        return None
    try:
        with open(profile_path, 'r', encoding='utf-8') as f:
            profiles = yaml.safe_load(f)
            profile_key = list(profiles.keys())[0]
            config = profiles[profile_key]['outputs'][target_name]
            return {
                "host": config.get('host'),
                "user": config.get('user'),
                "password": config.get('password'),
                "dbname": config.get('dbname', config.get('database')),
                "port": config.get('port', 5432)
            }
    except Exception:
        return None

def check_history_tables_exist(db_config):
    """admin.verification_summary, admin.dbt_log 두 테이블 존재 여부 반환 (bool, bool)"""
    try:
        with get_conn(db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'admin'
                      AND table_name IN ('verification_summary', 'dbt_log')
                """)
                found = {row[0] for row in cur.fetchall()}
        return 'verification_summary' in found, 'dbt_log' in found
    except Exception:
        return False, False

def get_schemas(db_config):
    """시스템 스키마를 제외한 사용자 스키마 목록"""
    with get_conn(db_config) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
              AND schema_name NOT LIKE 'pg_temp_%'
              AND schema_name NOT LIKE 'pg_toast_temp_%'
            ORDER BY schema_name
        """)
        return [row[0] for row in cur.fetchall()]

def get_db_tables(db_config, schema_name):
    """특정 스키마의 BASE TABLE 목록"""
    with get_conn(db_config) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s AND table_type = 'BASE TABLE' ORDER BY table_name",
            (schema_name,)
        )
        return [row[0] for row in cur.fetchall()]

def get_table_detail(db_config, schema_name, table_name):
    """컬럼 정보 및 PK 조회"""
    # Design Ref: §3.1 FR-02 — psycopg2.sql.Literal로 식별자 안전 처리
    qualified = f"{schema_name}.{table_name}"
    with get_conn(db_config) as conn:
        cur = conn.cursor()
        cur.execute(
            pgsql.SQL("""
            SELECT a.attname,
                   format_type(a.atttypid, a.atttypmod) as data_type,
                   pg_catalog.col_description(a.attrelid, a.attnum) as description,
                   NOT a.attnotnull as is_nullable
            FROM pg_catalog.pg_attribute a
            WHERE a.attrelid = {}::regclass
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            """).format(pgsql.Literal(qualified))
        )
        columns = [
            {
                "name": r[0], 
                "data_type": r[1], 
                "description": r[2] or "",
                "is_nullable": r[3]
            } for r in cur.fetchall()
        ]
        cur.execute(
            pgsql.SQL("SELECT obj_description({}::regclass, 'pg_class')")
            .format(pgsql.Literal(qualified))
        )
        row = cur.fetchone()
        table_comment = row[0] if row and row[0] else f"Model for {table_name}"
        cur.execute(
            "SELECT kcu.column_name "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name = kcu.constraint_name "
            "WHERE tc.constraint_type = 'PRIMARY KEY' "
            "  AND tc.table_name = %s AND tc.table_schema = %s",
            (table_name, schema_name)
        )
        pk = [row[0] for row in cur.fetchall()]
        return columns, pk, table_comment
