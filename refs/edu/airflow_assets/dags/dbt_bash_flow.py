from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os

# plugins 디렉토리를 python path에 추가
sys.path.append(os.path.join(os.environ['AIRFLOW_HOME'], 'plugins'))
from dbt_bash_utils import get_dbt_bash_operator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # 지침: 이전 실행 성공 여부에 의존하지 않음
    'start_date': datetime(2024, 3, 1),
    'retries': 0,
}

with DAG(
    'dbt_bash_flow',
    default_args=default_args,
    description='A dbt workflow using BashOperator utilities',
    schedule_interval='@daily',
    catchup=False,              # 지침: 과거 누락분 실행 방지
    is_paused_upon_creation=True, # 지침: 활성화 시 자동 실행 방지
    tags=['dbt', 'bash', 'edu001'],
) as dag:

    start = EmptyOperator(task_id='start')

    # 1. dbt debug: 연결 및 환경 점검
    dbt_debug = get_dbt_bash_operator(
        task_id='dbt_debug',
        command='debug'
    )

    # 2. dbt run staging: 스테이징 모델 실행
    dbt_run_staging = get_dbt_bash_operator(
        task_id='dbt_run_staging',
        command='run',
        select='staging'
    )

    # 3. dbt run marts: 마트 모델 실행
    dbt_run_marts = get_dbt_bash_operator(
        task_id='dbt_run_marts',
        command='run',
        select='marts'
    )

    end = EmptyOperator(task_id='end')

    # 워크플로우 정의
    start >> dbt_debug >> dbt_run_staging >> dbt_run_marts >> end
