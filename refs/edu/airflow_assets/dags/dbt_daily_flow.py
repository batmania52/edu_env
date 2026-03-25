from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os

# plugins 디렉토리를 python path에 추가
sys.path.append(os.path.join(os.environ['AIRFLOW_HOME'], 'plugins'))
from dbt_cosmos_utils import get_dbt_tag_task_group

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # 지침: 이전 실행 성공 여부에 의존하지 않음
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dbt_daily_flow',
    default_args=default_args,
    description='A daily dbt workflow using Cosmos TaskGroups',
    schedule_interval=None,
    catchup=False,              # 지침: 과거 누락분 실행 방지
    is_paused_upon_creation=True, # 지침: 활성화 시 자동 실행 방지
    tags=['dbt', 'edu001'],
) as dag:

    start = EmptyOperator(task_id='start')

    tg_staging = get_dbt_tag_task_group(
        dag=dag,
        group_id='staging_layer',
        tag='stg',
        schema='stg'
    )

    tg_marts = get_dbt_tag_task_group(
        dag=dag,
        group_id='marts_layer',
        tag='marts',
        schema='marts'
    )

    end = EmptyOperator(task_id='end')

    start >> tg_staging >> tg_marts >> end
