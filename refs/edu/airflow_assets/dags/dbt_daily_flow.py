from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import sys
import os
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
start_adj = {"months":0, "weeks":0, "days":-1, "hours":0, "minutes":0}
end_adj = {"months":0, "weeks":0, "days":0, "hours":0, "minutes":0}
now = datetime.now(local_tz)
end_time = now.replace(hour=23, minute=59, second=59, microsecond=0) + relativedelta(**end_adj)
start_time = now.replace(hour=0, minute=0, second=0, microsecond=0) + relativedelta(**start_adj)
# plugins 디렉토리를 python path에 추가
sys.path.append(os.path.join(os.environ['AIRFLOW_HOME'], 'plugins'))
from dbt_cosmos_utils import get_dbt_tag_task_group

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # 지침: 이전 실행 성공 여부에 의존하지 않음
    'start_date': datetime(2026, 1, 1, tzinfo=local_tz), # 오늘 00:00 KST 기준 (활성화 시 즉시 실행 방지)
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

params = {'data_interval_start': Param(start_time.strftime('%Y-%m-%d %H:%M:%S'), type="string", description="Data Interval Start"),
        'data_interval_end': Param(end_time.strftime('%Y-%m-%d %H:%M:%S'), type="string", description="Data Interval End"),
        'run_mode': Param("schedule", type="string", enum=["schedule", "manual"], description="Run Mode (manual/schedule)")}

with DAG(
    'dbt_daily_flow',
    default_args=default_args,
    description='A daily dbt workflow using Cosmos TaskGroups',
    schedule_interval='@daily',      # 매일 00:00 KST 실행
    # schedule_interval='0 0 * * *', # 크론탭 형식: 분 시 일 월 요일 (매일 00:00 실행)
    params=params,
    catchup=False,              # 지침: 과거 누락분 실행 방지
    max_active_runs=1,              # 지침: 동시에 하나의 실행만 허용
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
