from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param
from datetime import datetime
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1, tzinfo=local_tz), # 오늘 00:00 KST 기준 (활성화 시 즉시 실행 방지)
}

params = {
    'data_interval_start': Param(datetime.now(local_tz).strftime('%Y-%m-%d 00:00:00'), type="string"),
    'data_interval_end': Param(datetime.now(local_tz).strftime('%Y-%m-%d 23:59:59'), type="string"),
    'run_mode': Param("schedule", type="string", enum=["schedule", "manual"])
}

with DAG(
    'dbt_main_orchestrator',
    default_args=default_args,
    schedule_interval='@daily',
    params=params,
    catchup=False,
    max_active_runs=1,
    tags=['dbt', 'main'],
) as dag:
    
    trigger_stg = TriggerDagRunOperator(
        task_id='trigger_stg',
        trigger_dag_id='dbt_stg_flow',
        conf={
            "data_interval_start": "{{ params.data_interval_start }}",
            "data_interval_end": "{{ params.data_interval_end }}",
            "run_mode": "{{ params.run_mode }}"
        },
        wait_for_completion=True,
        poke_interval=5  # 5초마다 하위 DAG 상태 확인
    )

    trigger_marts = TriggerDagRunOperator(
        task_id='trigger_marts',
        trigger_dag_id='dbt_marts_flow',
        conf={
            "data_interval_start": "{{ params.data_interval_start }}",
            "data_interval_end": "{{ params.data_interval_end }}",
            "run_mode": "{{ params.run_mode }}"
        },
        wait_for_completion=True,
        poke_interval=5  # 5초마다 하위 DAG 상태 확인
    )

    trigger_stg >> trigger_marts
