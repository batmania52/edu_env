from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
import sys
import os
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")

sys.path.append(os.path.join(os.environ['AIRFLOW_HOME'], 'plugins'))
from dbt_cosmos_utils import get_dbt_tag_task_group

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1, tzinfo=local_tz),
}

params = {
    'data_interval_start': Param(datetime.now(local_tz).strftime('%Y-%m-%d 00:00:00'), type="string"),
    'data_interval_end': Param(datetime.now(local_tz).strftime('%Y-%m-%d 23:59:59'), type="string"),
    'run_mode': Param("manual", type="string", enum=["schedule", "manual"])
}

with DAG(
    'dbt_stg_flow',
    default_args=default_args,
    schedule_interval=None,
    params=params,
    catchup=False,
    max_active_runs=1,
    tags=['dbt', 'staging'],
) as dag:
    tg_staging = get_dbt_tag_task_group(
        dag=dag,
        group_id='staging_layer',
        tag='stg',
        schema='stg'
    )
