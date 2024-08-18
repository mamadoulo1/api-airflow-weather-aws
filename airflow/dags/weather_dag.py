from airflow import DAG
from datetime import datetime, timedelta, timezone
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

import json
import pandas as pd
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))
from etl.etl import transform_load_data

defaults_args = {
     'owner': 'Mamadou',
     'depends_on_past': False,
     'start_date': datetime(2024, 1, 8),
     'email': ['lomamadou@lomamadou.com'],
     'email_on_failure': False,
     'email_on_retry': False,
     'retries': 2,
     'retry_delay': timedelta(minutes=2)

}

with DAG(
        'weather_dag',
        default_args=defaults_args,
        schedule_interval='@daily',
        catchup=False) as dag:



        is_weather_api_ready = HttpSensor(
            task_id='is_weather_api_ready',
            http_conn_id='weathermap_api',
            endpoint='/data/2.5/weather?q=Portland&APPID=3ae9d347bfcf0beeab32fdecf67acd60'
    )
        extract_weather_data = SimpleHttpOperator(
            task_id="extract_weather_data",
            http_conn_id="weathermap_api",
            endpoint="/data/2.5/weather?q=Portland&APPID=3ae9d347bfcf0beeab32fdecf67acd60",
            method="GET",
            response_filter=lambda r: json.loads(r.text),
            log_response=True
    )
        transform_load_weather_data = PythonOperator(
            task_id='transform_load_weather_data',
            python_callable=transform_load_data
                
    )

        is_weather_api_ready >>  extract_weather_data >> transform_load_weather_data