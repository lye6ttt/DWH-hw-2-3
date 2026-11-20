from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable


def calculate_scd2(**context):
    from build_user_scd2 import run
    run(context)


def calculate_recipients(**context):
    from build_campaign_recipients import run
    run(context)


with DAG(
    dag_id="hw_2_3_scd2_campaigns",
    description="SCD2 и исторический JOIN за выбранные сутки",
    start_date=pendulum.datetime(2026, 1, 23, tz="UTC"),
    end_date=pendulum.datetime(2026, 1, 25, tz="UTC"),
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={"owner": "student", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["dwh", "hw-2.3"],
) as dag:
    build_user_scd2 = PythonOperator(
        task_id="build_user_scd2", python_callable=calculate_scd2
    )
    build_campaign_recipients = PythonOperator(
        task_id="build_campaign_recipients", python_callable=calculate_recipients
    )

    build_user_scd2 >> build_campaign_recipients
