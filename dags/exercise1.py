from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

with DAG(dag_id='exercise_1', default_args=default_args, schedule_interval=None,
         dagrun_timeout=timedelta(minutes=60)) as dag:
    t1 = DummyOperator(task_id='task1')
    t2 = DummyOperator(task_id='task2')
    t3 = DummyOperator(task_id='task3')
    t4 = DummyOperator(task_id='task4')
    t5 = DummyOperator(task_id='task5')

    t1 >> t2 >> [t3, t4] >> t5
