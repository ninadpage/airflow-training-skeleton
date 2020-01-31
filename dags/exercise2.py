from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(5),
}


def print_execution_date(**context):
    print(context['execution_date'])


with DAG(dag_id='exercise_2_templating', default_args=default_args,
         schedule_interval='@daily',
         dagrun_timeout=timedelta(minutes=60)) as dag:
    _print_execution_date = PythonOperator(
        task_id='print_execution_date',
        python_callable=print_execution_date,
        provide_context=True,
    )
    wait_1 = BashOperator(task_id='wait_1', bash_command='sleep 1')
    wait_5 = BashOperator(task_id='wait_5', bash_command='sleep 5')
    wait_10 = BashOperator(task_id='wait_10', bash_command='sleep 10')
    the_end = DummyOperator(task_id='the_end')

    _print_execution_date >> [wait_1, wait_5, wait_10] >> the_end
