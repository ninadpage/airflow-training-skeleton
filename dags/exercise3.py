from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(5),
}


weekday_person_mapping = {
    'Monday': 'bob',
    'Tuesday': 'joe',
    'Wednesday': 'alice',
    'Thursday': 'joe',
    'Friday': 'alice',
    'Saturday': 'alice',
    'Sunday': 'alice',
}


def _print_execution_date(**context):
    print(f"This was executed on {context['execution_date'].strftime('%A')},"
          f" {context['execution_date']}")


def _get_person_to_email(**context):
    return f"email_{weekday_person_mapping[context['execution_date'].strftime('%A')]}"


with DAG(dag_id='exercise_3_branching', default_args=default_args,
         schedule_interval='@daily',
         dagrun_timeout=timedelta(minutes=60)) as dag:
    print_weekday = PythonOperator(
        task_id='print_weekday',
        python_callable=_print_execution_date,
        provide_context=True,
    )

    branching = BranchPythonOperator(
        task_id='branch_weekday',
        python_callable=_get_person_to_email,
        provide_context=True,
    )

    email_bob = DummyOperator(task_id='email_bob')
    email_alice = DummyOperator(task_id='email_alice')
    email_joe = DummyOperator(task_id='email_joe')
    final_task = BashOperator(
        task_id='final_task',
        bash_command='echo "DAG invoked at {{ execution_date }}, finished at $(date)"',
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    print_weekday >> branching >> [email_bob, email_alice, email_joe] >> final_task
