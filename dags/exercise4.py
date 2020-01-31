from datetime import timedelta

import airflow
from airflow.contrib.operators.postgres_to_gcs_operator import \
    PostgresToGoogleCloudStorageOperator
from airflow.models import DAG


default_args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(5),
}


with DAG(dag_id='exercise_4_external_jobs', default_args=default_args,
         schedule_interval='@daily',
         dagrun_timeout=timedelta(minutes=60)) as dag:
    psql_to_gcs = PostgresToGoogleCloudStorageOperator(
        task_id='postgres_to_gcs',
        sql='select transaction, price, postcode from land_registry_price_paid_uk LIMIT 50;',
        bucket='gdd_airflow_1_npage',
        filename='land_registry_price_paid_uk-{{ ds }}',
        postgres_conn_id='postgres_gdd',
        google_cloud_storage_conn_id='google_cloud_storage_default',
    )
