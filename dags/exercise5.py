from datetime import timedelta, datetime

import airflow
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, \
    DataProcPySparkOperator, DataprocClusterDeleteOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.postgres_to_gcs_operator import \
    PostgresToGoogleCloudStorageOperator
from airflow.models import DAG, Variable
from airflow.utils.trigger_rule import TriggerRule

from operators.http_to_gcs_operator import HttpToGcsOperator


default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2019, 1, 1),
    'end_date': datetime(2019, 11, 28),
    'project_id': Variable.get('gcp_project'),
}


with DAG(dag_id='exercise_5_real_estate', default_args=default_args,
         schedule_interval='@daily',
         dagrun_timeout=timedelta(minutes=60)) as dag:
    # Fetch land registry prices for current day
    land_registry_prices_to_gcs = PostgresToGoogleCloudStorageOperator(
        task_id='land_registry_prices_to_gcs',
        sql="select * from land_registry_price_paid_uk "
            "WHERE transfer_date = '{{ ds }}';",
        bucket='gdd_airflow_npage_properties',
        filename='land_registry_price_paid_uk-{{ ds }}.json',
        postgres_conn_id='postgres_gdd',
        google_cloud_storage_conn_id='google_cloud_storage_default',
    )

    # Fetch exchange rate (average) from previous day until current day, and store
    # the result in GCS
    exchange_rates_to_gcs = HttpToGcsOperator(
        task_id='exchange_rates_to_gcs',
        base_url='https://api.exchangeratesapi.io',
        endpoint='history',
        data={
            'start_at': '{{ yesterday_ds }}',
            'end_at': '{{ ds }}',
            'symbols': 'EUR',
            'base': 'GBP',
        },
        gcs_bucket="gdd_airflow_npage_exchange_rates",
        gcs_path='exchange_rates-{{ ds }}.json',
    )

    # Create the Cloud Dataproc cluster
    create_dataproc_cluster = DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        cluster_name='ephemeral-real-estate-{{ ds_nodash }}',
        num_workers=2,
        zone=Variable.get('gce_zone'),
    )

    [land_registry_prices_to_gcs, exchange_rates_to_gcs] >> create_dataproc_cluster
