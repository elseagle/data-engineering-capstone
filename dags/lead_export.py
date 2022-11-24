import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from utils.transformers import get_leads, load_leads_to_parquet

log = logging.getLogger(__name__)


default_args = {
    "owner": "Sogo Ogundowole",
    "depends_on_past": False,
    "email": ["dowolebolu@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lead_export_dag",
    description="Exports daily leads to database",
    default_args=default_args,
    schedule_interval="0 7 * * *",  # 7am daily
    start_date=datetime(2022, 11, 23),
    catchup=False,
) as dag:

    @task(task_id="get_leads")
    def do_get_leads():
        """Get leads from the Google Sheet or CSV"""
        get_leads()

    @task(task_id="load_leads_to_parquet")
    def do_load_leads_to_parquet():
        """Load leads to the database."""
        load_leads_to_parquet()

    do_get_leads()
    do_load_leads_to_parquet()
