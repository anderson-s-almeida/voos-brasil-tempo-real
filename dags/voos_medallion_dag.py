"""
Orquestra o pipeline de voos em tempo real (OpenSky) no Databricks.

O coletor grava os JSONs no volume /Volumes/workspace/voos/raw a cada 2 minutos.
Esta DAG dispara periodicamente o Job do Databricks (bronze -> silver -> gold),
que processa de forma incremental apenas os arquivos novos.
"""
from datetime import datetime, timedelta

from airflow.sdk import DAG, Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="voos_medallion",
    description="Dispara o Job bronze/silver/gold de voos no Databricks",
    start_date=datetime(2026, 10, 1),
    schedule="0 * * * *",          # de hora em hora
    catchup=False,                  # nao executa horarios passados
    max_active_runs=1,              # nunca duas execucoes ao mesmo tempo (checkpoint compartilhado)
    default_args=default_args,
    tags=["voos", "databricks", "tempo-real"],
) as dag:

    rodar_job = DatabricksRunNowOperator(
        task_id="rodar_job_voos_medallion",
        databricks_conn_id="databricks_default",
        job_id=Variable.get("voos_job_id"),
    )
