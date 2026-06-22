"""DAG Airflow para executar o pipeline Bronze/Silver/Gold."""
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

SPARK_CONTAINER = "databricks-sim"


def on_failure_callback(context):
    print(json.dumps({
        "timestamp": datetime.utcnow().isoformat(),
        "level": "ERROR",
        "dag_id": context.get("dag").dag_id,
        "task_id": context.get("task").task_id,
        "execution_date": str(context.get("execution_date")),
        "exception": str(context.get("exception")),
        "status": "erro",
    }))


def read_metrics_from_spark(task_id, camada, **kwargs):
    ti = kwargs['ti']
    exit_code = ti.xcom_pull(task_ids=task_id)
    if exit_code == 0:
        metrics = {"camada": camada, "status": "sucesso"}
    else:
        metrics = {"camada": camada, "status": "erro"}
    ti.xcom_push(key=f"metrics_{camada}", value=metrics)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="pipeline_ecommerce",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "pipeline"],
    description="Executa o pipeline Bronze -> Silver -> Gold (datasets devem ser gerados previamente via tools/run_datagen.py)",
) as dag:

    t_bronze = BashOperator(
        task_id="bronze_ingestao",
        bash_command=f"docker exec {SPARK_CONTAINER} bash -c 'PYTHONPATH=/opt/spark/work-dir/tools spark-submit --packages io.delta:delta-spark_2.13:4.0.0 /opt/spark/work-dir/tools/run_bronze.py'",
    )

    t_silver = BashOperator(
        task_id="silver_transformacao",
        bash_command=f"docker exec {SPARK_CONTAINER} bash -c 'PYTHONPATH=/opt/spark/work-dir/tools spark-submit --packages io.delta:delta-spark_2.13:4.0.0 /opt/spark/work-dir/tools/run_silver.py'",
    )

    t_gold = BashOperator(
        task_id="gold_agregacao",
        bash_command=f"docker exec {SPARK_CONTAINER} bash -c 'PYTHONPATH=/opt/spark/work-dir/tools spark-submit --packages io.delta:delta-spark_2.13:4.0.0 /opt/spark/work-dir/tools/run_gold.py'",
    )

    t_bronze_metrics = PythonOperator(
        task_id="bronze_metrics",
        python_callable=read_metrics_from_spark,
        op_kwargs={"task_id": "bronze_ingestao", "camada": "bronze"},
    )

    t_silver_metrics = PythonOperator(
        task_id="silver_metrics",
        python_callable=read_metrics_from_spark,
        op_kwargs={"task_id": "silver_transformacao", "camada": "silver"},
    )

    t_gold_metrics = PythonOperator(
        task_id="gold_metrics",
        python_callable=read_metrics_from_spark,
        op_kwargs={"task_id": "gold_agregacao", "camada": "gold"},
    )

    t_bronze >> t_bronze_metrics >> t_silver >> t_silver_metrics >> t_gold >> t_gold_metrics
