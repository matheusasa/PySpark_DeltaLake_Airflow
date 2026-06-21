"""DAG Airflow para gerar datasets sinteticos do pipeline E-commerce."""
import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Permite importar o modulo datasets/gerar_dados.py
sys.path.insert(0, "/opt/airflow/datasets")
from gerar_dados import (
    gerar_fornecedores,
    gerar_clientes,
    gerar_cupons,
    gerar_produtos,
    gerar_vendas_fretes_pagamentos,
    gerar_avaliacoes,
)

OUTPUT_DIR = str(Path("/opt/airflow/datasets"))

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gerar_dados_ecommerce",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["datagen", "ecommerce"],
    description="Gera datasets sinteticos para o pipeline E-commerce (Bronze/Silver/Gold)",
) as dag:

    t_fornecedores = PythonOperator(
        task_id="gerar_fornecedores",
        python_callable=gerar_fornecedores,
        op_kwargs={
            "output_dir": OUTPUT_DIR,
            "run_timestamp": "{{ ts_nodash }}",
        },
    )

    t_clientes = PythonOperator(
        task_id="gerar_clientes",
        python_callable=gerar_clientes,
        op_kwargs={
            "output_dir": OUTPUT_DIR,
            "run_timestamp": "{{ ts_nodash }}",
        },
    )

    t_cupons = PythonOperator(
        task_id="gerar_cupons",
        python_callable=gerar_cupons,
        op_kwargs={
            "output_dir": OUTPUT_DIR,
            "run_timestamp": "{{ ts_nodash }}",
        },
    )

    t_produtos = PythonOperator(
        task_id="gerar_produtos",
        python_callable=gerar_produtos,
        op_kwargs={
            "output_dir": OUTPUT_DIR,
            "run_timestamp": "{{ ts_nodash }}",
        },
    )

    t_vendas = PythonOperator(
        task_id="gerar_vendas_fretes_pagamentos",
        python_callable=gerar_vendas_fretes_pagamentos,
        op_kwargs={
            "output_dir": OUTPUT_DIR,
            "run_timestamp": "{{ ts_nodash }}",
        },
    )

    t_avaliacoes = PythonOperator(
        task_id="gerar_avaliacoes",
        python_callable=gerar_avaliacoes,
        op_kwargs={
            "output_dir": OUTPUT_DIR,
            "run_timestamp": "{{ ts_nodash }}",
        },
    )

    # Dependencias: dimensoes em paralelo -> vendas -> avaliacoes
    [t_fornecedores, t_clientes, t_cupons, t_produtos] >> t_vendas >> t_avaliacoes
