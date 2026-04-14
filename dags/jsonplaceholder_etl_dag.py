"""
DAG для ETL процесса JSONPlaceholder -> SQLite
Использует контекстный менеджер 'with DAG' и logging
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# Импортируем ETL скрипт
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
from etl_jsonplaceholder import run_etl

# Настройка логирования
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG с использованием контекстного менеджера
with DAG(
    dag_id='jsonplaceholder_etl',
    default_args=default_args,
    description='ETL DAG: загрузка данных из JSONPlaceholder в SQLite',
    schedule_interval='@daily',  # Запуск каждый день
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'jsonplaceholder', 'sqlite'],
) as dag:

    def log_start(**kwargs):
        """Логирование начала запуска DAG"""
        logger.info("=" * 60)
        logger.info("Начало запуска DAG jsonplaceholder_etl")
        logger.info(f"Execution date: {kwargs['execution_date']}")
        logger.info("=" * 60)

    def log_completion(**kwargs):
        """Логирование завершения запуска DAG"""
        logger.info("=" * 60)
        logger.info("DAG jsonplaceholder_etl успешно завершен")
        logger.info("=" * 60)

    # Task 1: Логирование начала
    start_task = PythonOperator(
        task_id='log_start',
        python_callable=log_start,
        provide_context=True,
    )

    # Task 2: ETL процесс
    etl_task = PythonOperator(
        task_id='run_etl',
        python_callable=run_etl,
    )

    # Task 3: Логирование завершения
    end_task = PythonOperator(
        task_id='log_completion',
        python_callable=log_completion,
        provide_context=True,
    )

    # Определение зависимостей
    start_task >> etl_task >> end_task
