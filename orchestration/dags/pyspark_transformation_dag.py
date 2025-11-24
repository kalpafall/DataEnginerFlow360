"""
DAG Airflow pour les transformations PySpark
Orchestre les transformations de la couche RAW vers PROCESSED
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
import sys
from pathlib import Path
import os

# Ajouter le chemin du projet au PYTHONPATH
project_root = Path('/opt/airflow')
sys.path.insert(0, str(project_root))

from transformation.spark_transformation import SparkTransformation

# Configuration
CONFIG = {
    'data_lake_path': '/opt/airflow/data_lake',
}

# Définir JAVA_HOME
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-amd64'


def transform_users(**context):
    """Transforme les données utilisateurs"""
    transformer = SparkTransformation(CONFIG)
    try:
        output_path = transformer.transform_pipeline(
            dataset_name='users_fake',
            dataset_type='users',
            with_aggregation=False
        )
        context['ti'].xcom_push(key='users_processed', value=output_path)
        return output_path
    finally:
        transformer.stop()


def transform_transactions(**context):
    """Transforme les données transactions"""
    transformer = SparkTransformation(CONFIG)
    try:
        output_path = transformer.transform_pipeline(
            dataset_name='transactions_fake',
            dataset_type='transactions',
            with_aggregation=True
        )
        context['ti'].xcom_push(key='transactions_processed', value=output_path)
        return output_path
    finally:
        transformer.stop()


def transform_logs(**context):
    """Transforme les données logs"""
    transformer = SparkTransformation(CONFIG)
    try:
        output_path = transformer.transform_pipeline(
            dataset_name='logs_fake',
            dataset_type='logs',
            with_aggregation=True
        )
        context['ti'].xcom_push(key='logs_processed', value=output_path)
        return output_path
    finally:
        transformer.stop()


def validate_transformations(**context):
    """Valide que les transformations sont complètes"""
    ti = context['ti']
    
    users_processed = ti.xcom_pull(key='users_processed', task_ids='transform_users')
    transactions_processed = ti.xcom_pull(key='transactions_processed', task_ids='transform_transactions')
    logs_processed = ti.xcom_pull(key='logs_processed', task_ids='transform_logs')
    
    validation_report = {
        'users': users_processed is not None,
        'transactions': transactions_processed is not None,
        'logs': logs_processed is not None,
        'timestamp': datetime.now().isoformat()
    }
    
    print(f"Transformation Validation: {validation_report}")
    
    if not all(validation_report.values()):
        raise ValueError("Certaines transformations ont échoué")
    
    return validation_report


# Arguments par défaut
default_args = {
    'owner': 'dataengineer',
    'depends_on_past': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}


# Définition du DAG
with DAG(
    dag_id='pyspark_transformation',
    default_args=default_args,
    description='Transformations PySpark: RAW → PROCESSED',
    schedule_interval='0 3 * * *',  # Tous les jours à 3h (après ingestion)
    catchup=False,
    tags=['transformation', 'pyspark', 'processed'],
) as dag:
    
    # Attendre que l'ingestion soit terminée
    wait_for_ingestion = ExternalTaskSensor(
        task_id='wait_for_ingestion',
        external_dag_id='batch_ingestion',
        external_task_id='validate_ingestion',
        timeout=600,
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='reschedule',
    )
    
    # Transformations en parallèle
    task_transform_users = PythonOperator(
        task_id='transform_users',
        python_callable=transform_users,
        provide_context=True,
    )
    
    task_transform_transactions = PythonOperator(
        task_id='transform_transactions',
        python_callable=transform_transactions,
        provide_context=True,
    )
    
    task_transform_logs = PythonOperator(
        task_id='transform_logs',
        python_callable=transform_logs,
        provide_context=True,
    )
    
    # Validation
    task_validate = PythonOperator(
        task_id='validate_transformations',
        python_callable=validate_transformations,
        provide_context=True,
    )
    
    # Dépendances
    wait_for_ingestion >> [task_transform_users, task_transform_transactions, task_transform_logs]
    [task_transform_users, task_transform_transactions, task_transform_logs] >> task_validate
