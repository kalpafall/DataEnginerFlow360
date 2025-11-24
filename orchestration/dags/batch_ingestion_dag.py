"""
DAG Airflow pour l'ingestion batch de données
Orchestre l'ingestion depuis différentes sources vers la couche RAW
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import sys
from pathlib import Path

# Ajouter le chemin du projet au PYTHONPATH
project_root = Path('/opt/airflow')
sys.path.insert(0, str(project_root))

from ingestion.data_ingestion import DataIngestion

# Configuration
CONFIG = {
    'data_lake_path': '/opt/airflow/data_lake',
    'postgres': {
        'host': 'postgres',
        'port': 5432,
        'database': 'dataenginerflow360',
        'user': 'postgres',
        'password': '1234'
    }
}


def ingest_fake_users(**context):
    """Ingère des utilisateurs fake"""
    ingestion = DataIngestion(CONFIG)
    output_path = ingestion.generate_fake_data(
        dataset_name='users_fake',
        num_records=1000,
        data_type='users'
    )
    context['ti'].xcom_push(key='users_path', value=output_path)
    return output_path


def ingest_fake_transactions(**context):
    """Ingère des transactions fake"""
    ingestion = DataIngestion(CONFIG)
    output_path = ingestion.generate_fake_data(
        dataset_name='transactions_fake',
        num_records=5000,
        data_type='transactions'
    )
    context['ti'].xcom_push(key='transactions_path', value=output_path)
    return output_path


def ingest_fake_logs(**context):
    """Ingère des logs fake"""
    ingestion = DataIngestion(CONFIG)
    output_path = ingestion.generate_fake_data(
        dataset_name='logs_fake',
        num_records=10000,
        data_type='logs'
    )
    context['ti'].xcom_push(key='logs_path', value=output_path)
    return output_path


def ingest_from_api(**context):
    """Ingère depuis une API publique"""
    ingestion = DataIngestion(CONFIG)
    try:
        output_path = ingestion.ingest_from_api(
            api_url='https://jsonplaceholder.typicode.com/posts',
            dataset_name='api_posts'
        )
        context['ti'].xcom_push(key='api_posts_path', value=output_path)
        return output_path
    except Exception as e:
        print(f"Erreur ingestion API: {e}")
        return None


def validate_ingestion(**context):
    """Valide que l'ingestion s'est bien passée"""
    ti = context['ti']
    
    users_path = ti.xcom_pull(key='users_path', task_ids='fake_data.ingest_users')
    transactions_path = ti.xcom_pull(key='transactions_path', task_ids='fake_data.ingest_transactions')
    logs_path = ti.xcom_pull(key='logs_path', task_ids='fake_data.ingest_logs')
    
    validation_report = {
        'users': users_path is not None,
        'transactions': transactions_path is not None,
        'logs': logs_path is not None,
        'timestamp': datetime.now().isoformat()
    }
    
    print(f"Validation Report: {validation_report}")
    
    if not all(validation_report.values()):
        raise ValueError("Certaines ingestions ont échoué")
    
    return validation_report


# Arguments par défaut
default_args = {
    'owner': 'dataengineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}


# Définition du DAG
with DAG(
    dag_id='batch_ingestion',
    default_args=default_args,
    description='Ingestion batch de données depuis différentes sources',
    schedule_interval='0 2 * * *',  # Tous les jours à 2h du matin
    catchup=False,
    tags=['ingestion', 'batch', 'raw'],
) as dag:
    
    # Groupe de tâches pour les données fake
    with TaskGroup('fake_data', tooltip='Génération de données fake') as fake_data_group:
        
        task_ingest_users = PythonOperator(
            task_id='ingest_users',
            python_callable=ingest_fake_users,
            provide_context=True,
        )
        
        task_ingest_transactions = PythonOperator(
            task_id='ingest_transactions',
            python_callable=ingest_fake_transactions,
            provide_context=True,
        )
        
        task_ingest_logs = PythonOperator(
            task_id='ingest_logs',
            python_callable=ingest_fake_logs,
            provide_context=True,
        )
    
    # Groupe de tâches pour les sources externes
    with TaskGroup('external_sources', tooltip='Ingestion depuis sources externes') as external_group:
        
        task_ingest_api = PythonOperator(
            task_id='ingest_api',
            python_callable=ingest_from_api,
            provide_context=True,
        )
    
    # Validation finale
    task_validate = PythonOperator(
        task_id='validate_ingestion',
        python_callable=validate_ingestion,
        provide_context=True,
    )
    
    # Définition des dépendances
    [fake_data_group, external_group] >> task_validate
