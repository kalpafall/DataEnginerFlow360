"""
DAG Airflow pour orchestrer le pipeline complet d'ingestion et transformation
Architecture: Apache Airflow orchestre toutes les étapes du pipeline
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
from pathlib import Path

# Ajouter le chemin du projet au PYTHONPATH
project_root = Path('/home/mariama/Documents/ODC/DataEnginerFlow360')
sys.path.insert(0, str(project_root))

from ingestion.data_ingestion import DataIngestion
from transformation.spark_transformation import SparkTransformation
from storage.postgres_storage import PostgresStorage, WarehouseBuilder


# Configuration globale
CONFIG = {
    'data_lake_path': str(project_root / 'data_lake'),
    'postgres': {
        'host': 'postgres',
        'port': 5432,
        'database': 'dataenginerflow360',
        'user': 'postgres',
        'password': 'postgres'
    }
}


# Fonctions pour les tâches Airflow
def ingest_fake_users(**context):
    """Ingère des utilisateurs fake avec Faker"""
    ingestion = DataIngestion(CONFIG)
    output_path = ingestion.generate_fake_data(
        dataset_name='users_fake',
        num_records=1000,
        data_type='users'
    )
    context['ti'].xcom_push(key='users_path', value=output_path)
    return output_path


def ingest_fake_transactions(**context):
    """Ingère des transactions fake avec Faker"""
    ingestion = DataIngestion(CONFIG)
    output_path = ingestion.generate_fake_data(
        dataset_name='transactions_fake',
        num_records=5000,
        data_type='transactions'
    )
    context['ti'].xcom_push(key='transactions_path', value=output_path)
    return output_path


def ingest_fake_logs(**context):
    """Ingère des logs fake avec Faker"""
    ingestion = DataIngestion(CONFIG)
    output_path = ingestion.generate_fake_data(
        dataset_name='logs_fake',
        num_records=10000,
        data_type='logs'
    )
    context['ti'].xcom_push(key='logs_path', value=output_path)
    return output_path


def ingest_from_api(**context):
    """Ingère des données depuis une API publique"""
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


def transform_users(**context):
    """Transforme les données utilisateurs avec PySpark"""
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
    """Transforme les données transactions avec PySpark"""
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
    """Transforme les données logs avec PySpark"""
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


def load_to_postgres(**context):
    """Charge les données dans PostgreSQL (couche CURATED)"""
    storage = PostgresStorage(CONFIG)
    try:
        # Charger les datasets
        storage.load_from_processed('users_fake', 'users_fake')
        storage.load_from_processed('transactions_fake', 'transactions_fake')
        storage.load_from_processed('logs_fake', 'logs_fake')
        
        # Créer les index pour optimisation
        storage.create_indexes('users_fake', ['user_id', 'email', 'country'])
        storage.create_indexes('transactions_fake', ['user_id', 'transaction_date', 'category'])
        storage.create_indexes('logs_fake', ['source', 'level', 'log_date'])
        
        print("Chargement PostgreSQL terminé")
    finally:
        storage.close()


def build_analytics_tables(**context):
    """Construit les tables analytiques dans le Data Warehouse"""
    storage = PostgresStorage(CONFIG)
    try:
        builder = WarehouseBuilder(storage)
        builder.build_user_analytics()
        builder.build_transaction_summary()
        print("Tables analytiques créées")
    finally:
        storage.close()


def generate_ingestion_report(**context):
    """Génère un rapport d'ingestion"""
    ti = context['ti']
    
    users_path = ti.xcom_pull(key='users_path', task_ids='ingest_fake_users')
    transactions_path = ti.xcom_pull(key='transactions_path', task_ids='ingest_fake_transactions')
    logs_path = ti.xcom_pull(key='logs_path', task_ids='ingest_fake_logs')
    
    report = f"""
    ========================================
    RAPPORT D'INGESTION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    ========================================
    
    Données ingérées:
    - Utilisateurs: {users_path}
    - Transactions: {transactions_path}
    - Logs: {logs_path}
    
    Pipeline exécuté avec succès!
    ========================================
    """
    
    print(report)
    return report


# Arguments par défaut du DAG
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
    dag_id='data_pipeline_complete',
    default_args=default_args,
    description='Pipeline complet: Ingestion multi-sources -> Transformation PySpark -> PostgreSQL',
    schedule_interval='@daily',
    catchup=False,
    tags=['ingestion', 'transformation', 'datawarehouse'],
) as dag:
    
    # ===== ÉTAPE 1: INGESTION (couche RAW) =====
    task_ingest_users = PythonOperator(
        task_id='ingest_fake_users',
        python_callable=ingest_fake_users,
        provide_context=True,
    )
    
    task_ingest_transactions = PythonOperator(
        task_id='ingest_fake_transactions',
        python_callable=ingest_fake_transactions,
        provide_context=True,
    )
    
    task_ingest_logs = PythonOperator(
        task_id='ingest_fake_logs',
        python_callable=ingest_fake_logs,
        provide_context=True,
    )
    
    task_ingest_api = PythonOperator(
        task_id='ingest_from_api',
        python_callable=ingest_from_api,
        provide_context=True,
    )
    
    # ===== ÉTAPE 2: TRANSFORMATION (couche PROCESSED) =====
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
    
    # ===== ÉTAPE 3: CHARGEMENT POSTGRESQL (couche CURATED) =====
    task_load_postgres = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True,
    )
    
    # ===== ÉTAPE 4: CONSTRUCTION TABLES ANALYTIQUES =====
    task_build_analytics = PythonOperator(
        task_id='build_analytics_tables',
        python_callable=build_analytics_tables,
        provide_context=True,
    )
    
    # ===== ÉTAPE 5: RAPPORT =====
    task_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_ingestion_report,
        provide_context=True,
    )
    
    # Définition des dépendances (ordre d'exécution)
    # Ingestion en parallèle
    [task_ingest_users, task_ingest_transactions, task_ingest_logs, task_ingest_api]
    
    # Transformation après ingestion
    task_ingest_users >> task_transform_users
    task_ingest_transactions >> task_transform_transactions
    task_ingest_logs >> task_transform_logs
    
    # Chargement PostgreSQL après toutes les transformations
    [task_transform_users, task_transform_transactions, task_transform_logs] >> task_load_postgres
    
    # Construction des tables analytiques
    task_load_postgres >> task_build_analytics
    
    # Rapport final
    task_build_analytics >> task_report


# DAG alternatif pour ingestion Kafka streaming
with DAG(
    dag_id='kafka_streaming_ingestion',
    default_args=default_args,
    description='Ingestion streaming depuis Kafka',
    schedule_interval='@hourly',
    catchup=False,
    tags=['streaming', 'kafka'],
) as dag_streaming:
    
    def ingest_from_kafka(**context):
        """Ingère des données depuis Kafka"""
        ingestion = DataIngestion(CONFIG)
        try:
            output_path = ingestion.ingest_from_kafka(
                topic='events',
                dataset_name='kafka_events',
                bootstrap_servers=['kafka:9092'],
                max_messages=10000
            )
            return output_path
        except Exception as e:
            print(f"Erreur ingestion Kafka: {e}")
            return None
    
    task_kafka_ingest = PythonOperator(
        task_id='ingest_kafka_events',
        python_callable=ingest_from_kafka,
        provide_context=True,
    )
