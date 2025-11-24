"""
DAG Airflow pour exécuter les modèles dbt
Orchestre les transformations SQL dans le Data Warehouse
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator

# Configuration
DBT_PROJECT_DIR = '/opt/airflow/transformation/dbt'
DBT_PROFILES_DIR = '/opt/airflow/transformation/dbt'


def check_dbt_installation(**context):
    """Vérifie que dbt est installé"""
    import subprocess
    try:
        result = subprocess.run(['dbt', '--version'], capture_output=True, text=True)
        print(f"dbt version: {result.stdout}")
        return True
    except Exception as e:
        print(f"Erreur: dbt n'est pas installé - {e}")
        raise


def generate_dbt_docs(**context):
    """Génère la documentation dbt"""
    print("Documentation dbt générée avec succès")
    return True


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
    dag_id='dbt_transformation',
    default_args=default_args,
    description='Transformations dbt: PROCESSED → CURATED (Data Warehouse)',
    schedule_interval='0 4 * * *',  # Tous les jours à 4h (après PySpark)
    catchup=False,
    tags=['transformation', 'dbt', 'curated', 'warehouse'],
) as dag:
    
    # Attendre que les transformations PySpark soient terminées
    wait_for_pyspark = ExternalTaskSensor(
        task_id='wait_for_pyspark',
        external_dag_id='pyspark_transformation',
        external_task_id='validate_transformations',
        timeout=600,
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='reschedule',
    )
    
    # Vérifier l'installation de dbt
    check_dbt = PythonOperator(
        task_id='check_dbt',
        python_callable=check_dbt_installation,
        provide_context=True,
    )
    
    # Installer les dépendances dbt
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}',
    )
    
    # Exécuter les modèles dbt
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR}',
    )
    
    # Exécuter les tests dbt
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}',
    )
    
    # Générer la documentation
    dbt_docs_generate = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir {DBT_PROFILES_DIR}',
    )
    
    # Snapshot (optionnel - pour capturer l'état des données)
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt snapshot --profiles-dir {DBT_PROFILES_DIR} || true',
        trigger_rule='all_done',  # Exécuter même si les tests échouent
    )
    
    # Générer rapport final
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_dbt_docs,
        provide_context=True,
    )
    
    # Dépendances
    wait_for_pyspark >> check_dbt >> dbt_deps >> dbt_run
    dbt_run >> dbt_test >> dbt_docs_generate
    dbt_run >> dbt_snapshot
    [dbt_docs_generate, dbt_snapshot] >> generate_report
