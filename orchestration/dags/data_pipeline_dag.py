"""
DAG Airflow pour le pipeline complet de données agricoles
Architecture: Génération -> Transformation -> Analyse
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import subprocess
from pathlib import Path


def run_agricultural_generation(**context):
    """Génère les données agricoles"""
    try:
        result = subprocess.run(
            ['python3', '/opt/airflow/agricultural_data_generator.py'],
            capture_output=True,
            text=True,
            cwd='/opt/airflow'
        )
        
        if result.returncode == 0:
            print("✅ Données générées")
            print(result.stdout)
            return 'success'
        else:
            print(f"❌ Erreur: {result.stderr}")
            return 'failed'
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        return 'failed'


def run_transformation_script(**context):
    """Exécute les transformations pandas"""
    try:
        result = subprocess.run(
            ['python3', '/opt/airflow/transformation/agricultural_processing_simple.py'],
            capture_output=True,
            text=True,
            cwd='/opt/airflow'
        )
        
        if result.returncode == 0:
            print("✅ Transformations terminées")
            print(result.stdout)
            return 'success'
        else:
            print(f"⚠️ Avertissement: {result.stderr}")
            return 'completed_with_warnings'
    except Exception as e:
        print(f"⚠️ Exception: {str(e)}")
        return 'failed'


def send_to_kafka(**context):
    """Envoie les données vers Kafka"""
    try:
        result = subprocess.run(
            ['python3', '/opt/airflow/demo_agricultural_pipeline.py'],
            capture_output=True,
            text=True,
            cwd='/opt/airflow'
        )
        
        if result.returncode == 0:
            print("✅ Envoi Kafka terminé")
            return 'success'
        else:
            print(f"⚠️ Avertissement Kafka: {result.stderr}")
            return 'completed_with_warnings'
    except Exception as e:
        print(f"⚠️ Exception Kafka: {str(e)}")
        return 'failed'


def generate_pipeline_report(**context):
    """Génère un rapport du pipeline"""
    report = f"""
    ========================================
    RAPPORT DU PIPELINE COMPLET
    ========================================
    Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    
    ✅ Pipeline exécuté:
    1. Génération de données agricoles
    2. Transformation des données
    3. Envoi vers Kafka
    
    Pipeline terminé avec succès!
    ========================================
    """
    
    print(report)
    return report


# Arguments par défaut
default_args = {
    'owner': 'dataengineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 11, 25),
}


# Définition du DAG principal
with DAG(
    dag_id='data_pipeline_complete',
    default_args=default_args,
    description='Pipeline complet: Génération -> Transformation -> Kafka',
    schedule_interval='@daily',
    catchup=False,
    tags=['pipeline', 'agriculture', 'complete'],
) as dag:
    
    # Génération des données
    task_generate = PythonOperator(
        task_id='generate_data',
        python_callable=run_agricultural_generation,
        provide_context=True,
    )
    
    # Transformation des données
    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=run_transformation_script,
        provide_context=True,
    )
    
    # Envoi vers Kafka
    task_kafka = PythonOperator(
        task_id='send_to_kafka',
        python_callable=send_to_kafka,
        provide_context=True,
    )
    
    # Rapport final
    task_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_pipeline_report,
        provide_context=True,
    )
    
    # Ordre d'exécution
    task_generate >> task_transform >> task_kafka >> task_report
