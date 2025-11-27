"""
DAG Airflow pour l'ingestion batch de données agricoles
Orchestre la génération et l'ingestion de données depuis le générateur agricole
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import subprocess
import json
from pathlib import Path


def generate_agricultural_data(**context):
    """Génère des données agricoles"""
    try:
        result = subprocess.run(
            ['python3', '/opt/airflow/agricultural_data_generator.py'],
            capture_output=True,
            text=True,
            cwd='/opt/airflow'
        )
        
        if result.returncode == 0:
            print("✅ Données agricoles générées avec succès")
            print(result.stdout)
            context['ti'].xcom_push(key='generation_status', value='success')
            return 'success'
        else:
            print(f"❌ Erreur lors de la génération: {result.stderr}")
            context['ti'].xcom_push(key='generation_status', value='failed')
            return 'failed'
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
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
            print("✅ Données envoyées vers Kafka")
            print(result.stdout)
            return 'success'
        else:
            print(f"⚠️ Avertissement Kafka: {result.stderr}")
            return 'completed_with_warnings'
    except Exception as e:
        print(f"⚠️ Exception Kafka: {str(e)}")
        return 'failed'


def validate_data_lake(**context):
    """Valide que les données sont dans le Data Lake"""
    data_lake_path = Path('/opt/airflow/data_lake/raw/agriculture')
    
    if not data_lake_path.exists():
        raise ValueError("Le répertoire agriculture n'existe pas dans le Data Lake")
    
    expected_files = ['farms.json', 'fields.json', 'crops.json', 'harvests.json', 
                     'weather.json', 'sensors.json', 'sales.json']
    
    validation_report = {}
    for file in expected_files:
        file_path = data_lake_path / file
        if file_path.exists():
            file_size = file_path.stat().st_size
            validation_report[file] = {
                'exists': True,
                'size_bytes': file_size,
                'size_kb': round(file_size / 1024, 2)
            }
        else:
            validation_report[file] = {'exists': False}
    
    print("📊 Rapport de validation:")
    for file, info in validation_report.items():
        if info['exists']:
            print(f"  ✅ {file}: {info['size_kb']} KB")
        else:
            print(f"  ❌ {file}: MANQUANT")
    
    # Vérifier que tous les fichiers existent
    all_exist = all(info['exists'] for info in validation_report.values())
    
    if not all_exist:
        raise ValueError("Certains fichiers de données sont manquants")
    
    context['ti'].xcom_push(key='validation_report', value=validation_report)
    return validation_report


def generate_summary_report(**context):
    """Génère un rapport récapitulatif"""
    ti = context['ti']
    
    generation_status = ti.xcom_pull(key='generation_status', task_ids='generate_data')
    validation_report = ti.xcom_pull(key='validation_report', task_ids='validate_data')
    
    total_size = sum(info['size_kb'] for info in validation_report.values() if info['exists'])
    
    report = f"""
    ========================================
    📊 RAPPORT D'INGESTION AGRICOLE
    ========================================
    Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    
    Statut de génération: {generation_status}
    
    Fichiers générés:
    {chr(10).join([f"  - {file}: {info['size_kb']} KB" for file, info in validation_report.items() if info['exists']])}
    
    Taille totale: {total_size:.2f} KB
    
    ✅ Pipeline d'ingestion terminé avec succès!
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


# Définition du DAG
with DAG(
    dag_id='batch_ingestion',
    default_args=default_args,
    description='Ingestion batch de données agricoles',
    schedule_interval='0 2 * * *',  # Tous les jours à 2h du matin
    catchup=False,
    tags=['ingestion', 'batch', 'agriculture'],
) as dag:
    
    # Génération des données
    task_generate = PythonOperator(
        task_id='generate_data',
        python_callable=generate_agricultural_data,
        provide_context=True,
    )
    
    # Envoi vers Kafka
    task_kafka = PythonOperator(
        task_id='send_to_kafka',
        python_callable=send_to_kafka,
        provide_context=True,
    )
    
    # Validation du Data Lake
    task_validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data_lake,
        provide_context=True,
    )
    
    # Génération du rapport
    task_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_summary_report,
        provide_context=True,
    )
    
    # Définition des dépendances
    task_generate >> task_kafka >> task_validate >> task_report
