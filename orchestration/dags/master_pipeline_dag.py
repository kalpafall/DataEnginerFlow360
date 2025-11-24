"""
DAG Master pour orchestrer l'ensemble du pipeline de données
Coordonne: Ingestion → PySpark → dbt
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor


def log_pipeline_start(**context):
    """Log le début du pipeline"""
    execution_date = context['execution_date']
    print(f"=== DÉMARRAGE DU PIPELINE COMPLET ===")
    print(f"Date d'exécution: {execution_date}")
    print(f"Pipeline: Ingestion → PySpark → dbt")
    return execution_date.isoformat()


def log_pipeline_end(**context):
    """Log la fin du pipeline"""
    ti = context['ti']
    start_time = ti.xcom_pull(task_ids='log_start')
    
    print(f"=== PIPELINE TERMINÉ AVEC SUCCÈS ===")
    print(f"Démarré à: {start_time}")
    print(f"Terminé à: {datetime.now().isoformat()}")
    
    return {
        'status': 'success',
        'start_time': start_time,
        'end_time': datetime.now().isoformat()
    }


# Arguments par défaut
default_args = {
    'owner': 'dataengineer',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}


# Définition du DAG Master
with DAG(
    dag_id='master_data_pipeline',
    default_args=default_args,
    description='Pipeline Master: Orchestration complète du flux de données',
    schedule_interval='0 2 * * *',  # Tous les jours à 2h du matin
    catchup=False,
    tags=['master', 'orchestration', 'pipeline'],
    max_active_runs=1,  # Un seul run à la fois
) as dag:
    
    # Log de démarrage
    task_log_start = PythonOperator(
        task_id='log_start',
        python_callable=log_pipeline_start,
        provide_context=True,
    )
    
    # Déclencher l'ingestion batch
    trigger_ingestion = TriggerDagRunOperator(
        task_id='trigger_batch_ingestion',
        trigger_dag_id='batch_ingestion',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    # Déclencher les transformations PySpark
    trigger_pyspark = TriggerDagRunOperator(
        task_id='trigger_pyspark_transformation',
        trigger_dag_id='pyspark_transformation',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    # Déclencher dbt
    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt_transformation',
        trigger_dag_id='dbt_transformation',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    # Log de fin
    task_log_end = PythonOperator(
        task_id='log_end',
        python_callable=log_pipeline_end,
        provide_context=True,
    )
    
    # Définition du flux
    task_log_start >> trigger_ingestion >> trigger_pyspark >> trigger_dbt >> task_log_end
