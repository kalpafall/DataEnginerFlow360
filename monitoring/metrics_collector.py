"""
Script de collecte de métriques custom pour Prometheus
Collecte des métriques de qualité de données et métriques métier
"""
import time
import psycopg2
from prometheus_client import start_http_server, Gauge, Counter, Histogram
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Définition des métriques
data_rows_processed = Counter(
    'data_rows_processed_total',
    'Nombre total de lignes de données traitées',
    ['dataset', 'layer']
)

data_validation_errors = Counter(
    'data_validation_errors_total',
    'Nombre total d'erreurs de validation',
    ['dataset', 'error_type']
)

dbt_test_failures = Gauge(
    'dbt_test_failures_total',
    'Nombre de tests dbt échoués'
)

pipeline_duration = Histogram(
    'pipeline_duration_seconds',
    'Durée d'exécution du pipeline',
    ['pipeline_name']
)

data_freshness = Gauge(
    'data_freshness_minutes',
    'Fraîcheur des données en minutes',
    ['dataset']
)


class MetricsCollector:
    """Collecteur de métriques custom"""
    
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
    
    def connect_db(self):
        """Connexion à PostgreSQL"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            logger.info("Connecté à PostgreSQL")
        except Exception as e:
            logger.error(f"Erreur connexion DB: {e}")
    
    def collect_data_quality_metrics(self):
        """Collecte les métriques de qualité de données"""
        if not self.conn:
            self.connect_db()
        
        try:
            cursor = self.conn.cursor()
            
            # Compter les lignes dans chaque table
            tables = ['users_fake', 'transactions_fake', 'logs_fake']
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                data_rows_processed.labels(dataset=table, layer='curated').inc(count)
                logger.info(f"{table}: {count} lignes")
            
            # Vérifier la fraîcheur des données
            cursor.execute("""
                SELECT 
                    'transactions_fake' as dataset,
                    EXTRACT(EPOCH FROM (NOW() - MAX(transaction_date)))/60 as minutes
                FROM transactions_fake
            """)
            result = cursor.fetchone()
            if result:
                data_freshness.labels(dataset=result[0]).set(result[1])
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"Erreur collecte métriques: {e}")
            data_validation_errors.labels(dataset='all', error_type='collection_error').inc()
    
    def collect_dbt_metrics(self):
        """Collecte les métriques dbt"""
        # Simuler la collecte de résultats de tests dbt
        # En production, cela lirait les résultats de dbt test
        try:
            # Exemple: lire depuis un fichier de résultats dbt
            # Pour l'instant, on simule
            dbt_test_failures.set(0)
            logger.info("Métriques dbt collectées")
        except Exception as e:
            logger.error(f"Erreur collecte dbt: {e}")
    
    def run(self, interval=60):
        """Boucle principale de collecte"""
        logger.info(f"Démarrage collecteur de métriques (interval: {interval}s)")
        
        while True:
            try:
                self.collect_data_quality_metrics()
                self.collect_dbt_metrics()
                time.sleep(interval)
            except KeyboardInterrupt:
                logger.info("Arrêt du collecteur")
                break
            except Exception as e:
                logger.error(f"Erreur dans la boucle: {e}")
                time.sleep(interval)


if __name__ == '__main__':
    # Configuration
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'dataenginerflow360',
        'user': 'postgres',
        'password': '1234'
    }
    
    # Démarrer le serveur HTTP pour Prometheus
    start_http_server(8000)
    logger.info("Serveur de métriques démarré sur :8000")
    
    # Démarrer la collecte
    collector = MetricsCollector(db_config)
    collector.run(interval=60)
