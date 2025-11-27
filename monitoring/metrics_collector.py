import time
import json
import threading
import psycopg2
from kafka import KafkaConsumer
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
    'Nombre total d\'erreurs de validation',
    ['dataset', 'error_type']
)

dbt_test_failures = Gauge(
    'dbt_test_failures_total',
    'Nombre de tests dbt échoués'
)

pipeline_duration = Histogram(
    'pipeline_duration_seconds',
    'Durée d\'exécution du pipeline',
    ['pipeline_name']
)

data_freshness = Gauge(
    'data_freshness_minutes',
    'Fraîcheur des données en minutes',
    ['dataset']
)

# Métriques Agricoles (IoT)
sensor_value = Gauge(
    'sensor_value',
    'Valeur du capteur IoT',
    ['type', 'location', 'sensor_id']
)

class MetricsCollector:
    """Collecteur de métriques custom"""
    
    def __init__(self, db_config, kafka_bootstrap):
        self.db_config = db_config
        self.kafka_bootstrap = kafka_bootstrap
        self.conn = None
    
    def connect_db(self):
        """Connexion à PostgreSQL"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            logger.info("Connecté à PostgreSQL")
        except Exception as e:
            logger.error(f"Erreur connexion DB: {e}")
    
    def consume_kafka_metrics(self):
        """Consomme les messages Kafka et met à jour les métriques"""
        logger.info(f"Démarrage du consommateur Kafka sur {self.kafka_bootstrap}")
        try:
            consumer = KafkaConsumer(
                'sensor_data',
                bootstrap_servers=self.kafka_bootstrap,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='metrics_collector_group'
            )
            
            for message in consumer:
                data = message.value
                # Mettre à jour la gauge Prometheus
                if 'sensor_type' in data and 'value' in data:
                    sensor_value.labels(
                        type=data['sensor_type'],
                        location=data.get('location', 'unknown'),
                        sensor_id=data.get('sensor_id', 'unknown')
                    ).set(data['value'])
                    
                    # logger.debug(f"Métrique mise à jour: {data['sensor_type']} = {data['value']}")
                    
        except Exception as e:
            logger.error(f"Erreur Kafka Consumer: {e}")

    def collect_data_quality_metrics(self):
        """Collecte les métriques de qualité de données"""
        if not self.conn:
            self.connect_db()
        
        try:
            cursor = self.conn.cursor()
            
            # Compter les lignes dans chaque table (si elles existent)
            # ... (logique existante simplifiée pour la démo)
            pass
            
            cursor.close()
            
        except Exception as e:
            # logger.error(f"Erreur collecte métriques DB: {e}")
            pass
    
    def run(self, interval=60):
        """Boucle principale de collecte"""
        logger.info(f"Démarrage collecteur de métriques")
        
        # Démarrer le consommateur Kafka dans un thread séparé
        kafka_thread = threading.Thread(target=self.consume_kafka_metrics)
        kafka_thread.daemon = True
        kafka_thread.start()
        
        while True:
            try:
                self.collect_data_quality_metrics()
                time.sleep(interval)
            except KeyboardInterrupt:
                logger.info("Arrêt du collecteur")
                break
            except Exception as e:
                logger.error(f"Erreur dans la boucle: {e}")
                time.sleep(interval)


if __name__ == '__main__':
    import os
    
    # Configuration via variables d'environnement
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '5433')),
        'database': 'dataenginerflow360',
        'user': 'postgres',
        'password': 'postgres'
    }
    
    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9093')
    
    # Démarrer le serveur HTTP pour Prometheus
    start_http_server(8000)
    logger.info(f"Serveur de métriques démarré sur :8000 (DB={db_config['host']}, Kafka={kafka_bootstrap})")
    
    # Démarrer la collecte
    collector = MetricsCollector(db_config, kafka_bootstrap)
    collector.run(interval=60)
