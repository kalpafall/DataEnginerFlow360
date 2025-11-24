import logging
from data_ingestion import DataIngestion
from kafka import KafkaProducer
import json
import time

import sys
import os
# Add current directory to sys.path
sys.path.append(os.getcwd())

logging.basicConfig(level=logging.INFO)

def test_kafka_ingestion():
    # 1. Produire des messages dans Kafka
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9093'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    topic = 'test_events'
    print(f"🚀 Envoi de messages dans le topic {topic}...")
    
    for i in range(10):
        message = {'id': i, 'event': 'test', 'timestamp': time.time()}
        producer.send(topic, message)
    
    producer.flush()
    print("✅ Messages envoyés !")

    # 2. Ingérer depuis Kafka
    config = {
        "data_lake_path": "./data_lake",
        "kafka": {
            "bootstrap_servers": ["localhost:9093"]
        }
    }
    
    ingestion = DataIngestion(config)
    print("\n🎧 Test Ingestion Kafka...")
    try:
        output_path = ingestion.ingest_from_kafka(
            dataset_name="test_kafka",
            topic=topic,
            max_messages=10
        )
        if output_path:
            print(f"✅ Succès ! Fichier créé : {output_path}")
        else:
            print("❌ Échec : Aucun fichier créé.")
    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    test_kafka_ingestion()
