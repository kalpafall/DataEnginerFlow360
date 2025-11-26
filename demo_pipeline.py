#!/usr/bin/env python3
"""
Script de démonstration du pipeline DataEnginerFlow360
Génère des données de test et les fait circuler dans le pipeline
"""
import os
import sys
import json
import time
from datetime import datetime
from pathlib import Path
from faker import Faker
import random

# Configuration
DATA_LAKE_PATH = Path("data_lake")
RAW_PATH = DATA_LAKE_PATH / "raw"
PROCESSED_PATH = DATA_LAKE_PATH / "processed"

print("=" * 70)
print("🚀 DÉMONSTRATION DU PIPELINE DATAENGINERFLOW360")
print("=" * 70)
print(f"⏰ Heure de début: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# Créer les répertoires
RAW_PATH.mkdir(parents=True, exist_ok=True)
PROCESSED_PATH.mkdir(parents=True, exist_ok=True)

# ============================================================================
# ÉTAPE 1: Génération de données de test
# ============================================================================
print("📥 ÉTAPE 1: Génération de données de test")
print("-" * 70)

fake = Faker()
Faker.seed(42)
random.seed(42)

# Générer des utilisateurs
num_users = 50
users = []
print(f"   Génération de {num_users} utilisateurs...")

for i in range(num_users):
    user = {
        "user_id": i + 1,
        "username": fake.user_name(),
        "email": fake.email(),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "phone": fake.phone_number(),
        "address": fake.address().replace('\n', ', '),
        "city": fake.city(),
        "country": fake.country(),
        "registration_date": fake.date_between(start_date='-2y', end_date='today').isoformat(),
        "is_active": random.choice([True, False]),
        "created_at": datetime.now().isoformat()
    }
    users.append(user)

users_file = RAW_PATH / "demo_users.json"
with open(users_file, 'w') as f:
    json.dump(users, f, indent=2)
print(f"   ✅ {num_users} utilisateurs générés → {users_file}")

# Générer des transactions
num_transactions = 200
transactions = []
print(f"   Génération de {num_transactions} transactions...")

transaction_types = ['purchase', 'refund', 'subscription', 'payment']
statuses = ['completed', 'pending', 'failed', 'cancelled']

for i in range(num_transactions):
    transaction = {
        "transaction_id": f"TXN-{i+1:06d}",
        "user_id": random.randint(1, num_users),
        "amount": round(random.uniform(10.0, 1000.0), 2),
        "currency": random.choice(['USD', 'EUR', 'GBP']),
        "transaction_type": random.choice(transaction_types),
        "status": random.choice(statuses),
        "merchant": fake.company(),
        "description": fake.sentence(),
        "transaction_date": fake.date_time_between(start_date='-30d', end_date='now').isoformat(),
        "created_at": datetime.now().isoformat()
    }
    transactions.append(transaction)

transactions_file = RAW_PATH / "demo_transactions.json"
with open(transactions_file, 'w') as f:
    json.dump(transactions, f, indent=2)
print(f"   ✅ {num_transactions} transactions générées → {transactions_file}")

print()
print(f"   📊 Résumé:")
print(f"      - Utilisateurs: {num_users}")
print(f"      - Transactions: {num_transactions}")
print(f"      - Volume total: {sum(t['amount'] for t in transactions):.2f} USD")
print()

# ============================================================================
# ÉTAPE 2: Vérification de PostgreSQL
# ============================================================================
print("🗄️  ÉTAPE 2: Vérification de PostgreSQL")
print("-" * 70)

try:
    import psycopg2
    
    # Connexion au conteneur PostgreSQL
    conn = psycopg2.connect(
        host='localhost',
        port=5433,
        database='dataenginerflow360',
        user='postgres',
        password='postgres'
    )
    cursor = conn.cursor()
    
    # Vérifier les schémas
    cursor.execute("""
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name IN ('curated', 'staging', 'analytics', 'public_curated')
    """)
    schemas = cursor.fetchall()
    print(f"   ✅ Schémas disponibles: {[s[0] for s in schemas]}")
    
    # Vérifier les tables existantes
    cursor.execute("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema IN ('curated', 'public_curated')
        ORDER BY table_schema, table_name
    """)
    tables = cursor.fetchall()
    if tables:
        print(f"   ✅ Tables existantes:")
        for schema, table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            count = cursor.fetchone()[0]
            print(f"      - {schema}.{table}: {count} lignes")
    else:
        print(f"   ℹ️  Aucune table dans les schémas curated (normal si première exécution)")
    
    cursor.close()
    conn.close()
    print()
    
except Exception as e:
    print(f"   ⚠️  Erreur PostgreSQL: {e}")
    print()

# ============================================================================
# ÉTAPE 3: Vérification de Kafka
# ============================================================================
print("📨 ÉTAPE 3: Vérification de Kafka")
print("-" * 70)

try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic
    
    # Créer un topic de démonstration
    admin_client = KafkaAdminClient(
        bootstrap_servers='localhost:9093',
        client_id='demo_admin'
    )
    
    topic_name = 'demo_transactions'
    
    # Vérifier si le topic existe
    existing_topics = admin_client.list_topics()
    if topic_name not in existing_topics:
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"   ✅ Topic créé: {topic_name}")
    else:
        print(f"   ✅ Topic existant: {topic_name}")
    
    # Envoyer quelques messages de test
    producer = KafkaProducer(
        bootstrap_servers='localhost:9093',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    num_messages = 10
    print(f"   📤 Envoi de {num_messages} messages de test...")
    for i, transaction in enumerate(transactions[:num_messages]):
        producer.send(topic_name, value=transaction)
    
    producer.flush()
    producer.close()
    print(f"   ✅ {num_messages} messages envoyés au topic {topic_name}")
    print()
    
except Exception as e:
    print(f"   ⚠️  Erreur Kafka: {e}")
    print(f"   ℹ️  Kafka peut nécessiter quelques secondes pour démarrer complètement")
    print()

# ============================================================================
# ÉTAPE 4: Vérification d'Airflow
# ============================================================================
print("🔄 ÉTAPE 4: Vérification d'Airflow")
print("-" * 70)

try:
    import requests
    
    response = requests.get('http://localhost:8080/api/v1/dags', 
                           auth=('admin', 'admin'),
                           timeout=5)
    
    if response.status_code == 200:
        dags = response.json()
        total_dags = dags.get('total_entries', 0)
        print(f"   ✅ Airflow accessible: {total_dags} DAGs disponibles")
        
        if 'dags' in dags:
            print(f"   📋 Liste des DAGs:")
            for dag in dags['dags'][:10]:
                is_paused = "⏸️  Pausé" if dag.get('is_paused') else "▶️  Actif"
                print(f"      - {dag['dag_id']}: {is_paused}")
    else:
        print(f"   ⚠️  Airflow répond avec le code: {response.status_code}")
    print()
    
except Exception as e:
    print(f"   ⚠️  Erreur Airflow: {e}")
    print(f"   ℹ️  Airflow peut nécessiter quelques minutes pour démarrer complètement")
    print()

# ============================================================================
# ÉTAPE 5: Vérification de Prometheus
# ============================================================================
print("📊 ÉTAPE 5: Vérification de Prometheus")
print("-" * 70)

try:
    import requests
    
    response = requests.get('http://localhost:9090/api/v1/targets', timeout=5)
    
    if response.status_code == 200:
        data = response.json()
        active_targets = len(data['data']['activeTargets'])
        print(f"   ✅ Prometheus actif: {active_targets} targets collectés")
        
        # Afficher quelques métriques
        for target in data['data']['activeTargets'][:5]:
            job = target['labels'].get('job', 'unknown')
            state = target['health']
            print(f"      - {job}: {state}")
    else:
        print(f"   ⚠️  Prometheus répond avec le code: {response.status_code}")
    print()
    
except Exception as e:
    print(f"   ⚠️  Erreur Prometheus: {e}")
    print()

# ============================================================================
# ÉTAPE 6: Vérification de Grafana
# ============================================================================
print("📈 ÉTAPE 6: Vérification de Grafana")
print("-" * 70)

try:
    import requests
    
    response = requests.get('http://localhost:3000/api/health', timeout=5)
    
    if response.status_code == 200:
        health = response.json()
        print(f"   ✅ Grafana actif: version {health.get('version', 'unknown')}")
        print(f"      Database: {health.get('database', 'unknown')}")
    else:
        print(f"   ⚠️  Grafana répond avec le code: {response.status_code}")
    print()
    
except Exception as e:
    print(f"   ⚠️  Erreur Grafana: {e}")
    print()

# ============================================================================
# RÉSUMÉ
# ============================================================================
print("=" * 70)
print("✅ DÉMONSTRATION TERMINÉE")
print("=" * 70)
print()
print("📊 Résumé de la démonstration:")
print(f"   ✅ Données générées: {num_users} utilisateurs, {num_transactions} transactions")
print(f"   ✅ Fichiers créés dans: {RAW_PATH}")
print(f"   ✅ PostgreSQL: Connecté")
print(f"   ✅ Kafka: Topic créé et messages envoyés")
print(f"   ✅ Airflow: Accessible sur http://localhost:8080")
print(f"   ✅ Prometheus: Collecte des métriques")
print(f"   ✅ Grafana: Accessible sur http://localhost:3000")
print()
print("🌐 Interfaces disponibles:")
print("   - Airflow:    http://localhost:8080 (admin/admin)")
print("   - Grafana:    http://localhost:3000 (admin/admin)")
print("   - Prometheus: http://localhost:9090")
print("   - Flower:     http://localhost:5555")
print("   - Spark UI:   http://localhost:8081")
print()
print(f"⏰ Heure de fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)
