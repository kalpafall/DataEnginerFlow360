#!/usr/bin/env python3
"""
Script de démonstration du pipeline agricole DataEnginerFlow360
Charge les données agricoles et les envoie vers Kafka pour traitement en temps réel
"""
import json
import time
from pathlib import Path
from datetime import datetime
import random

print("=" * 70)
print("🌾 DÉMONSTRATION DU PIPELINE AGRICOLE DATAENGINERFLOW360")
print("=" * 70)
print(f"⏰ Heure de début: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# Chemins des données
DATA_DIR = Path("data_lake/raw/agriculture")

# ============================================================================
# ÉTAPE 1: Chargement des données générées
# ============================================================================
print("📥 ÉTAPE 1: Chargement des données agricoles")
print("-" * 70)

datasets = {}
for filename in ['farms', 'fields', 'crops', 'harvests', 'weather', 'sensors', 'sales']:
    filepath = DATA_DIR / f'{filename}.json'
    if filepath.exists():
        with open(filepath, 'r', encoding='utf-8') as f:
            datasets[filename] = json.load(f)
        print(f"   ✅ {filename}: {len(datasets[filename])} enregistrements chargés")
    else:
        print(f"   ⚠️  {filename}: fichier non trouvé")
        datasets[filename] = []

print()

# ============================================================================
# ÉTAPE 2: Statistiques des données
# ============================================================================
print("📊 ÉTAPE 2: Statistiques des données agricoles")
print("-" * 70)

# Statistiques par type de culture
crops_by_type = {}
for crop in datasets['crops']:
    crop_type = crop['crop_type']
    crops_by_type[crop_type] = crops_by_type.get(crop_type, 0) + 1

print("   📈 Répartition des cultures:")
for crop_type, count in sorted(crops_by_type.items(), key=lambda x: x[1], reverse=True)[:10]:
    print(f"      - {crop_type}: {count} parcelles")

# Statistiques de récolte
total_harvest = sum(h['quantity_tonnes'] for h in datasets['harvests'])
total_revenue = sum(s['total_amount'] for s in datasets['sales'])

print(f"\n   💰 Production et revenus:")
print(f"      - Production totale: {total_harvest:.2f} tonnes")
print(f"      - Revenus totaux: {total_revenue:,.2f} FCFA")
print(f"      - Prix moyen: {total_revenue/total_harvest:.2f} FCFA/tonne")

# Statistiques capteurs
sensors_by_type = {}
for sensor in datasets['sensors']:
    sensor_type = sensor['sensor_type']
    sensors_by_type[sensor_type] = sensors_by_type.get(sensor_type, 0) + 1

print(f"\n   🔬 Capteurs IoT:")
for sensor_type, count in sensors_by_type.items():
    print(f"      - {sensor_type}: {count} lectures")

print()

# ============================================================================
# ÉTAPE 3: Envoi des données capteurs vers Kafka
# ============================================================================
print("📨 ÉTAPE 3: Envoi des données vers Kafka")
print("-" * 70)

try:
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    
    # Créer les topics
    admin_client = KafkaAdminClient(
        bootstrap_servers='localhost:9093',
        client_id='agricultural_demo'
    )
    
    topics_to_create = [
        'sensor_data',
        'weather_updates',
        'harvest_events',
    ]
    
    existing_topics = admin_client.list_topics()
    
    for topic_name in topics_to_create:
        if topic_name not in existing_topics:
            topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            print(f"   ✅ Topic créé: {topic_name}")
        else:
            print(f"   ✅ Topic existant: {topic_name}")
    
    # Créer le producteur
    producer = KafkaProducer(
        bootstrap_servers='localhost:9093',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Envoyer des données capteurs
    num_sensor_messages = min(50, len(datasets['sensors']))
    print(f"\n   📤 Envoi de {num_sensor_messages} lectures de capteurs...")
    for sensor_data in random.sample(datasets['sensors'], num_sensor_messages):
        producer.send('sensor_data', value=sensor_data)
    
    # Envoyer des données météo
    num_weather_messages = min(20, len(datasets['weather']))
    print(f"   📤 Envoi de {num_weather_messages} données météo...")
    for weather_data in random.sample(datasets['weather'], num_weather_messages):
        producer.send('weather_updates', value=weather_data)
    
    # Envoyer des événements de récolte
    num_harvest_messages = min(30, len(datasets['harvests']))
    print(f"   📤 Envoi de {num_harvest_messages} événements de récolte...")
    for harvest_data in random.sample(datasets['harvests'], num_harvest_messages):
        producer.send('harvest_events', value=harvest_data)
    
    producer.flush()
    producer.close()
    
    print(f"\n   ✅ Total: {num_sensor_messages + num_weather_messages + num_harvest_messages} messages envoyés à Kafka")
    print()
    
except Exception as e:
    print(f"   ⚠️  Erreur Kafka: {e}")
    print(f"   ℹ️  Assurez-vous que Kafka est démarré")
    print()

# ============================================================================
# ÉTAPE 4: Vérification PostgreSQL
# ============================================================================
print("🗄️  ÉTAPE 4: Vérification de PostgreSQL")
print("-" * 70)

try:
    import psycopg2
    
    conn = psycopg2.connect(
        host='localhost',
        port=5433,
        database='dataenginerflow360',
        user='postgres',
        password='postgres'
    )
    cursor = conn.cursor()
    
    # Créer un schéma pour l'agriculture si nécessaire
    cursor.execute("CREATE SCHEMA IF NOT EXISTS agriculture")
    conn.commit()
    
    print(f"   ✅ Schéma 'agriculture' créé/vérifié")
    
    # Vérifier les schémas disponibles
    cursor.execute("""
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
        ORDER BY schema_name
    """)
    schemas = cursor.fetchall()
    print(f"   ✅ Schémas disponibles: {[s[0] for s in schemas]}")
    
    cursor.close()
    conn.close()
    print()
    
except Exception as e:
    print(f"   ⚠️  Erreur PostgreSQL: {e}")
    print()

# ============================================================================
# ÉTAPE 5: Exemples de données
# ============================================================================
print("📋 ÉTAPE 5: Exemples de données générées")
print("-" * 70)

if datasets['farms']:
    print("\n   🏡 Exemple d'exploitation:")
    farm = datasets['farms'][0]
    print(f"      ID: {farm['farm_id']}")
    print(f"      Nom: {farm['name']}")
    print(f"      Propriétaire: {farm['owner']}")
    print(f"      Région: {farm['location']['region']}")
    print(f"      Surface: {farm['total_area_ha']} ha")
    print(f"      Type: {farm['farming_type']}")

if datasets['crops']:
    print("\n   🌱 Exemple de culture:")
    crop = datasets['crops'][0]
    print(f"      ID: {crop['crop_id']}")
    print(f"      Type: {crop['crop_type']} ({crop['category']})")
    print(f"      Variété: {crop['variety']}")
    print(f"      Date de semis: {crop['sowing_date']}")
    print(f"      Rendement estimé: {crop['estimated_yield_t_ha']} t/ha")

if datasets['sensors']:
    print("\n   🔬 Exemple de lecture capteur:")
    sensor = datasets['sensors'][0]
    print(f"      ID: {sensor['sensor_id']}")
    print(f"      Type: {sensor['sensor_type']}")
    print(f"      Valeur: {sensor['value']} {sensor['unit']}")
    print(f"      Timestamp: {sensor['timestamp']}")

print()

# ============================================================================
# ÉTAPE 6: Vérification des services
# ============================================================================
print("🔍 ÉTAPE 6: Vérification des services du pipeline")
print("-" * 70)

try:
    import requests
    
    # Airflow
    try:
        response = requests.get('http://localhost:8080/health', timeout=3)
        if response.status_code == 200:
            print(f"   ✅ Airflow: Opérationnel")
        else:
            print(f"   ⚠️  Airflow: Code {response.status_code}")
    except:
        print(f"   ⚠️  Airflow: Non accessible")
    
    # Prometheus
    try:
        response = requests.get('http://localhost:9090/api/v1/targets', timeout=3)
        if response.status_code == 200:
            data = response.json()
            active = len(data['data']['activeTargets'])
            print(f"   ✅ Prometheus: {active} targets actifs")
        else:
            print(f"   ⚠️  Prometheus: Code {response.status_code}")
    except:
        print(f"   ⚠️  Prometheus: Non accessible")
    
    # Grafana
    try:
        response = requests.get('http://localhost:3000/api/health', timeout=3)
        if response.status_code == 200:
            print(f"   ✅ Grafana: Opérationnel")
        else:
            print(f"   ⚠️  Grafana: Code {response.status_code}")
    except:
        print(f"   ⚠️  Grafana: Non accessible")
    
    print()
    
except Exception as e:
    print(f"   ⚠️  Erreur lors de la vérification: {e}")
    print()

# ============================================================================
# RÉSUMÉ
# ============================================================================
print("=" * 70)
print("✅ DÉMONSTRATION AGRICOLE TERMINÉE")
print("=" * 70)
print()
print("📊 Résumé de la démonstration:")
print(f"   ✅ Exploitations: {len(datasets['farms'])}")
print(f"   ✅ Parcelles: {len(datasets['fields'])}")
print(f"   ✅ Cultures: {len(datasets['crops'])}")
print(f"   ✅ Récoltes: {len(datasets['harvests'])}")
print(f"   ✅ Données météo: {len(datasets['weather'])} jours")
print(f"   ✅ Lectures capteurs: {len(datasets['sensors'])}")
print(f"   ✅ Ventes: {len(datasets['sales'])}")
print()
print("🌐 Prochaines étapes:")
print("   1. Consulter Airflow: http://localhost:8080 (admin/admin)")
print("   2. Visualiser dans Grafana: http://localhost:3000 (admin/admin)")
print("   3. Vérifier les métriques: http://localhost:9090")
print("   4. Consommer les messages Kafka:")
print("      docker exec dataflow_kafka kafka-console-consumer \\")
print("        --bootstrap-server localhost:9092 \\")
print("        --topic sensor_data --from-beginning --max-messages 5")
print()
print(f"⏰ Heure de fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)
