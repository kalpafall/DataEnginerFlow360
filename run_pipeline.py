"""
Script de test simple pour exécuter le pipeline end-to-end
"""
import os
import sys
from pathlib import Path

# Configuration
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-amd64'

print("=" * 60)
print("🚀 DÉMARRAGE DU PIPELINE DATAENGINERFLOW360")
print("=" * 60)

# 1. Générer des données de test
print("\n📥 ÉTAPE 1: Génération de données de test...")
sys.path.insert(0, str(Path(__file__).parent))

from ingestion.data_ingestion import DataIngestion

config = {
    'data_lake_path': 'data_lake',
    'postgres': {
        'host': 'localhost',
        'port': 5432,
        'database': 'dataenginerflow360',
        'user': 'postgres',
        'password': '1234'
    }
}

ingestion = DataIngestion(config)

# Générer des données fake
print("   Génération de 100 utilisateurs...")
users_path = ingestion.generate_fake_data(
    dataset_name='users_test',
    num_records=100,
    data_type='users'
)
print(f"   ✅ Utilisateurs générés: {users_path}")

print("   Génération de 500 transactions...")
transactions_path = ingestion.generate_fake_data(
    dataset_name='transactions_test',
    num_records=500,
    data_type='transactions'
)
print(f"   ✅ Transactions générées: {transactions_path}")

# 2. Transformer avec dbt
print("\n🔄 ÉTAPE 2: Transformation avec dbt...")
import subprocess

result = subprocess.run(
    ['dbt', 'run', '--profiles-dir', '.'],
    cwd='transformation/dbt',
    capture_output=True,
    text=True
)

if result.returncode == 0:
    print("   ✅ Modèles dbt exécutés avec succès")
    # Compter les lignes dans le output
    for line in result.stdout.split('\n'):
        if 'PASS' in line or 'OK' in line:
            print(f"   {line.strip()}")
else:
    print(f"   ❌ Erreur dbt: {result.stderr}")

# 3. Vérifier les données dans PostgreSQL
print("\n📊 ÉTAPE 3: Vérification des données...")
import psycopg2

try:
    conn = psycopg2.connect(**config['postgres'])
    cursor = conn.cursor()
    
    # Compter les lignes dans dim_users
    cursor.execute("SELECT COUNT(*) FROM public_curated.dim_users")
    users_count = cursor.fetchone()[0]
    print(f"   ✅ dim_users: {users_count} lignes")
    
    # Compter les lignes dans fact_transactions
    cursor.execute("SELECT COUNT(*) FROM public_curated.fact_transactions")
    transactions_count = cursor.fetchone()[0]
    print(f"   ✅ fact_transactions: {transactions_count} lignes")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"   ⚠️  Erreur PostgreSQL: {e}")

# 4. Vérifier Prometheus
print("\n📈 ÉTAPE 4: Vérification du monitoring...")
import requests

try:
    response = requests.get('http://localhost:9090/api/v1/targets')
    if response.status_code == 200:
        data = response.json()
        active_targets = len(data['data']['activeTargets'])
        print(f"   ✅ Prometheus: {active_targets} targets actifs")
    else:
        print(f"   ⚠️  Prometheus non accessible")
except Exception as e:
    print(f"   ⚠️  Erreur Prometheus: {e}")

print("\n" + "=" * 60)
print("✅ PIPELINE TERMINÉ AVEC SUCCÈS!")
print("=" * 60)
print("\n📊 Prochaines étapes:")
print("   1. Accéder à Grafana: http://localhost:3000")
print("   2. Accéder à Prometheus: http://localhost:9090")
print("   3. Démarrer Airflow (optionnel)")
print("=" * 60)
