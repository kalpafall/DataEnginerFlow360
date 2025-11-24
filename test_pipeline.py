"""
Script simple pour tester le pipeline DataEnginerFlow360
"""
import os
import subprocess

# Configuration
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-amd64'

print("=" * 70)
print("🚀 TEST DU PIPELINE DATAENGINERFLOW360")
print("=" * 70)

# 1. Test dbt
print("\n📊 ÉTAPE 1: Test des modèles dbt...")
print("-" * 70)

result = subprocess.run(
    ['dbt', 'run', '--profiles-dir', '.'],
    cwd='transformation/dbt',
    capture_output=True,
    text=True
)

if result.returncode == 0:
    print("✅ Modèles dbt exécutés avec succès!")
    for line in result.stdout.split('\n'):
        if 'OK created' in line or 'PASS' in line or 'Done.' in line:
            print(f"   {line.strip()}")
else:
    print(f"❌ Erreur dbt:")
    print(result.stderr)

# 2. Test dbt tests
print("\n🧪 ÉTAPE 2: Exécution des tests dbt...")
print("-" * 70)

result = subprocess.run(
    ['dbt', 'test', '--profiles-dir', '.'],
    cwd='transformation/dbt',
    capture_output=True,
    text=True
)

if result.returncode == 0:
    print("✅ Tests dbt réussis!")
    for line in result.stdout.split('\n'):
        if 'PASS' in line or 'Done.' in line:
            print(f"   {line.strip()}")
else:
    print(f"⚠️  Certains tests ont échoué")

# 3. Vérifier PostgreSQL
print("\n💾 ÉTAPE 3: Vérification de PostgreSQL...")
print("-" * 70)

try:
    import psycopg2
    
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='dataenginerflow360',
        user='postgres',
        password='1234'
    )
    cursor = conn.cursor()
    
    # Vérifier dim_users
    cursor.execute("SELECT COUNT(*) FROM public_curated.dim_users")
    users_count = cursor.fetchone()[0]
    print(f"✅ Table dim_users: {users_count} lignes")
    
    # Vérifier fact_transactions
    cursor.execute("SELECT COUNT(*) FROM public_curated.fact_transactions")
    transactions_count = cursor.fetchone()[0]
    print(f"✅ Table fact_transactions: {transactions_count} lignes")
    
    # Afficher un échantillon
    cursor.execute("SELECT * FROM public_curated.dim_users LIMIT 3")
    print("\n📋 Échantillon dim_users:")
    for row in cursor.fetchall():
        print(f"   User ID: {row[0]}, Email: {row[1]}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Erreur PostgreSQL: {e}")

# 4. Vérifier Prometheus
print("\n📈 ÉTAPE 4: Vérification de Prometheus...")
print("-" * 70)

try:
    import requests
    
    response = requests.get('http://localhost:9090/api/v1/targets', timeout=2)
    if response.status_code == 200:
        data = response.json()
        active_targets = len(data['data']['activeTargets'])
        up_targets = sum(1 for t in data['data']['activeTargets'] if t['health'] == 'up')
        print(f"✅ Prometheus actif: {up_targets}/{active_targets} targets UP")
    else:
        print(f"⚠️  Prometheus: Status {response.status_code}")
except Exception as e:
    print(f"⚠️  Prometheus non accessible: {e}")

# 5. Vérifier les services Docker
print("\n🐳 ÉTAPE 5: Services Docker...")
print("-" * 70)

result = subprocess.run(
    ['docker', 'ps', '--format', '{{.Names}}\t{{.Status}}'],
    capture_output=True,
    text=True
)

if result.returncode == 0:
    services = [line for line in result.stdout.split('\n') if 'dataflow' in line]
    print(f"✅ {len(services)} services Docker actifs:")
    for service in services[:7]:  # Afficher les 7 premiers
        if service:
            print(f"   {service}")
else:
    print("⚠️  Impossible de lister les services Docker")

# Résumé final
print("\n" + "=" * 70)
print("✅ TESTS TERMINÉS!")
print("=" * 70)
print("\n📊 Accès aux interfaces:")
print("   • Prometheus: http://localhost:9090")
print("   • Grafana:    http://localhost:3000 (admin/admin)")
print("   • cAdvisor:   http://localhost:8082")
print("\n💡 Prochaines étapes:")
print("   1. Créer les dashboards Grafana")
print("   2. Démarrer Airflow: cd docker && docker-compose up -d airflow-webserver")
print("   3. Tester les DAGs Airflow")
print("=" * 70)
