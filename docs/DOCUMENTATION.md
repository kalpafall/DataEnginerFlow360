# 📚 Documentation DataEnginerFlow360

## Table des Matières

- [Architecture](#architecture)
- [Installation](#installation)
- [Configuration](#configuration)
- [Utilisation](#utilisation)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## Architecture

### Vue d'Ensemble

```
┌─────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                          │
│  • Kafka Streaming                                          │
│  • API Batch (REST)                                         │
│  • File Batch (CSV, JSON, Parquet)                         │
│  • Fake Data Generation (Faker)                            │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│                   PROCESSING LAYER                          │
│  • PySpark Structured Streaming (Real-time)                │
│  • PySpark Batch (Complex transformations)                 │
│  • dbt (Data Warehouse - Star Schema)                      │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│                  ORCHESTRATION (Airflow)                    │
│  • batch_ingestion_dag (2:00 AM)                           │
│  • pyspark_transformation_dag (3:00 AM)                    │
│  • dbt_dag (4:00 AM)                                       │
│  • master_pipeline_dag (Orchestration)                     │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│              MONITORING (Prometheus + Grafana)              │
│  • Prometheus (Metrics collection)                         │
│  • Grafana (Visualization)                                 │
│  • Exporters (PostgreSQL, Kafka, Airflow, Docker)         │
│  • Custom Metrics (Data Quality)                           │
└─────────────────────────────────────────────────────────────┘
```

## Installation

### Prérequis

- Docker & Docker Compose
- Python 3.12+
- Java 11 (pour PySpark)
- Git

### Étapes d'Installation

1. **Cloner le repository**

```bash
git clone https://github.com/kalpafall/DataEnginerFlow360.git
cd DataEnginerFlow360
```

2. **Créer l'environnement virtuel**

```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate  # Windows
```

3. **Installer les dépendances**

```bash
pip install -r requirements.txt
```

4. **Configurer Java (pour PySpark)**

```bash
# Ubuntu/Debian
sudo apt install openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

5. **Démarrer les services Docker**

```bash
cd docker
docker-compose up -d
```

## Configuration

### PostgreSQL

Fichier: `transformation/dbt/profiles.yml`

```yaml
dataflow360:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: postgres
      password: 1234 # Changez ceci
      port: 5432
      dbname: dataenginerflow360
```

### Prometheus

Fichier: `docker/prometheus/prometheus.yml`

- Scrape interval: 15s
- Targets: postgres-exporter, statsd-exporter, kafka-exporter, cadvisor

### Airflow

- Webserver: http://localhost:8080
- Credentials: admin/admin

## Utilisation

### Test Rapide du Pipeline

```bash
python3 test_pipeline.py
```

### Exécuter dbt

```bash
cd transformation/dbt
dbt run
dbt test
```

### Exécuter PySpark

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
python3 transformation/spark_transformation.py \
    --dataset transactions_fake \
    --type transactions \
    --aggregate
```

### Démarrer Airflow

```bash
cd docker
docker-compose up -d airflow-webserver airflow-scheduler
```

## Monitoring

### Accès aux Interfaces

| Service    | URL                   | Credentials |
| ---------- | --------------------- | ----------- |
| Grafana    | http://localhost:3000 | admin/admin |
| Prometheus | http://localhost:9090 | -           |
| Airflow    | http://localhost:8080 | admin/admin |
| cAdvisor   | http://localhost:8082 | -           |

### Métriques Disponibles

#### Airflow

- `airflow_dag_run_duration_seconds` - Durée des DAGs
- `airflow_dag_run_success_total` - Succès des DAGs
- `airflow_dag_run_failed_total` - Échecs des DAGs

#### PostgreSQL

- `pg_stat_database_numbackends` - Connexions actives
- `pg_stat_database_xact_commit` - Transactions committées

#### Kafka

- `kafka_consumergroup_lag` - Consumer lag
- `kafka_topic_partition_current_offset` - Offset actuel

#### Docker

- `container_cpu_usage_seconds_total` - CPU usage
- `container_memory_usage_bytes` - Mémoire utilisée

## Troubleshooting

### PostgreSQL: Port 5432 déjà utilisé

```bash
# Arrêter PostgreSQL local
sudo systemctl stop postgresql

# Ou utiliser un autre port dans docker-compose.yml
ports:
  - "5433:5432"
```

### Java not found (PySpark)

```bash
# Installer Java 11
sudo apt install openjdk-11-jdk

# Définir JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

### dbt: Connection failed

```bash
# Vérifier PostgreSQL
sudo -u postgres psql -d dataenginerflow360

# Vérifier le mot de passe dans profiles.yml
```

### Docker: Permission denied

```bash
# Ajouter l'utilisateur au groupe docker
sudo usermod -aG docker $USER
newgrp docker
```

## Structure des Données

### Data Lake

```
data_lake/
├── raw/           # Données brutes
├── processed/     # Données transformées (PySpark)
└── curated/       # Données finales (PostgreSQL)
```

### Star Schema (dbt)

- **Dimensions**: dim_users
- **Facts**: fact_transactions

## Performance

### Optimisations Recommandées

1. **PySpark**: Ajuster `spark.executor.memory`
2. **PostgreSQL**: Créer des index sur les colonnes fréquemment requêtées
3. **Kafka**: Augmenter `num.partitions` pour le parallélisme
4. **Airflow**: Ajuster `parallelism` et `dag_concurrency`

## Sécurité

### Bonnes Pratiques

- ✅ Changer les mots de passe par défaut
- ✅ Utiliser des secrets pour les credentials
- ✅ Activer SSL pour les connexions
- ✅ Limiter l'accès réseau avec des firewalls
- ✅ Sauvegarder régulièrement les données

## Support

- **Issues**: https://github.com/kalpafall/DataEnginerFlow360/issues
- **Email**: fallyama2003@gmail.com

## Licence

MIT License - voir [LICENSE](../LICENSE)
