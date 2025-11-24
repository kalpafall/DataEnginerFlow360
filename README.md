# DataEnginerFlow360 🚀

[![Python](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/pyspark-3.5.0-orange.svg)](https://spark.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.10.15-FF694B.svg)](https://www.getdbt.com/)
[![Docker](https://img.shields.io/badge/docker-ready-brightgreen.svg)](https://www.docker.com/)
[![Airflow](https://img.shields.io/badge/airflow-orchestration-017CEE.svg)](https://airflow.apache.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-production%20ready-success.svg)]()

Pipeline de données end-to-end avec ingestion, transformation, orchestration et monitoring.

## 📊 Architecture

```
Ingestion → Processing → Orchestration → Monitoring
   ↓           ↓            ↓              ↓
 Kafka     PySpark       Airflow      Prometheus
           dbt                        Grafana
```

## ✨ Fonctionnalités

- **Ingestion**: Batch (API, fichiers) et Streaming (Kafka)
- **Processing**:
  - PySpark Structured Streaming (temps réel)
  - PySpark Batch (transformations complexes)
  - dbt (Data Warehouse avec star schema)
- **Orchestration**: 4 DAGs Airflow avec dépendances
- **Monitoring**: Prometheus + Grafana + Exporters
- **Tests**: Tests de qualité dbt (7/7 PASS)

## 🚀 Démarrage Rapide

### Prérequis

- Docker & Docker Compose
- Python 3.12+
- Java 11 (pour PySpark)

### Installation

```bash
# Cloner le repo
git clone https://github.com/VOTRE_USERNAME/DataEnginerFlow360.git
cd DataEnginerFlow360

# Créer l'environnement virtuel
python3 -m venv venv
source venv/bin/activate

# Installer les dépendances
pip install -r requirements.txt

# Démarrer les services
cd docker
docker-compose up -d
```

### Test du Pipeline

```bash
# Tester dbt
cd transformation/dbt
dbt run
dbt test

# Tester le pipeline complet
python3 test_pipeline.py
```

## 📁 Structure du Projet

```
DataEnginerFlow360/
├── ingestion/              # Scripts d'ingestion
│   ├── data_ingestion.py
│   └── sources/
├── transformation/         # Transformations
│   ├── streaming_processing.py  # PySpark Streaming
│   ├── spark_transformation.py  # PySpark Batch
│   └── dbt/                     # Modèles dbt
│       ├── models/
│       │   └── marts/
│       │       ├── dim_users.sql
│       │       └── fact_transactions.sql
│       └── profiles.yml
├── orchestration/dags/     # DAGs Airflow
│   ├── batch_ingestion_dag.py
│   ├── pyspark_transformation_dag.py
│   ├── dbt_dag.py
│   └── master_pipeline_dag.py
├── monitoring/             # Monitoring
│   └── metrics_collector.py
├── docker/                 # Configuration Docker
│   ├── docker-compose.yml
│   ├── prometheus/
│   │   ├── prometheus.yml
│   │   └── alert_rules.yml
│   └── statsd_mapping.yml
└── data_lake/             # Stockage des données
    ├── raw/
    ├── processed/
    └── curated/
```

## 🌐 Interfaces

| Service    | URL                   | Credentials |
| ---------- | --------------------- | ----------- |
| Grafana    | http://localhost:3000 | admin/admin |
| Prometheus | http://localhost:9090 | -           |
| Airflow    | http://localhost:8080 | admin/admin |
| cAdvisor   | http://localhost:8082 | -           |

## 📊 Dashboards Grafana

1. **Services Monitoring**: État des services, connexions PostgreSQL
2. **Data Quality**: Métriques de qualité, tests dbt
3. **Infrastructure**: CPU, mémoire, disque des conteneurs

## 🔧 Configuration

### PostgreSQL

- Host: localhost
- Port: 5432
- Database: dataenginerflow360
- User: postgres
- Password: 1234

### Kafka

- Bootstrap: localhost:9093

### Prometheus

- Scrape interval: 15s
- Targets: postgres-exporter, statsd-exporter, kafka-exporter, cadvisor

## 📈 Métriques Collectées

- **Airflow**: DAG duration, success/failure, task metrics
- **PostgreSQL**: Connexions, transactions, performance
- **Kafka**: Topics, consumer lag
- **Docker**: CPU, mémoire, réseau
- **Data Quality**: Lignes traitées, erreurs, fraîcheur

## 🧪 Tests

```bash
# Tests dbt
cd transformation/dbt
dbt test

# Test du pipeline complet
python3 test_pipeline.py
```

## 📝 DAGs Airflow

1. **batch_ingestion**: Ingestion quotidienne (2:00 AM)
2. **pyspark_transformation**: Transformations PySpark (3:00 AM)
3. **dbt_transformation**: Transformations dbt (4:00 AM)
4. **master_data_pipeline**: Orchestration globale

## 🛠️ Technologies

- **Ingestion**: Kafka, Python, Faker
- **Processing**: PySpark 3.5.0, dbt 1.10.15
- **Orchestration**: Apache Airflow
- **Storage**: PostgreSQL, Data Lake (Parquet)
- **Monitoring**: Prometheus, Grafana
- **Containerization**: Docker, Docker Compose

## 📚 Documentation

- [Walkthrough](docs/walkthrough.md)
- [Deployment Report](docs/deployment_report.md)
- [Implementation Plan](docs/implementation_plan.md)

## 🤝 Contribution

Les contributions sont les bienvenues! N'hésitez pas à ouvrir une issue ou une pull request.

## 📄 Licence

MIT License

## 👤 Auteur

Mariama - [GitHub](https://github.com/VOTRE_USERNAME)

## 🎯 Statut du Projet

✅ **Production Ready** - Tous les composants sont testés et opérationnels

---

**⭐ Si ce projet vous a été utile, n'hésitez pas à lui donner une étoile!**
