import logging
from typing import Dict, Any, Optional
from datetime import datetime
from pathlib import Path
import json
import pandas as pd
import requests
from faker import Faker

from sources.mongodb_source import MongoDBSource
from sources.cassandra_source import CassandraSource
from sources.kafka_source import KafkaSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataIngestion:
    """Classe pour gérer l'ingestion de données depuis différentes sources"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.data_lake_path = config.get("data_lake_path", "./data_lake")
        self._ensure_data_lake_structure()

    def _ensure_data_lake_structure(self):
        for layer in ["raw", "processed", "curated"]:
            path = Path(self.data_lake_path) / layer
            path.mkdir(parents=True, exist_ok=True)
        logger.info(f"✅ Structure du data lake prête: {self.data_lake_path}")

    # -----------------------------
    # 🍃 INGESTION MONGODB
    # -----------------------------
    def ingest_from_mongodb(self, dataset_name: str, query: dict = None, limit: int = None) -> str:
        try:
            mongo_cfg = self.config.get("mongodb", {})
            host = mongo_cfg.get("host", "localhost")
            port = mongo_cfg.get("port", 27017)
            uri = mongo_cfg.get("uri")
            database = mongo_cfg.get("database")
            collection = mongo_cfg.get("collection")

            source = MongoDBSource(
                database=database,
                collection=collection,
                uri=uri,
                host=host,
                port=port
            )

            df = source.fetch(query=query)
            if limit:
                df = df.head(limit)

            df["_ingestion_timestamp"] = datetime.now()
            df["_source"] = "mongodb"

            output_path = self._save_to_raw(df, dataset_name, "mongodb")
            logger.info(f"✅ MongoDB ingéré ({len(df)} lignes) -> {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"❌ Erreur MongoDB: {e}")
            raise

    # -----------------------------
    # 🌾 INGESTION CASSANDRA
    # -----------------------------
    def ingest_from_cassandra(self, dataset_name: str, query: str) -> str:
        try:
            cass_cfg = self.config.get("cassandra", {})
            hosts = cass_cfg.get("hosts", ["localhost"])
            port = cass_cfg.get("port", 9042)
            keyspace = cass_cfg.get("keyspace", "agri_keyspace")
            username = cass_cfg.get("username")
            password = cass_cfg.get("password")

            source = CassandraSource(
                hosts=hosts,
                port=port,
                keyspace=keyspace,
                username=username,
                password=password,
                create_keyspace=True,
                replication_factor=1
            )

            df = source.fetch(query)
            df["_ingestion_timestamp"] = datetime.now()
            df["_source"] = "cassandra"

            output_path = self._save_to_raw(df, dataset_name, "cassandra")
            logger.info(f"✅ Cassandra ingéré ({len(df)} lignes) -> {output_path}")
        except Exception as e:
            logger.error(f"❌ Erreur Cassandra: {e}")
            raise

    # -----------------------------
    # 🎧 INGESTION KAFKA
    # -----------------------------
    def ingest_from_kafka(self, dataset_name: str, topic: str, bootstrap_servers: list = None, max_messages: int = 100) -> str:
        try:
            kafka_cfg = self.config.get("kafka", {})
            if not bootstrap_servers:
                bootstrap_servers = kafka_cfg.get("bootstrap_servers", ["localhost:9092"])
            
            source = KafkaSource(
                bootstrap_servers=bootstrap_servers,
                topic=topic,
                group_id=f"ingestion_group_{dataset_name}"
            )

            df = source.fetch(max_messages=max_messages)
            
            if df.empty:
                logger.warning("⚠️ Aucun message récupéré depuis Kafka")
                return None

            df["_ingestion_timestamp"] = datetime.now()
            df["_source"] = "kafka"

            output_path = self._save_to_raw(df, dataset_name, "kafka")
            logger.info(f"✅ Kafka ingéré ({len(df)} messages) -> {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"❌ Erreur Kafka: {e}")
            raise

    # -----------------------------
    # 🪣 SAUVEGARDE PARQUET
    # -----------------------------
    def _save_to_raw(self, df: pd.DataFrame, dataset_name: str, source_type: str) -> str:
        date_partition = datetime.now().strftime("year=%Y/month=%m/day=%d")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        output_dir = Path(self.data_lake_path) / "raw" / dataset_name / date_partition
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / f"{dataset_name}_{source_type}_{timestamp}.parquet"
        df.to_parquet(output_file, index=False, compression="snappy")

        metadata = {
            "dataset_name": dataset_name,
            "source_type": source_type,
            "ingestion_timestamp": datetime.now().isoformat(),
            "row_count": len(df),
            "columns": df.columns.tolist(),
            "file_path": str(output_file),
            "partition": date_partition,
        }

        metadata_file = output_file.with_suffix(".json")
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)

        return str(output_file)

# -------------------------------------------------------------------------
# 🚀 EXÉCUTION DE DÉMO
# -------------------------------------------------------------------------
if __name__ == "__main__":
    config = {
        "data_lake_path": "./data_lake",
        "mongodb": {
            "host": "localhost",
            "port": 27018,  # Port Docker exposé
            "database": "agri_db",
            "collection": "agri_data_fake"
        },
        "cassandra": {
            "hosts": ["localhost"],
            "port": 9042,  # Port Docker
            "keyspace": "agri_keyspace"
        }
    }

    ingestion = DataIngestion(config)

    print("\n=== 🍃 Test MongoDB ===")
    try:
        ingestion.ingest_from_mongodb("mongo_data")
    except Exception as e:
        print("MongoDB Error:", e)

    print("\n=== 🌾 Test Cassandra ===")
    try:
        ingestion.ingest_from_cassandra("cassandra_data", "SELECT * FROM farms LIMIT 20;")
    except Exception as e:
        print("Cassandra Error:", e)

