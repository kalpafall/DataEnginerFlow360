import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import logging
from typing import Optional, List
import uuid  # nécessaire pour vérifier les UUID

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class CassandraSource:
    """Source d'extraction depuis Cassandra"""

    def __init__(
        self,
        hosts: List[str],
        keyspace: str,
        port: int = 9042,
        username: Optional[str] = None,
        password: Optional[str] = None,
        create_keyspace: bool = False,
        replication_factor: int = 1
    ):
        self.hosts = hosts
        self.port = port
        self.keyspace = keyspace

        try:
            if username and password:
                auth = PlainTextAuthProvider(username=username, password=password)
                self.cluster = Cluster(hosts, port=self.port, auth_provider=auth)
            else:
                self.cluster = Cluster(hosts, port=self.port)

            self.session = self.cluster.connect()

            # Création automatique du keyspace si demandé
            if create_keyspace:
                cql = f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {replication_factor}}};
                """
                self.session.execute(cql)
                logger.info(f"🍂 Keyspace vérifié/créé: {keyspace}")

            self.session.set_keyspace(self.keyspace)
            logger.info(f"🍂 Connexion Cassandra OK: keyspace={keyspace}")

        except Exception as e:
            logger.error(f"❌ Erreur connexion Cassandra: {e}")
            raise

    def fetch(self, query: str) -> pd.DataFrame:
        """Exécute une requête CQL et retourne un DataFrame, convertissant les UUID en string"""
        try:
            stmt = SimpleStatement(query)
            rows = self.session.execute(stmt)
            df = pd.DataFrame(list(rows))

            # Conversion des UUID en string pour compatibilité Parquet
            for col in df.columns:
                if not df[col].empty and isinstance(df[col].dropna().iloc[0], uuid.UUID):
                    df[col] = df[col].astype(str)

            logger.info(f"📥 {len(df)} lignes récupérées depuis Cassandra")
            return df
        except Exception as e:
            logger.error(f"❌ Erreur fetch Cassandra: {e}")
            raise
