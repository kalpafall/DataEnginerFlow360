"""
Module de stockage PostgreSQL pour la couche CURATED (Data Warehouse)
Architecture: PROCESSED -> PostgreSQL + dbt -> CURATED
"""
import logging
from typing import Dict, Any, List, Optional
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresStorage:
    """Classe pour gérer le stockage dans PostgreSQL (Data Warehouse - couche CURATED)"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialise la connexion PostgreSQL
        
        Args:
            config: Configuration contenant les paramètres de connexion
        """
        self.config = config
        self.data_lake_path = config.get('data_lake_path', './data_lake')
        self.engine = self._create_engine()
    
    def _create_engine(self) -> Engine:
        """Crée le moteur SQLAlchemy pour PostgreSQL"""
        db_config = self.config.get('postgres', {})
        
        host = db_config.get('host', 'localhost')
        port = db_config.get('port', 5432)
        database = db_config.get('database', 'dataenginerflow360')
        user = db_config.get('user', 'postgres')
        password = db_config.get('password', 'postgres')
        
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        
        engine = create_engine(connection_string, echo=False)
        logger.info(f"Connexion PostgreSQL établie: {database}@{host}")
        
        return engine
    
    def create_schema(self, schema_name: str = 'curated'):
        """
        Crée un schéma PostgreSQL pour la couche CURATED
        
        Args:
            schema_name: Nom du schéma
        """
        with self.engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            conn.commit()
        logger.info(f"Schéma créé: {schema_name}")
    
    def load_from_processed(
        self,
        dataset_name: str,
        table_name: str,
        schema: str = 'curated',
        if_exists: str = 'replace'
    ) -> int:
        """
        Charge les données depuis la couche PROCESSED vers PostgreSQL
        
        Args:
            dataset_name: Nom du dataset dans PROCESSED
            table_name: Nom de la table PostgreSQL
            schema: Schéma PostgreSQL
            if_exists: Action si la table existe (replace, append, fail)
            
        Returns:
            Nombre de lignes insérées
        """
        processed_path = Path(self.data_lake_path) / 'processed' / dataset_name
        
        if not processed_path.exists():
            raise FileNotFoundError(f"Dataset PROCESSED non trouvé: {processed_path}")
        
        logger.info(f"Chargement depuis PROCESSED: {processed_path} -> {schema}.{table_name}")
        
        # Lire tous les fichiers Parquet
        parquet_files = list(processed_path.rglob('*.parquet'))
        
        if not parquet_files:
            raise FileNotFoundError(f"Aucun fichier Parquet trouvé dans {processed_path}")
        
        # Charger et combiner les DataFrames
        dfs = [pd.read_parquet(file) for file in parquet_files]
        df = pd.concat(dfs, ignore_index=True)
        
        logger.info(f"Données lues: {len(df)} lignes, {len(df.columns)} colonnes")
        
        # Créer le schéma si nécessaire
        self.create_schema(schema)
        
        # Charger dans PostgreSQL
        df.to_sql(
            name=table_name,
            con=self.engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method='multi',
            chunksize=10000
        )
        
        logger.info(f"Données chargées dans {schema}.{table_name}: {len(df)} lignes")
        
        return len(df)
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Exécute une requête SQL et retourne les résultats
        
        Args:
            query: Requête SQL
            
        Returns:
            DataFrame avec les résultats
        """
        logger.info(f"Exécution requête SQL")
        
        with self.engine.connect() as conn:
            result = pd.read_sql(text(query), conn)
        
        logger.info(f"Requête exécutée: {len(result)} lignes retournées")
        
        return result
    
    def create_indexes(
        self,
        table_name: str,
        columns: List[str],
        schema: str = 'curated'
    ):
        """
        Crée des index sur une table pour optimiser les requêtes
        
        Args:
            table_name: Nom de la table
            columns: Liste des colonnes à indexer
            schema: Schéma PostgreSQL
        """
        with self.engine.connect() as conn:
            for column in columns:
                index_name = f"idx_{table_name}_{column}"
                query = f"""
                    CREATE INDEX IF NOT EXISTS {index_name}
                    ON {schema}.{table_name} ({column})
                """
                conn.execute(text(query))
                logger.info(f"Index créé: {index_name}")
            conn.commit()
    
    def create_materialized_view(
        self,
        view_name: str,
        query: str,
        schema: str = 'curated'
    ):
        """
        Crée une vue matérialisée pour des requêtes fréquentes
        
        Args:
            view_name: Nom de la vue
            query: Requête SQL de la vue
            schema: Schéma PostgreSQL
        """
        with self.engine.connect() as conn:
            # Supprimer si existe
            conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {schema}.{view_name}"))
            
            # Créer la vue
            full_query = f"""
                CREATE MATERIALIZED VIEW {schema}.{view_name} AS
                {query}
            """
            conn.execute(text(full_query))
            conn.commit()
        
        logger.info(f"Vue matérialisée créée: {schema}.{view_name}")
    
    def refresh_materialized_view(self, view_name: str, schema: str = 'curated'):
        """
        Rafraîchit une vue matérialisée
        
        Args:
            view_name: Nom de la vue
            schema: Schéma PostgreSQL
        """
        with self.engine.connect() as conn:
            conn.execute(text(f"REFRESH MATERIALIZED VIEW {schema}.{view_name}"))
            conn.commit()
        
        logger.info(f"Vue matérialisée rafraîchie: {schema}.{view_name}")
    
    def get_table_info(self, schema: str = 'curated') -> pd.DataFrame:
        """
        Récupère les informations sur les tables du schéma
        
        Args:
            schema: Schéma PostgreSQL
            
        Returns:
            DataFrame avec les informations des tables
        """
        query = f"""
            SELECT 
                table_name,
                pg_size_pretty(pg_total_relation_size(quote_ident(table_schema) || '.' || quote_ident(table_name))) as size
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            ORDER BY table_name
        """
        
        return self.execute_query(query)
    
    def close(self):
        """Ferme la connexion"""
        self.engine.dispose()
        logger.info("Connexion PostgreSQL fermée")


class WarehouseBuilder:
    """Classe pour construire le Data Warehouse avec des tables analytiques"""
    
    def __init__(self, storage: PostgresStorage):
        self.storage = storage
    
    def build_user_analytics(self):
        """Crée une table analytique pour les utilisateurs"""
        logger.info("Construction table analytique: user_analytics")
        
        # Cette requête suppose que les tables users et transactions existent
        query = """
            SELECT 
                u.user_id,
                u.name,
                u.email,
                u.age,
                u.age_group,
                u.country,
                COUNT(DISTINCT t.transaction_id) as total_transactions,
                COALESCE(SUM(t.amount), 0) as total_spent,
                COALESCE(AVG(t.amount), 0) as avg_transaction_amount,
                COALESCE(MAX(t.transaction_date), u.registration_date) as last_transaction_date
            FROM curated.users_fake u
            LEFT JOIN curated.transactions_fake t ON u.user_id = t.user_id
            GROUP BY u.user_id, u.name, u.email, u.age, u.age_group, u.country, u.registration_date
        """
        
        try:
            self.storage.create_materialized_view('user_analytics', query)
            self.storage.create_indexes('user_analytics', ['user_id', 'country', 'age_group'])
        except Exception as e:
            logger.warning(f"Impossible de créer user_analytics: {e}")
    
    def build_transaction_summary(self):
        """Crée une table de résumé des transactions"""
        logger.info("Construction table analytique: transaction_summary")
        
        query = """
            SELECT 
                transaction_year,
                transaction_month,
                category,
                status,
                COUNT(*) as transaction_count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount,
                MIN(amount) as min_amount,
                MAX(amount) as max_amount
            FROM curated.transactions_fake
            GROUP BY transaction_year, transaction_month, category, status
            ORDER BY transaction_year DESC, transaction_month DESC
        """
        
        try:
            self.storage.create_materialized_view('transaction_summary', query)
        except Exception as e:
            logger.warning(f"Impossible de créer transaction_summary: {e}")


if __name__ == '__main__':
    config = {
        'data_lake_path': './data_lake',
        'postgres': {
            'host': 'localhost',
            'port': 5432,
            'database': 'dataenginerflow360',
            'user': 'postgres',
            'password': 'postgres'
        }
    }
    
    storage = PostgresStorage(config)
    
    try:
        # Charger les données depuis PROCESSED
        storage.load_from_processed('users_fake', 'users_fake')
        storage.load_from_processed('transactions_fake', 'transactions_fake')
        
        # Créer des index
        storage.create_indexes('users_fake', ['user_id', 'email', 'country'])
        storage.create_indexes('transactions_fake', ['user_id', 'transaction_date', 'status'])
        
        # Construire les tables analytiques
        builder = WarehouseBuilder(storage)
        builder.build_user_analytics()
        builder.build_transaction_summary()
        
        # Afficher les tables
        print("\n=== Tables du Data Warehouse ===")
        print(storage.get_table_info())
        
    finally:
        storage.close()
