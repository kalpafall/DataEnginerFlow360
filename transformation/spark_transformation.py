"""
Module de transformation PySpark pour nettoyer et enrichir les données
Architecture: RAW -> PySpark + Hive -> PROCESSED
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkTransformation:
    """Classe pour les transformations PySpark sur les données du data lake"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialise Spark et la configuration
        
        Args:
            config: Configuration contenant les paramètres Spark et chemins
        """
        self.config = config
        self.data_lake_path = config.get('data_lake_path', './data_lake')
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self) -> SparkSession:
        """Crée une session Spark avec configuration Hive"""
        spark = SparkSession.builder \
            .appName("DataEnginerFlow360_Transformation") \
            .config("spark.sql.warehouse.dir", f"{self.data_lake_path}/hive_warehouse") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.sql.adaptive.enabled", "true") \
            .enableHiveSupport() \
            .getOrCreate()
        
        logger.info(f"Spark session créée: {spark.version}")
        return spark
    
    def read_from_raw(self, dataset_name: str) -> DataFrame:
        """
        Lit les données depuis la couche RAW
        
        Args:
            dataset_name: Nom du dataset
            
        Returns:
            DataFrame Spark
        """
        raw_path = f"{self.data_lake_path}/raw/{dataset_name}"
        logger.info(f"Lecture depuis RAW: {raw_path}")
        
        try:
            df = self.spark.read.parquet(f"{raw_path}/**/*.parquet")
            logger.info(f"Données lues: {df.count()} lignes, {len(df.columns)} colonnes")
            return df
        except Exception as e:
            logger.error(f"Erreur lecture RAW: {e}")
            raise
    
    def clean_data(self, df: DataFrame, dataset_type: str = 'generic') -> DataFrame:
        """
        Nettoie les données (suppression doublons, valeurs nulles, validation)
        
        Args:
            df: DataFrame source
            dataset_type: Type de dataset (users, transactions, logs, generic)
            
        Returns:
            DataFrame nettoyé
        """
        logger.info(f"Nettoyage des données pour type: {dataset_type}")
        
        # Suppression des doublons
        initial_count = df.count()
        df = df.dropDuplicates()
        duplicates_removed = initial_count - df.count()
        logger.info(f"Doublons supprimés: {duplicates_removed}")
        
        if dataset_type == 'users':
            # Nettoyage spécifique aux users
            df = df.filter(F.col('email').isNotNull()) \
                   .filter(F.col('email').rlike(r'^[\w\.-]+@[\w\.-]+\.\w+$')) \
                   .filter(F.col('age').between(0, 120))
            
            # Standardisation
            df = df.withColumn('email', F.lower(F.trim(F.col('email')))) \
                   .withColumn('name', F.trim(F.col('name')))
        
        elif dataset_type == 'transactions':
            # Nettoyage spécifique aux transactions
            df = df.filter(F.col('amount').isNotNull()) \
                   .filter(F.col('amount') > 0) \
                   .filter(F.col('transaction_id').isNotNull())
            
            # Validation du statut
            valid_statuses = ['completed', 'pending', 'failed']
            df = df.filter(F.col('status').isin(valid_statuses))
        
        elif dataset_type == 'logs':
            # Nettoyage spécifique aux logs
            df = df.filter(F.col('timestamp').isNotNull()) \
                   .filter(F.col('level').isin(['INFO', 'WARNING', 'ERROR', 'DEBUG']))
        
        # Suppression des colonnes métadonnées d'ingestion temporaires
        metadata_cols = [col for col in df.columns if col.startswith('_')]
        df = df.drop(*metadata_cols)
        
        final_count = df.count()
        logger.info(f"Nettoyage terminé: {initial_count} -> {final_count} lignes")
        
        return df
    
    def enrich_data(self, df: DataFrame, dataset_type: str = 'generic') -> DataFrame:
        """
        Enrichit les données avec des colonnes calculées et agrégations
        
        Args:
            df: DataFrame source
            dataset_type: Type de dataset
            
        Returns:
            DataFrame enrichi
        """
        logger.info(f"Enrichissement des données pour type: {dataset_type}")
        
        if dataset_type == 'users':
            # Catégorisation par âge
            df = df.withColumn('age_group', 
                F.when(F.col('age') < 25, 'young')
                 .when(F.col('age') < 50, 'adult')
                 .otherwise('senior'))
            
            # Extraction domaine email
            df = df.withColumn('email_domain', 
                F.regexp_extract(F.col('email'), r'@([\w\.-]+)', 1))
        
        elif dataset_type == 'transactions':
            # Conversion en EUR (exemple simplifié)
            df = df.withColumn('amount_eur',
                F.when(F.col('currency') == 'USD', F.col('amount') * 0.92)
                 .when(F.col('currency') == 'GBP', F.col('amount') * 1.17)
                 .otherwise(F.col('amount')))
            
            # Classification du montant
            df = df.withColumn('amount_category',
                F.when(F.col('amount') < 50, 'small')
                 .when(F.col('amount') < 500, 'medium')
                 .otherwise('large'))
            
            # Extraction date et heure
            df = df.withColumn('transaction_date', F.to_date(F.col('transaction_date')))
            df = df.withColumn('transaction_year', F.year(F.col('transaction_date')))
            df = df.withColumn('transaction_month', F.month(F.col('transaction_date')))
            df = df.withColumn('transaction_day', F.dayofmonth(F.col('transaction_date')))
        
        elif dataset_type == 'logs':
            # Extraction date et heure
            df = df.withColumn('log_date', F.to_date(F.col('timestamp')))
            df = df.withColumn('log_hour', F.hour(F.col('timestamp')))
            
            # Catégorisation de sévérité
            df = df.withColumn('severity_level',
                F.when(F.col('level') == 'ERROR', 3)
                 .when(F.col('level') == 'WARNING', 2)
                 .otherwise(1))
        
        # Ajout timestamp de transformation
        df = df.withColumn('_processed_timestamp', F.current_timestamp())
        
        logger.info(f"Enrichissement terminé: {len(df.columns)} colonnes")
        
        return df
    
    def aggregate_data(self, df: DataFrame, dataset_type: str = 'transactions') -> DataFrame:
        """
        Crée des agrégations pour l'analyse
        
        Args:
            df: DataFrame source
            dataset_type: Type de dataset
            
        Returns:
            DataFrame agrégé
        """
        logger.info(f"Agrégation des données pour type: {dataset_type}")
        
        if dataset_type == 'transactions':
            # Agrégation par user_id
            agg_df = df.groupBy('user_id') \
                .agg(
                    F.count('transaction_id').alias('total_transactions'),
                    F.sum('amount').alias('total_amount'),
                    F.avg('amount').alias('avg_amount'),
                    F.max('amount').alias('max_amount'),
                    F.min('amount').alias('min_amount'),
                    F.max('transaction_date').alias('last_transaction_date'),
                    F.countDistinct('merchant').alias('unique_merchants')
                )
            
            logger.info(f"Agrégation terminée: {agg_df.count()} groupes")
            return agg_df
        
        elif dataset_type == 'logs':
            # Agrégation par source et level
            agg_df = df.groupBy('source', 'level', 'log_date') \
                .agg(
                    F.count('log_id').alias('log_count')
                ) \
                .orderBy('log_date', 'source', 'level')
            
            return agg_df
        
        return df
    
    def save_to_processed(
        self, 
        df: DataFrame, 
        dataset_name: str,
        mode: str = 'overwrite',
        partition_by: Optional[List[str]] = None
    ) -> str:
        """
        Sauvegarde les données dans la couche PROCESSED
        
        Args:
            df: DataFrame à sauvegarder
            dataset_name: Nom du dataset
            mode: Mode d'écriture (overwrite, append)
            partition_by: Colonnes de partitionnement
            
        Returns:
            Chemin du répertoire sauvegardé
        """
        processed_path = f"{self.data_lake_path}/processed/{dataset_name}"
        logger.info(f"Sauvegarde dans PROCESSED: {processed_path}")
        
        try:
            writer = df.write.mode(mode).format('parquet')
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            writer.save(processed_path)
            
            logger.info(f"Données sauvegardées: {processed_path}")
            return processed_path
            
        except Exception as e:
            logger.error(f"Erreur sauvegarde PROCESSED: {e}")
            raise
    
    def register_hive_table(self, dataset_name: str, layer: str = 'processed'):
        """
        Enregistre le dataset comme table Hive pour requêtes SQL
        
        Args:
            dataset_name: Nom du dataset
            layer: Couche (raw, processed, curated)
        """
        table_name = f"{layer}_{dataset_name}"
        data_path = f"{self.data_lake_path}/{layer}/{dataset_name}"
        
        logger.info(f"Enregistrement table Hive: {table_name}")
        
        try:
            self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            
            self.spark.sql(f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS {table_name}
                USING PARQUET
                LOCATION '{data_path}'
            """)
            
            logger.info(f"Table Hive créée: {table_name}")
            
        except Exception as e:
            logger.error(f"Erreur création table Hive: {e}")
            raise
    
    def transform_pipeline(
        self,
        dataset_name: str,
        dataset_type: str,
        with_aggregation: bool = False
    ) -> str:
        """
        Pipeline complet de transformation: RAW -> Clean -> Enrich -> PROCESSED
        
        Args:
            dataset_name: Nom du dataset
            dataset_type: Type de dataset
            with_aggregation: Créer aussi une table agrégée
            
        Returns:
            Chemin du dataset transformé
        """
        logger.info(f"=== Pipeline transformation: {dataset_name} ===")
        
        # 1. Lecture depuis RAW
        df = self.read_from_raw(dataset_name)
        
        # 2. Nettoyage
        df_clean = self.clean_data(df, dataset_type)
        
        # 3. Enrichissement
        df_enriched = self.enrich_data(df_clean, dataset_type)
        
        # 4. Sauvegarde dans PROCESSED
        partition_cols = None
        if dataset_type == 'transactions':
            partition_cols = ['transaction_year', 'transaction_month']
        elif dataset_type == 'logs':
            partition_cols = ['log_date']
        
        output_path = self.save_to_processed(
            df_enriched, 
            dataset_name,
            partition_by=partition_cols
        )
        
        # 5. Enregistrement Hive
        self.register_hive_table(dataset_name, 'processed')
        
        # 6. Agrégation optionnelle
        if with_aggregation:
            df_agg = self.aggregate_data(df_enriched, dataset_type)
            agg_name = f"{dataset_name}_agg"
            self.save_to_processed(df_agg, agg_name)
            self.register_hive_table(agg_name, 'processed')
        
        logger.info(f"=== Pipeline terminé: {output_path} ===")
        
        return output_path
    
    def stop(self):
        """Arrête la session Spark"""
        self.spark.stop()
        logger.info("Session Spark arrêtée")


if __name__ == '__main__':
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description='PySpark Transformation Job')
    parser.add_argument('--dataset', type=str, required=True, help='Name of the dataset to process')
    parser.add_argument('--type', type=str, required=True, choices=['users', 'transactions', 'logs', 'generic'], help='Type of dataset')
    parser.add_argument('--aggregate', action='store_true', help='Perform aggregation')
    parser.add_argument('--data-lake', type=str, default='./data_lake', help='Path to data lake root')
    
    args = parser.parse_args()
    
    config = {
        'data_lake_path': args.data_lake
    }
    
    transformer = SparkTransformation(config)
    
    try:
        output_path = transformer.transform_pipeline(
            dataset_name=args.dataset,
            dataset_type=args.type,
            with_aggregation=args.aggregate
        )
        logger.info(f"Job completed successfully. Output: {output_path}")
    except Exception as e:
        logger.error(f"Job failed: {e}")
        sys.exit(1)
    finally:
        transformer.stop()
