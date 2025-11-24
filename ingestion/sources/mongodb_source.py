import pandas as pd
from pymongo import MongoClient
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class MongoDBSource:
    """Source d'extraction depuis MongoDB"""

    def __init__(
        self,
        database: str,
        collection: str,
        uri: Optional[str] = None,
        host: str = "localhost",
        port: int = 27018,   # 👉 Correction ici
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.database = database
        self.collection = collection

        try:
            if uri:
                self.client = MongoClient(uri)
            else:
                self.client = MongoClient(host=host, port=port, username=username, password=password)
            
            self.db = self.client[self.database]
            self.coll = self.db[self.collection]
            logger.info(f"🍃 Connexion MongoDB OK: {database}.{collection}")
        except Exception as e:
            logger.error(f"❌ Erreur connexion MongoDB: {e}")
            raise

    def fetch(self, query: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Récupère des documents MongoDB"""
        query = query or {}
        try:
            data = list(self.coll.find(query))
            # Convert ObjectId to string
            for doc in data:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
            df = pd.DataFrame(data)
            logger.info(f"📥 {len(df)} documents récupérés depuis MongoDB")
            return df
        except Exception as e:
            logger.error(f"❌ Erreur fetch MongoDB: {e}")
            raise
