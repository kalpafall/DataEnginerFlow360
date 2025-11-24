import logging
from typing import List, Dict, Any, Optional
from kafka import KafkaConsumer
import json
import pandas as pd

logger = logging.getLogger(__name__)

class KafkaSource:
    """Source d'extraction depuis Kafka"""

    def __init__(
        self,
        bootstrap_servers: List[str],
        topic: str,
        group_id: Optional[str] = None,
        auto_offset_reset: str = 'earliest'
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset

    def fetch(self, max_messages: int = 100, timeout_ms: int = 5000) -> pd.DataFrame:
        """Récupère des messages depuis Kafka"""
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset=self.auto_offset_reset,
                enable_auto_commit=True,
                group_id=self.group_id,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=timeout_ms
            )

            messages = []
            logger.info(f"🎧 Écoute du topic Kafka: {self.topic}...")
            
            for message in consumer:
                messages.append(message.value)
                if len(messages) >= max_messages:
                    break
            
            consumer.close()
            
            df = pd.DataFrame(messages)
            logger.info(f"📥 {len(df)} messages récupérés depuis Kafka")
            return df

        except Exception as e:
            logger.error(f"❌ Erreur fetch Kafka: {e}")
            raise
