from pymongo import MongoClient
from datetime import datetime
import uuid

# Connexion MongoDB
client = MongoClient("mongodb://localhost:27018/")
db = client.agri_db
collection = db.agri_data_fake

# Documents de test
docs = [
    {"_id": str(uuid.uuid4()), "farm_name": "Ferme Alpha", "location": "Dakar", "size": 12.5, "created_at": datetime.now()},
    {"_id": str(uuid.uuid4()), "farm_name": "Ferme Beta", "location": "Thiès", "size": 8.2, "created_at": datetime.now()},
    {"_id": str(uuid.uuid4()), "farm_name": "Ferme Gamma", "location": "Saint-Louis", "size": 15.0, "created_at": datetime.now()},
]

# Insertion
collection.insert_many(docs)
print(f"{len(docs)} documents insérés dans MongoDB !")
