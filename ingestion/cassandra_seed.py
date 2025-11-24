from cassandra.cluster import Cluster
import uuid

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('agri_keyspace')

# Lignes de test
farms = [
    (uuid.uuid4(), "Ferme Alpha", "Dakar", 12.5),
    (uuid.uuid4(), "Ferme Beta", "Thiès", 8.2),
    (uuid.uuid4(), "Ferme Gamma", "Saint-Louis", 15.0),
]

for f in farms:
    session.execute(
        "INSERT INTO farms (id, name, location, size) VALUES (%s, %s, %s, %s)",
        f
    )

print(f"{len(farms)} lignes insérées dans Cassandra !")
