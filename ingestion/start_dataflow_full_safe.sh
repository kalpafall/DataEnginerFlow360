#!/bin/bash
set -e

echo "🧹 Suppression des anciens conteneurs Mongo + Cassandra..."
docker rm -f mongo_agri cassandra_agri || true

echo "🚀 Démarrage de MongoDB..."
docker run -d --name mongo_agri -p 27018:27017 mongo:latest

echo "🚀 Démarrage de Cassandra..."
docker run -d --name cassandra_agri -p 9042:9042 cassandra:latest

# ---------- ATTENTE MongoDB ----------
echo "⏳ Attente que MongoDB soit prêt..."
until docker exec mongo_agri mongosh --eval "db.adminCommand('ping')" >/dev/null 2>&1; do
  echo "⏳ MongoDB non prêt, attente 3s..."
  sleep 3
done
echo "✅ MongoDB prêt !"

# ---------- ATTENTE Cassandra ----------
echo "⏳ Attente que Cassandra soit prêt..."
until docker exec cassandra_agri cqlsh -e "describe keyspaces" >/dev/null 2>&1; do
  echo "⏳ Cassandra non prêt, attente 5s..."
  sleep 5
done
echo "✅ Cassandra prêt !"

# ---------- CREATION KEYSPACE ET TABLES ----------
echo "🍂 Création du keyspace et des tables Cassandra..."
docker exec -i cassandra_agri cqlsh <<EOF
CREATE KEYSPACE IF NOT EXISTS agri_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE agri_keyspace;
CREATE TABLE IF NOT EXISTS farms (
    id UUID PRIMARY KEY,
    name text,
    location text,
    size double
);
EOF

# ---------- INSERTION DONNEES TEST ----------
echo "🌱 Insertion de données de test dans MongoDB..."
docker exec -i mongo_agri mongosh <<EOF
use agri_db;
db.agri_data_fake.insertMany([
  { farm_name: "Ferme Alpha", location: "Dakar", size: 12.5, created_at: new Date() },
  { farm_name: "Ferme Beta", location: "Thiès", size: 8.2, created_at: new Date() },
  { farm_name: "Ferme Gamma", location: "Saint-Louis", size: 15.0, created_at: new Date() }
]);
EOF

echo "🌾 Insertion de données de test dans Cassandra..."
docker exec -i cassandra_agri cqlsh <<EOF
USE agri_keyspace;
INSERT INTO farms (id, name, location, size) VALUES (uuid(), 'Ferme Alpha', 'Dakar', 12.5);
INSERT INTO farms (id, name, location, size) VALUES (uuid(), 'Ferme Beta', 'Thiès', 8.2);
INSERT INTO farms (id, name, location, size) VALUES (uuid(), 'Ferme Gamma', 'Saint-Louis', 15.0);
EOF

# ---------- LANCEMENT SCRIPT D'INGESTION ----------
echo "📥 Lancement du script d'ingestion..."
python3 data_ingestion.py

echo "✅ Tout est prêt : MongoDB + Cassandra + Data Lake avec données de test !"
