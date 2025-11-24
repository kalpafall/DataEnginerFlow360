#!/bin/bash

# Script de démarrage pour DataEnginerFlow360
set -e

echo "========================================="
echo "  DataEnginerFlow360 - Démarrage"
echo "========================================="

# Couleurs pour les messages
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Vérifier que Docker est installé
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Erreur: Docker n'est pas installé${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Erreur: docker-compose n'est pas installé${NC}"
    exit 1
fi

# Créer les répertoires nécessaires
echo -e "${YELLOW}Création des répertoires...${NC}"
mkdir -p ../data_lake/{raw,processed,curated}
mkdir -p ../orchestration/dags
mkdir -p prometheus

# Créer le fichier prometheus.yml si nécessaire
if [ ! -f "prometheus/prometheus.yml" ]; then
    echo -e "${YELLOW}Création de prometheus.yml...${NC}"
    mkdir -p prometheus
    cat > prometheus/prometheus.yml <<EOF
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']
EOF
fi

# Construire les images
echo -e "${YELLOW}Construction des images Docker...${NC}"
docker-compose build

# Initialiser Airflow
echo -e "${YELLOW}Initialisation d'Airflow...${NC}"
docker-compose up airflow-init || true

# Démarrer tous les services
echo -e "${YELLOW}Démarrage des services...${NC}"
docker-compose up -d

# Attendre que les services soient prêts
echo -e "${YELLOW}Attente du démarrage des services...${NC}"
sleep 15

# Vérifier l'état des services
echo -e "\n${GREEN}Vérification de l'état des services:${NC}"
docker-compose ps

# Afficher les URLs d'accès
echo -e "\n${GREEN}=========================================${NC}"
echo -e "${GREEN}Services disponibles:${NC}"
echo -e "${GREEN}=========================================${NC}"
echo -e "Airflow UI:      http://localhost:8080"
echo -e "  Utilisateur:   admin"
echo -e "  Mot de passe:  admin"
echo -e ""
echo -e "Flower (Celery): http://localhost:5555"
echo -e "Grafana:         http://localhost:3000"
echo -e "  Utilisateur:   admin"
echo -e "  Mot de passe:  admin"
echo -e ""
echo -e "PostgreSQL:      localhost:5432"
echo -e "  Database:      dataenginerflow360"
echo -e "  Utilisateur:   postgres"
echo -e "  Mot de passe:  postgres"
echo -e ""
echo -e "Kafka:           localhost:9093"
echo -e "${GREEN}=========================================${NC}"

echo -e "\n${GREEN}Pour voir les logs:${NC} docker-compose logs -f"
echo -e "${GREEN}Pour arrêter:${NC} docker-compose down"
echo -e "${GREEN}Pour arrêter et supprimer les volumes:${NC} docker-compose down -v"
