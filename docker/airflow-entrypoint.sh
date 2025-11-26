#!/bin/bash
set -e

# Attendre que PostgreSQL soit prêt
echo "Waiting for PostgreSQL..."
while ! nc -z postgres 5432; do
  sleep 1
done
echo "PostgreSQL is ready!"

# Initialiser la base de données Airflow si nécessaire
if [ "$1" = "webserver" ]; then
  echo "Initializing Airflow database..."
  airflow db init || airflow db upgrade
  
  echo "Creating admin user..."
  airflow users create \
    --username "${_AIRFLOW_WWW_USER_USERNAME:-admin}" \
    --password "${_AIRFLOW_WWW_USER_PASSWORD:-admin}" \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com || echo "User already exists"
fi

# Exécuter la commande Airflow
exec airflow "$@"
