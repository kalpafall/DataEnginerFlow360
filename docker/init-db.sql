-- Script d'initialisation PostgreSQL pour DataEnginerFlow360

-- Créer la base de données Airflow
SELECT 'CREATE DATABASE airflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

-- Créer les schémas pour le Data Warehouse
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Extensions utiles
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Créer un utilisateur pour les applications
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'dataflow_app') THEN
    CREATE USER dataflow_app WITH PASSWORD 'dataflow123';
  END IF;
END
$$;
GRANT ALL PRIVILEGES ON SCHEMA curated TO dataflow_app;
GRANT ALL PRIVILEGES ON SCHEMA staging TO dataflow_app;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO dataflow_app;

-- Logs
\echo 'Database initialization completed successfully';
