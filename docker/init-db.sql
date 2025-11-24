-- Script d'initialisation PostgreSQL pour DataEnginerFlow360

-- Créer la base de données Airflow si nécessaire
CREATE DATABASE IF NOT EXISTS airflow;

-- Créer les schémas pour le Data Warehouse
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Extensions utiles
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Créer un utilisateur pour les applications
CREATE USER IF NOT EXISTS dataflow_app WITH PASSWORD 'dataflow123';
GRANT ALL PRIVILEGES ON SCHEMA curated TO dataflow_app;
GRANT ALL PRIVILEGES ON SCHEMA staging TO dataflow_app;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO dataflow_app;

-- Logs
\echo 'Database initialization completed successfully';
