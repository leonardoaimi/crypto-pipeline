-- PostgreSQL initialisation for the crypto pipeline
-- Runs once on first container start via docker-entrypoint-initdb.d

CREATE SCHEMA IF NOT EXISTS crypto;
