#!/usr/bin/env bash
# Cria o pool exigido pela DAG (2 slots). Rode apos `docker compose up -d`.
set -euo pipefail
docker compose exec -T airflow-scheduler airflow pools set ecommerce_pool 2 "Limita concorrencia das tasks mapeadas por categoria"
