# ShopBrasil - Pipeline de Pricing (Airflow)

Pipeline Airflow (TaskFlow API) que coleta produtos da FakeStore API, calcula metricas por categoria
(preco medio/min/max e quantidade) e grava no PostgreSQL. Roda todo dia as 06:00 (America/Sao_Paulo).

## Como rodar
```bash
cp .env.example .env              # ou: echo "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
docker compose up -d
./scripts/setup.sh                # cria o pool ecommerce_pool (2 slots)
```
UI: http://localhost:8080 (usuario `airflow` / senha `airflow`). Ative e dispare a DAG `shopbrasil_pricing_daily`.
A connection `postgres_shopbrasil` e criada via variavel de ambiente no `docker-compose.yaml`.

## Conferir
```bash
docker compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM pricing_snapshot;"
```

## Parar e limpar (remove containers, volumes e imagens)
```bash
docker compose down -v --rmi all
```

## Estrutura
- `dags/shopbrasil_pricing_pipeline.py` - DAG
- `plugins/operators/schema_validator.py` - `ValidarProdutosOperator` (valida o schema dos produtos antes do processamento)
- `scripts/setup.sh` - cria o pool
- `pyproject.toml` / `requirements.txt` - dependencias (uv)

## Design
- **Topologias:** linear (ingestao -> analise -> carga), fan-out (`.expand` por categoria), fan-in (`consolidar_resultados`).
- **Resiliencia:** 3 retries com exponential backoff, `try/except + raise`, callbacks de failure/retry/success.
- **Paralelismo:** Dynamic Task Mapping limitado pelo pool `ecommerce_pool` (2 slots).
- **Validacao:** `ValidarProdutosOperator` confere campos e tipos; payload invalido falha sem retry.
- **Idempotencia:** `pricing_snapshot` apaga a `run_date` e reinsere; `pricing_history` e append.
