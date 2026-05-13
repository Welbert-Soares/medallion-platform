# Medallion Platform

Pipeline de dados end-to-end baseado no padrão Medallion, implementado no Databricks com orquestração via GitHub Actions e transformação com dbt Core.

**Princípio central:** arquitetura orientada a metadata — adicionar uma nova fonte de dados é uma operação de dados (`INSERT` em uma tabela), não de código.

---

## Arquitetura

```
GitHub → CI/CD → Databricks Workflows → Bronze → Silver → Gold → Consumo
                          ↑
              cli_catalog.controller.ingestions
              (catálogo central de fontes)
```

| Camada | Schema | Ferramenta | Responsabilidade |
|---|---|---|---|
| Control | `cli_catalog.controller` | SQL | Catálogo de fontes + log de execuções |
| Bronze | `cli_catalog.bronze` | PySpark + Delta | Dado bruto preservado, append-only |
| Silver | `cli_catalog.silver` | dbt Core | Limpeza, tipagem, deduplicação |
| Gold | `cli_catalog.gold` | dbt Core | KPIs e métricas agregadas |

## Estrutura do repositório

```
medallion-platform/
├── .github/workflows/
│   ├── ci.yml                  # lint + dbt compile em todo PR
│   └── deploy.yml              # upload + job update + trigger em merge na main
├── databricks/
│   ├── notebooks/
│   │   ├── setup/
│   │   │   └── 00_create_tables.py    # setup inicial (rodar uma vez)
│   │   ├── ingestion/
│   │   │   ├── controller.py          # orquestra ingestões em paralelo
│   │   │   └── generic_ingestion.py   # motor de ingestão reutilizável
│   │   └── dbt/
│   │       ├── run_silver.py          # executa modelos Silver
│   │       └── run_gold.py            # executa modelos Gold
│   └── workflows/
│       └── main_workflow.json         # job como código (IaC)
├── dbt/
│   ├── models/
│   │   ├── silver/                    # silver_weather.sql, silver_ibge.sql
│   │   └── gold/                      # gold_kpi_daily.sql
│   ├── macros/generate_schema_name.sql
│   ├── dbt_project.yml
│   └── profiles.yml.example
├── data/sources/
│   └── ibge_municipios.csv            # 32 capitais + municípios demo
├── .env.example
└── Makefile
```

## Setup rápido

**Pré-requisitos:** Databricks workspace, Python 3.11+

```bash
# 1. Instalar dependências
make setup

# 2. Copiar e preencher credenciais
cp .env.example .env
# editar .env com DATABRICKS_HOST, TOKEN, HTTP_PATH

# 3. Configurar dbt
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
# editar com as mesmas credenciais

# 4. Upload manual inicial (antes do GitHub Actions)
make upload

# 5. Rodar o pipeline
make run
```

**Primeira vez apenas:** rodar o notebook `databricks/notebooks/setup/00_create_tables.py` no Databricks para criar os schemas e semear as fontes demo.

## Adicionando uma nova fonte

```sql
-- 1. Inserir na tabela de controle
INSERT INTO cli_catalog.controller.ingestions VALUES (
  'minha_fonte', 'api',
  '{"url": "https://api.exemplo.com/data", "params": {...}}',
  'bronze', 'minha_fonte_raw',
  'append', NULL, NULL, NULL, true,
  '{"domain": "meu-dominio", "owner": "meu-time"}',
  current_timestamp(), current_timestamp()
);

-- 2. Criar modelo Silver correspondente em dbt/models/silver/silver_minha_fonte.sql
-- 3. Abrir PR → ci.yml valida → merge → deploy.yml sobe e executa
```

## GitHub Secrets necessários

| Secret | Valor |
|---|---|
| `DATABRICKS_HOST` | `https://<workspace>.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Personal Access Token (PAT) |
| `DATABRICKS_HTTP_PATH` | HTTP Path do SQL Warehouse |

## Fontes demo

| Fonte | Tipo | Status |
|---|---|---|
| Open Meteo (São Paulo hourly weather) | API REST | ativo |
| IBGE Municípios (32 municípios) | CSV | ativo |
| PostgreSQL orders | JDBC/SQL | inativo — requer Docker |

## Comandos úteis

```bash
make compile   # valida SQL dos modelos dbt sem executar
make test      # roda testes de qualidade dbt
make docs      # gera e serve documentação dbt
make upload    # sobe notebooks e dbt project para o Databricks
make run       # aciona o pipeline manualmente
```

## Monitoramento

```sql
-- Status das últimas execuções
SELECT ingestion_name, status, rows_written, started_at
FROM cli_catalog.controller.execution_log
WHERE started_at >= NOW() - INTERVAL 24 HOURS
ORDER BY started_at DESC;

-- Falhas na última semana
SELECT ingestion_name, COUNT(*) AS failures, MAX(error_message) AS last_error
FROM cli_catalog.controller.execution_log
WHERE status = 'failed' AND started_at >= NOW() - INTERVAL 7 DAYS
GROUP BY 1 ORDER BY 2 DESC;
```
