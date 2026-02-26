# DataHub Integration

Python CLI tool for ingesting metadata from YAML files into [DataHub](https://datahubproject.io) and querying it back via the DataHub Graph API.

## Features

- **YAML-driven ingestion** — define tables/columns, query templates, and business terms in YAML
- **Draft/Approval workflow** — all ingested entities are tagged `Draft`; data stewards approve them in the DataHub UI
- **Metadata fetching** — list tables, columns, SQL fragments, query templates, and business terms
- **Version history** — inspect how glossary term definitions changed over time

## Setup

```bash
# Windows — run the setup script
setup_env.bat

# Or manually
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Configuration

Set environment variables (or accept defaults):

| Variable | Default | Description |
|---|---|---|
| `DATAHUB_GMS_SERVER` | `http://localhost:8080` | DataHub GMS endpoint |
| `DATAHUB_TOKEN` | *(empty)* | Auth token |
| `DATAHUB_PLATFORM` | `postgres` | Default data platform |
| `DATAHUB_ENV` | `PROD` | Default environment |

## YAML Data Files

All source files live in `data/`:

### `tables.yaml`
```yaml
tables:
  - TABLE_NAME: t1
    COLUMNS:
      - NAME: col1
        COL_TYPE: bigint
        DESCRIPTION: "this is a test col"
```

### `query_templates.yaml`
```yaml
query_templates:
  - parameterized_intent: "total cost"
    parameterized_sql: |
      select * from t1 where costtype = 1
```

### `business_terms.yaml`
```yaml
business_terms:
  - "FY starts with feb"
  - "Default currency is CHF"
```

## CLI Usage

```bash
# --- Ingestion (with optional --dry-run) ---
python main.py ingest-tables   --file data/tables.yaml --dry-run
python main.py ingest-templates --file data/query_templates.yaml
python main.py ingest-terms    --file data/business_terms.yaml

# --- Fetching ---
python main.py list-tables      --platform postgres --db mydb
python main.py list-columns     --urn "urn:li:dataset:(urn:li:dataPlatform:postgres,t1,PROD)"
python main.py sql-fragments    --urn "urn:li:dataset:(urn:li:dataPlatform:postgres,t1,PROD)"
python main.py query-templates
python main.py business-terms

# --- Version History ---
python main.py term-history     --urn "urn:li:glossaryTerm:fy_starts_with_feb"
```

## Draft/Approval Workflow

1. ETL pipeline ingests metadata → entities get a **`Draft`** tag
2. Data steward searches for `Draft` in DataHub UI
3. Steward reviews, removes `Draft`, adds **`Approved`** tag
4. Fetch commands automatically **exclude** `Draft` entities — only approved data is returned

## Project Structure

```
datahub-integration/
├── config.py              # Env-var based configuration
├── main.py                # CLI entry point
├── setup_env.bat          # Venv bootstrap script
├── requirements.txt       # Python dependencies
├── data/                  # YAML source files
│   ├── tables.yaml
│   ├── query_templates.yaml
│   └── business_terms.yaml
├── etl/
│   └── ingest.py          # YAML → DataHub ingestion
└── services/
    ├── metadata_service.py  # 5 fetch methods
    └── version_history.py   # Aspect version history
```
