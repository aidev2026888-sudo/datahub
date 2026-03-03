# DataHub Integration

Python CLI tool for ingesting metadata from YAML files into [DataHub](https://datahubproject.io) and querying it back via the DataHub OpenAPI v3 endpoint.

## Features

- **YAML-driven ingestion** — define tables/columns, query templates, and business terms in YAML
- **Scoped metadata** — query templates and business terms are linked to datasets at table/schema/db level
- **Glossary grouping** — business terms are organized under GlossaryNode (term groups)
- **Draft/Approval workflow** — all ingested entities are tagged `Draft`; data stewards approve them in the DataHub UI
- **Metadata fetching** — list tables, columns, SQL fragments, query templates, and business terms (uses OpenAPI v3 `get_entities` batch endpoint)
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
| `DATAHUB_DATABASE` | *(empty)* | Default database name |
| `DATAHUB_SCHEMA` | `public` | Default schema name |

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
Query templates are grouped by scope. The scope determines which dataset the queries are linked to via `QuerySubjects`.

```yaml
query_templates:
  - scope:
      platform: postgres
      database: mydb
      schema: public
      table: t1
    queries:
      - parameterized_intent: "total cost"
        parameterized_sql: |
          select * from t1 where costtype = 1
      - parameterized_intent: "total budget"
        parameterized_sql: |
          select * from t1 where costtype = 2
```

### `business_terms.yaml`
Business terms are grouped by scope. Each entry contains a `group` (GlossaryNode), a `scope` (dataset link), and a `terms` list.

```yaml
business_terms:
  - group: "Finance"
    scope:
      platform: postgres
      database: mydb
    terms:
      - "FY starts with feb"
      - "Default currency is CHF"
  - group: "Operations"
    scope:
      platform: postgres
      database: mydb
      schema: public
      table: t1
    terms:
      - "Cost type 1 is actual"
      - "Cost type 2 is budget"
```

## CLI Usage

```bash
# --- Ingestion (with optional --dry-run) ---
python main.py ingest-tables   --file data/tables.yaml --platform postgres --db mydb --schema public
python main.py ingest-templates --file data/query_templates.yaml --dry-run
python main.py ingest-terms    --file data/business_terms.yaml --dry-run

# --- Fetching ---
python main.py list-tables      --platform postgres --db mydb --schema public
python main.py list-columns     --urn "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.t1,PROD)"
python main.py sql-fragments    --urn "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.t1,PROD)"
python main.py query-templates
python main.py business-terms

# --- Version History ---
python main.py term-history     --urn "urn:li:glossaryTerm:mydb_fy_starts_with_feb"
```

## Draft/Approval Workflow

1. ETL pipeline ingests metadata → entities get a **`Draft`** tag
2. Data steward searches for `Draft` in DataHub UI
3. Steward reviews, removes `Draft`, adds **`Approved`** tag
4. All fetch commands (`list-tables`, `list-columns`, `sql-fragments`, etc.) automatically **exclude** `Draft` entities — only approved data is returned

## How Scoping Works

| Entity | Scope mechanism | URN format |
|---|---|---|
| Tables | CLI args `--platform/--db/--schema` | `urn:li:dataset:(urn:li:dataPlatform:{platform},{db}.{schema}.{table},{env})` |
| Query templates | YAML `scope` block → `QuerySubjects` aspect | `urn:li:query:{scope}_{intent}` |
| Business terms | YAML `scope` block → `GlossaryTerms` aspect on dataset | `urn:li:glossaryTerm:{scope}_{term}` |
| Term groups | YAML `group` field → `GlossaryNodeInfo` aspect | `urn:li:glossaryNode:{group}` |

## Project Structure

```
datahub-integration/
├── config.py                # Env-var based configuration
├── main.py                  # CLI entry point
├── setup_env.bat            # Venv bootstrap script
├── requirements.txt         # Python dependencies
├── data/                    # YAML source files
│   ├── tables.yaml
│   ├── query_templates.yaml
│   └── business_terms.yaml
├── etl/
│   └── ingest.py            # YAML → DataHub ingestion (emit MCPs)
├── services/
│   ├── metadata_service.py  # 5 fetch methods (OpenAPI v3 get_entities)
│   └── version_history.py   # Aspect version history
└── tests/
    └── test_metadata_service.py  # Unit tests (mocked DataHubGraph)
```
