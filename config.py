"""
Centralized configuration for the DataHub integration project.
Reads connection details from environment variables.
"""
import os

# DataHub GMS server URL
DATAHUB_GMS_SERVER = os.getenv("DATAHUB_GMS_SERVER", "http://localhost:8080")

# DataHub authentication token (leave empty if auth is disabled)
DATAHUB_TOKEN = os.getenv("DATAHUB_TOKEN", "")

# Default data platform for dataset ingestion (e.g., postgres, mysql, snowflake)
DEFAULT_PLATFORM = os.getenv("DATAHUB_PLATFORM", "postgres")

# Default environment tag (PROD, DEV, STAGING)
DEFAULT_ENV = os.getenv("DATAHUB_ENV", "PROD")

# Default database name (used in dataset URN: <db>.<schema>.<table>)
DEFAULT_DATABASE = os.getenv("DATAHUB_DATABASE", "")

# Default schema name (used in dataset URN: <db>.<schema>.<table>)
DEFAULT_SCHEMA = os.getenv("DATAHUB_SCHEMA", "public")
