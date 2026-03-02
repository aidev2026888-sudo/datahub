"""
ETL Ingestion Module — reads YAML source files and emits metadata to DataHub.

Supports three ingestion types:
  1. Tables & columns (SchemaMetadata + DatasetProperties)
  2. Query templates (QueryProperties + Template tag)
  3. Business terms (GlossaryTermInfo)

All entities are tagged with 'Draft' upon ingestion to support human-approval workflow.
"""
import re
import time
import yaml
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    SchemaMetadataClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    NumberTypeClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    TagAssociationClass,
    GlossaryTermInfoClass,
    QueryPropertiesClass,
    QueryLanguageClass,
    QueryStatementClass,
    AuditStampClass,
    OtherSchemaClass,
)

import config

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DRAFT_TAG = GlobalTagsClass(
    tags=[TagAssociationClass(tag="urn:li:tag:Draft")]
)

TEMPLATE_AND_DRAFT_TAGS = GlobalTagsClass(
    tags=[
        TagAssociationClass(tag="urn:li:tag:Template"),
        TagAssociationClass(tag="urn:li:tag:Draft"),
    ]
)


def _get_emitter() -> DatahubRestEmitter:
    """Build an emitter from centralised config."""
    return DatahubRestEmitter(
        gms_server=config.DATAHUB_GMS_SERVER,
        token=config.DATAHUB_TOKEN if config.DATAHUB_TOKEN else None,
        disable_ssl_verification=True,
    )


def _load_yaml(path: str) -> dict:
    """Load and return a YAML file as a dict."""
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _slugify(name: str) -> str:
    """Turn a human-readable name into a URL-safe slug for URNs."""
    return re.sub(r"[^a-zA-Z0-9_.]", "_", name.strip()).lower()


def _map_col_type(col_type: str):
    """Map a YAML column type string to a DataHub SchemaFieldDataType."""
    numeric_types = {"bigint", "int", "integer", "float", "double", "decimal", "numeric", "smallint", "tinyint"}
    if col_type.lower() in numeric_types:
        return SchemaFieldDataTypeClass(type=NumberTypeClass())
    return SchemaFieldDataTypeClass(type=StringTypeClass())


# ---------------------------------------------------------------------------
# 1. Ingest tables & columns
# ---------------------------------------------------------------------------

def ingest_tables(
    yaml_path: str,
    platform: str | None = None,
    env: str | None = None,
    db: str | None = None,
    schema: str | None = None,
    dry_run: bool = False,
):
    """
    Read tables.yaml and emit Dataset entities with SchemaMetadata.

    YAML format expected:
        tables:
          - TABLE_NAME: t1
            COLUMNS:
              - NAME: col1
                COL_TYPE: bigint
                DESCRIPTION: "some description"
    """
    platform = platform or config.DEFAULT_PLATFORM
    env = env or config.DEFAULT_ENV
    db = db or config.DEFAULT_DATABASE
    schema = schema or config.DEFAULT_SCHEMA

    data = _load_yaml(yaml_path)
    tables = data.get("tables", [])

    if not tables:
        print("No tables found in YAML.")
        return

    emitter = None if dry_run else _get_emitter()
    mcps: list[MetadataChangeProposalWrapper] = []

    for table in tables:
        table_name = table["TABLE_NAME"]
        columns = table.get("COLUMNS", [])

        # Build qualified name: db.schema.table (skip empty parts)
        name_parts = [p for p in [db, schema, table_name] if p]
        qualified_name = ".".join(name_parts)

        dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{qualified_name},{env})"

        # --- Schema fields ---
        fields = []
        for col in columns:
            field = SchemaFieldClass(
                fieldPath=col["NAME"],
                type=_map_col_type(col.get("COL_TYPE", "string")),
                nativeDataType=col.get("COL_TYPE", "string"),
                description=col.get("DESCRIPTION", ""),
            )
            fields.append(field)

        schema = SchemaMetadataClass(
            schemaName=table_name,
            platform=f"urn:li:dataPlatform:{platform}",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        )

        # --- Dataset properties (description, custom props) ---
        props = DatasetPropertiesClass(
            name=table_name,
            description=f"Table {table_name} ingested from YAML.",
        )

        mcps.append(MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=schema))
        mcps.append(MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=props))
        mcps.append(MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=DRAFT_TAG))

    if dry_run:
        print(f"[DRY RUN] Would emit {len(mcps)} MCPs for {len(tables)} table(s):")
        for t in tables:
            print(f"  - {t['TABLE_NAME']}  ({len(t.get('COLUMNS', []))} columns)")
        return

    for mcp in mcps:
        emitter.emit_mcp(mcp)
    print(f"Ingested {len(tables)} table(s) with Draft tag.")


# ---------------------------------------------------------------------------
# 2. Ingest query templates
# ---------------------------------------------------------------------------

def ingest_query_templates(yaml_path: str, dry_run: bool = False):
    """
    Read query_templates.yaml and emit Query entities.

    YAML format expected:
        query_templates:
          - parameterized_intent: "total cost"
            parameterized_sql: |
              select * from t1 where costtype = 1
    """
    data = _load_yaml(yaml_path)
    templates = data.get("query_templates", [])

    if not templates:
        print("No query templates found in YAML.")
        return

    emitter = None if dry_run else _get_emitter()
    mcps: list[MetadataChangeProposalWrapper] = []

    for tmpl in templates:
        intent = tmpl["parameterized_intent"]
        sql = tmpl["parameterized_sql"].strip()
        slug = _slugify(intent)
        query_urn = f"urn:li:query:{slug}"

        now_ms = int(time.time() * 1000)
        audit_stamp = AuditStampClass(time=now_ms, actor="urn:li:corpuser:datahub")

        props = QueryPropertiesClass(
            statement=QueryStatementClass(value=sql, language=QueryLanguageClass.SQL),
            source="MANUAL",
            created=audit_stamp,
            lastModified=audit_stamp,
            name=intent,
            description=intent,
        )

        mcps.append(MetadataChangeProposalWrapper(entityUrn=query_urn, aspect=props))
        mcps.append(MetadataChangeProposalWrapper(entityUrn=query_urn, aspect=TEMPLATE_AND_DRAFT_TAGS))

    if dry_run:
        print(f"[DRY RUN] Would emit {len(mcps)} MCPs for {len(templates)} query template(s):")
        for t in templates:
            print(f"  - {t['parameterized_intent']}")
        return

    for mcp in mcps:
        emitter.emit_mcp(mcp)
    print(f"Ingested {len(templates)} query template(s) with Draft + Template tags.")


# ---------------------------------------------------------------------------
# 3. Ingest business terms
# ---------------------------------------------------------------------------

def ingest_business_terms(yaml_path: str, dry_run: bool = False):
    """
    Read business_terms.yaml and emit GlossaryTerm entities.

    YAML format expected:
        business_terms:
          - "FY starts with feb"
          - "Default currency is CHF"
    """
    data = _load_yaml(yaml_path)
    terms = data.get("business_terms", [])

    if not terms:
        print("No business terms found in YAML.")
        return

    emitter = None if dry_run else _get_emitter()
    mcps: list[MetadataChangeProposalWrapper] = []

    for term_text in terms:
        slug = _slugify(term_text)
        term_urn = f"urn:li:glossaryTerm:{slug}"

        info = GlossaryTermInfoClass(
            definition=term_text,
            name=term_text,
            termSource="EXTERNAL",
        )

        mcps.append(MetadataChangeProposalWrapper(entityUrn=term_urn, aspect=info))
        mcps.append(MetadataChangeProposalWrapper(entityUrn=term_urn, aspect=DRAFT_TAG))

    if dry_run:
        print(f"[DRY RUN] Would emit {len(mcps)} MCPs for {len(terms)} business term(s):")
        for t in terms:
            print(f"  - {t}")
        return

    for mcp in mcps:
        emitter.emit_mcp(mcp)
    print(f"Ingested {len(terms)} business term(s) with Draft tag.")
