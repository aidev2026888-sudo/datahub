"""
DataHub Metadata Service — fetches metadata from a DataHub instance.

Provides 5 methods mapped to the Agent's requirements:
  1. list_tables      — search datasets by platform/db
  2. list_columns     — get schema fields for a dataset
  3. get_sql_fragments — queries linked to a dataset (excluding Draft)
  4. get_query_templates — global templates tagged 'Template'
  5. get_business_terms — glossary terms (excluding Draft)
"""
import json
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    SchemaMetadataClass,
    QueryPropertiesClass,
    GlossaryTermInfoClass,
)


class DataHubMetadataService:
    """High-level read interface to a DataHub metadata graph."""

    def __init__(self, server: str, token: str | None = None):
        cfg = DatahubClientConfig(server=server, token=token)
        self.graph = DataHubGraph(cfg)

    # ------------------------------------------------------------------
    # 1. List tables
    # ------------------------------------------------------------------
    def list_tables(self, platform: str, db_name: str) -> str:
        """
        List all tables (datasets) inside a database.

        Uses DataHub search to find datasets whose browse path
        contains the database name.
        """
        query = f"browsePaths:*{db_name}*"
        results = self.graph.search(entity_type="dataset", query=query, count=50)

        output = []
        for entity in results:
            urn = entity.urn
            # Extract table name from URN
            # e.g. urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.users,PROD)
            table_name = urn.split(",")[1] if "," in urn else urn

            properties = self.graph.get_aspect(urn, "datasetProperties")
            desc = properties.description if properties else ""

            output.append({
                "table_name": table_name,
                "urn": urn,
                "description": desc,
            })
        return json.dumps(output, indent=2)

    # ------------------------------------------------------------------
    # 2. List columns
    # ------------------------------------------------------------------
    def list_columns(self, dataset_urn: str) -> str:
        """
        List columns (schema fields) for a given dataset URN.
        """
        schema: SchemaMetadataClass | None = self.graph.get_aspect(
            entity_urn=dataset_urn,
            aspect_type=SchemaMetadataClass,
        )

        if not schema:
            return json.dumps([{"error": "No schema found for this dataset."}])

        columns = []
        for field in schema.fields:
            columns.append({
                "name": field.fieldPath,
                "col_type": field.nativeDataType,
                "description": field.description or "",
            })

        return json.dumps([{"urn": dataset_urn, "columns": columns}], indent=2)

    # ------------------------------------------------------------------
    # 3. SQL fragments for a dataset
    # ------------------------------------------------------------------
    def get_sql_fragments(self, dataset_urn: str) -> str:
        """
        Return SQL fragments (query entities) linked to a dataset.
        Excludes entities tagged 'Draft' to enforce approval workflow.
        """
        query_str = f"subjects:{dataset_urn} AND -tags:Draft"
        results = self.graph.search(entity_type="query", query=query_str, count=20)

        output = []
        for entity in results:
            props: QueryPropertiesClass | None = self.graph.get_aspect(
                entity.urn, QueryPropertiesClass
            )
            if props:
                output.append({
                    "name": props.name,
                    "table_scope": dataset_urn,
                    "intent": props.description,
                    "sql_fragment": props.statement.value if props.statement else "",
                })
        return json.dumps(output, indent=2)

    # ------------------------------------------------------------------
    # 4. Query templates
    # ------------------------------------------------------------------
    def get_query_templates(self) -> str:
        """
        Return global query templates (tagged 'Template', excluding 'Draft').
        """
        results = self.graph.search(
            entity_type="query", query="tags:Template AND -tags:Draft", count=20
        )

        output = []
        for entity in results:
            props: QueryPropertiesClass | None = self.graph.get_aspect(
                entity.urn, QueryPropertiesClass
            )
            if props:
                output.append({
                    "parameterized_intent": props.description or "Generic Template",
                    "parameterized_sql": props.statement.value if props.statement else "",
                })
        return json.dumps(output, indent=2)

    # ------------------------------------------------------------------
    # 5. Business terms
    # ------------------------------------------------------------------
    def get_business_terms(self) -> str:
        """
        Return approved business glossary terms (excludes 'Draft').
        """
        results = self.graph.search(
            entity_type="glossaryTerm", query="-tags:Draft", count=100
        )

        output = []
        for entity in results:
            info: GlossaryTermInfoClass | None = self.graph.get_aspect(
                entity.urn, GlossaryTermInfoClass
            )
            if info:
                name = info.name or entity.urn.split(":")[-1]
                output.append(f"TERM: {name}\nDEFINITION: {info.definition}")

        return json.dumps(output, indent=2)
