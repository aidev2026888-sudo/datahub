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
    DatasetPropertiesClass,
    SchemaMetadataClass,
    QueryPropertiesClass,
    GlossaryTermInfoClass,
    GlobalTagsClass,
)


class DataHubMetadataService:
    """High-level read interface to a DataHub metadata graph."""

    def __init__(self, server: str, token: str | None = None):
        cfg = DatahubClientConfig(server=server, token=token, disable_ssl_verification=True)
        self.graph = DataHubGraph(cfg)

    # ------------------------------------------------------------------
    # 1. List tables
    # ------------------------------------------------------------------
    def list_tables(
        self,
        platform: str,
        db_name: str | None = None,
        schema_name: str | None = None,
    ) -> str:
        """
        List all tables (datasets), optionally filtered by database and/or schema.

        Uses the native platform filter, then filters by db/schema in Python
        by parsing the qualified name from the URN.
        URN convention: urn:li:dataset:(urn:li:dataPlatform:<platform>,<db>.<schema>.<table>,<env>)
        """
        urns = list(self.graph.get_urns_by_filter(
            entity_types=["dataset"],
            platform=platform,
        ))

        output = []
        for urn in urns[:200]:
            # Parse the dataset name from URN
            # e.g. urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.users,PROD)
            dataset_name = urn.split(",")[1] if "," in urn else urn
            name_parts = dataset_name.split(".")

            if len(name_parts) >= 3:
                db, schema, table = name_parts[0], name_parts[1], ".".join(name_parts[2:])
            elif len(name_parts) == 2:
                db, schema, table = "", name_parts[0], name_parts[1]
            else:
                db, schema, table = "", "", dataset_name

            # Filter by database and/or schema if requested
            if db_name and db.lower() != db_name.lower():
                continue
            if schema_name and schema.lower() != schema_name.lower():
                continue

            properties = self.graph.get_aspect(urn, DatasetPropertiesClass)
            desc = properties.description if properties else ""

            output.append({
                "database": db,
                "schema": schema,
                "table": table,
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
        urns = list(self.graph.get_urns_by_filter(
            entity_types=["query"],
            query=f"*{dataset_urn}*",
        ))

        output = []
        for urn in urns[:20]:
            # Skip entities tagged Draft
            tags: GlobalTagsClass | None = self.graph.get_aspect(urn, GlobalTagsClass)
            if tags and any(t.tag == "urn:li:tag:Draft" for t in (tags.tags or [])):
                continue

            props: QueryPropertiesClass | None = self.graph.get_aspect(
                urn, QueryPropertiesClass
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
        urns = list(self.graph.get_urns_by_filter(
            entity_types=["query"],
        ))

        output = []
        for urn in urns[:50]:
            # Check tags: must have Template, must NOT have Draft
            tags: GlobalTagsClass | None = self.graph.get_aspect(urn, GlobalTagsClass)
            tag_names = [t.tag for t in (tags.tags if tags else [])]
            if "urn:li:tag:Template" not in tag_names:
                continue
            if "urn:li:tag:Draft" in tag_names:
                continue

            props: QueryPropertiesClass | None = self.graph.get_aspect(
                urn, QueryPropertiesClass
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
        urns = list(self.graph.get_urns_by_filter(
            entity_types=["glossaryTerm"],
        ))

        output = []
        for urn in urns[:100]:
            # Skip entities tagged Draft
            tags: GlobalTagsClass | None = self.graph.get_aspect(urn, GlobalTagsClass)
            if tags and any(t.tag == "urn:li:tag:Draft" for t in (tags.tags or [])):
                continue

            info: GlossaryTermInfoClass | None = self.graph.get_aspect(
                urn, GlossaryTermInfoClass
            )
            if info:
                name = info.name or urn.split(":")[-1]
                output.append(f"TERM: {name}\nDEFINITION: {info.definition}")

        return json.dumps(output, indent=2)
