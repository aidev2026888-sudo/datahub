"""
DataHub Metadata Service — fetches metadata from a DataHub instance.

Provides 5 methods mapped to the Agent's requirements:
  1. list_tables      — search datasets by platform/db
  2. list_columns     — get schema fields for a dataset
  3. get_sql_fragments — queries linked to a dataset (excluding Draft)
  4. get_query_templates — global templates tagged 'Template'
  5. get_business_terms — glossary terms (excluding Draft)

All aspect fetching uses DataHubGraph.get_entities() (OpenAPI v3 batchGet)
instead of the legacy get_aspect() REST call.
"""
import json
from typing import Any
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
    # Helper: fetch aspects for one or more entities via OpenAPI v3
    # ------------------------------------------------------------------
    def _fetch_entity_aspects(
        self,
        entity_name: str,
        urns: list[str],
        aspect_names: list[str],
    ) -> dict[str, dict[str, Any]]:
        """
        Batch-fetch typed aspects for a list of URNs using the OpenAPI v3
        ``/entity/{entity_name}/batchGet`` endpoint.

        Returns:
            ``{urn: {aspect_name: typed_aspect_object, ...}, ...}``
            URNs with no matching aspects are omitted from the result.
        """
        if not urns:
            return {}

        try:
            raw = self.graph.get_entities(
                entity_name=entity_name,
                urns=urns,
                aspects=aspect_names,
            )
            # raw: Dict[urn, Dict[aspect_name, (typed_aspect, SystemMetadata|None)]]
            # Strip the SystemMetadata wrapper for convenience
            return {
                urn: {name: tup[0] for name, tup in aspects.items()}
                for urn, aspects in raw.items()
            }
        except Exception as e:
            print(f"[_fetch_entity_aspects] Error ({type(e).__name__}): {e}")
            return {}

    # ------------------------------------------------------------------
    # 1. List tables
    # ------------------------------------------------------------------
    def list_tables(
        self,
        platform: str,
        db_name: str | None = None,
        schema_name: str | None = None,
        include_draft: bool = False,
    ) -> str:
        """
        List all tables (datasets), optionally filtered by database and/or schema.

        Uses the native platform filter, then filters by db/schema in Python
        by parsing the qualified name from the URN.
        Excludes datasets tagged 'Draft' by default.
        URN convention: urn:li:dataset:(urn:li:dataPlatform:<platform>,<db>.<schema>.<table>,<env>)
        """
        urns = list(self.graph.get_urns_by_filter(
            entity_types=["dataset"],
            platform=platform,
        ))
        urns = urns[:200]

        if not include_draft:
            # Batch-fetch globalTags to filter out Draft entities
            aspects = self._fetch_entity_aspects(
                entity_name="dataset",
                urns=urns,
                aspect_names=["globalTags"],
            )
            filtered_urns = []
            for urn in urns:
                tags: GlobalTagsClass | None = aspects.get(urn, {}).get("globalTags")
                if tags and any(t.tag == "urn:li:tag:Draft" for t in (tags.tags or [])):
                    continue
                filtered_urns.append(urn)
            urns = filtered_urns

        output = []
        for urn in urns:
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

            output.append({
                "database": db,
                "schema": schema,
                "table": table,
                "urn": urn,
            })
        return json.dumps(output, indent=2)

    # ------------------------------------------------------------------
    # 2. List columns
    # ------------------------------------------------------------------
    def list_columns(self, dataset_urn: str, include_draft: bool = False) -> str:
        """
        List columns (schema fields) for a given dataset URN.
        Excludes datasets tagged 'Draft' by default.
        """
        aspects = self._fetch_entity_aspects(
            entity_name="dataset",
            urns=[dataset_urn],
            aspect_names=["schemaMetadata", "globalTags"],
        )

        entity = aspects.get(dataset_urn, {})
        tags: GlobalTagsClass | None = entity.get("globalTags")

        if not include_draft:
            if tags and any(t.tag == "urn:li:tag:Draft" for t in (tags.tags or [])):
                return json.dumps([{"error": f"Dataset {dataset_urn} is currently in Draft state and must be approved."}])

        schema = entity.get("schemaMetadata")
        if not isinstance(schema, SchemaMetadataClass):
            return json.dumps([{"error": f"No schema found for dataset {dataset_urn}."}])

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
        urns = urns[:20]

        # Batch-fetch globalTags + queryProperties for all query URNs
        aspects = self._fetch_entity_aspects(
            entity_name="query",
            urns=urns,
            aspect_names=["globalTags", "queryProperties"],
        )

        output = []
        for urn in urns:
            entity = aspects.get(urn, {})
            tags: GlobalTagsClass | None = entity.get("globalTags")
            props: QueryPropertiesClass | None = entity.get("queryProperties")

            # Skip entities tagged Draft
            if tags and any(t.tag == "urn:li:tag:Draft" for t in (tags.tags or [])):
                continue

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
        urns = urns[:50]

        # Batch-fetch globalTags + queryProperties for all query URNs
        aspects = self._fetch_entity_aspects(
            entity_name="query",
            urns=urns,
            aspect_names=["globalTags", "queryProperties"],
        )

        output = []
        for urn in urns:
            entity = aspects.get(urn, {})
            tags: GlobalTagsClass | None = entity.get("globalTags")
            props: QueryPropertiesClass | None = entity.get("queryProperties")

            # Check tags: must have Template, must NOT have Draft
            tag_names = [t.tag for t in (tags.tags if tags else [])]
            if "urn:li:tag:Template" not in tag_names:
                continue
            if "urn:li:tag:Draft" in tag_names:
                continue

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
        urns = urns[:100]

        # Batch-fetch globalTags + glossaryTermInfo for all term URNs
        aspects = self._fetch_entity_aspects(
            entity_name="glossaryTerm",
            urns=urns,
            aspect_names=["globalTags", "glossaryTermInfo"],
        )

        output = []
        for urn in urns:
            entity = aspects.get(urn, {})
            tags: GlobalTagsClass | None = entity.get("globalTags")
            info: GlossaryTermInfoClass | None = entity.get("glossaryTermInfo")

            # Skip entities tagged Draft
            if tags and any(t.tag == "urn:li:tag:Draft" for t in (tags.tags or [])):
                continue

            if info:
                name = info.name or urn.split(":")[-1]
                output.append(f"TERM: {name}\nDEFINITION: {info.definition}")

        return json.dumps(output, indent=2)
