"""
Unit tests for DataHubMetadataService.

Uses unittest.mock to patch the DataHubGraph client so tests run without
a live DataHub instance.
"""
import json
import unittest
from unittest.mock import patch, MagicMock

from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    SchemaMetadataClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    QueryPropertiesClass,
    QueryStatementClass,
    QueryLanguageClass,
    GlossaryTermInfoClass,
    GlobalTagsClass,
    TagAssociationClass,
    OtherSchemaClass,
)


# Patch DataHubGraph so it doesn't try to connect on init
@patch("services.metadata_service.DataHubGraph", autospec=True)
class TestDataHubMetadataService(unittest.TestCase):

    def _make_service(self, MockGraph):
        """Create a service with a mocked graph."""
        from services.metadata_service import DataHubMetadataService
        svc = DataHubMetadataService(server="http://fake:8080", token="fake")
        # The mock graph instance is the return value of the constructor
        svc.graph = MockGraph.return_value
        return svc

    # ---------------------------------------------------------------
    # 1. list_tables
    # ---------------------------------------------------------------
    def test_list_tables_parses_3_part_urn(self, MockGraph):
        svc = self._make_service(MockGraph)
        svc.graph.get_urns_by_filter.return_value = [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.users,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.orders,PROD)",
        ]
        props = MagicMock(spec=DatasetPropertiesClass)
        props.description = "A table"
        svc.graph.get_aspect.return_value = props

        result = json.loads(svc.list_tables("postgres"))
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["database"], "mydb")
        self.assertEqual(result[0]["schema"], "public")
        self.assertEqual(result[0]["table"], "users")

    def test_list_tables_filters_by_db(self, MockGraph):
        svc = self._make_service(MockGraph)
        svc.graph.get_urns_by_filter.return_value = [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.users,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,otherdb.public.orders,PROD)",
        ]
        props = MagicMock(spec=DatasetPropertiesClass)
        props.description = ""
        svc.graph.get_aspect.return_value = props

        result = json.loads(svc.list_tables("postgres", db_name="mydb"))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["database"], "mydb")

    def test_list_tables_filters_by_schema(self, MockGraph):
        svc = self._make_service(MockGraph)
        svc.graph.get_urns_by_filter.return_value = [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.t1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.analytics.t2,PROD)",
        ]
        props = MagicMock(spec=DatasetPropertiesClass)
        props.description = ""
        svc.graph.get_aspect.return_value = props

        result = json.loads(svc.list_tables("postgres", schema_name="analytics"))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["schema"], "analytics")
        self.assertEqual(result[0]["table"], "t2")

    def test_list_tables_2_part_name(self, MockGraph):
        svc = self._make_service(MockGraph)
        svc.graph.get_urns_by_filter.return_value = [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,public.users,PROD)",
        ]
        props = MagicMock(spec=DatasetPropertiesClass)
        props.description = ""
        svc.graph.get_aspect.return_value = props

        result = json.loads(svc.list_tables("postgres"))
        self.assertEqual(result[0]["database"], "")
        self.assertEqual(result[0]["schema"], "public")
        self.assertEqual(result[0]["table"], "users")

    def test_list_tables_1_part_name(self, MockGraph):
        svc = self._make_service(MockGraph)
        svc.graph.get_urns_by_filter.return_value = [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,users,PROD)",
        ]
        props = MagicMock(spec=DatasetPropertiesClass)
        props.description = ""
        svc.graph.get_aspect.return_value = props

        result = json.loads(svc.list_tables("postgres"))
        self.assertEqual(result[0]["database"], "")
        self.assertEqual(result[0]["schema"], "")
        self.assertEqual(result[0]["table"], "users")

    def test_list_tables_does_not_call_get_aspect(self, MockGraph):
        """list_tables should not call get_aspect (no description fetch)."""
        svc = self._make_service(MockGraph)
        svc.graph.get_urns_by_filter.return_value = [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.t1,PROD)",
        ]

        svc.list_tables("postgres")
        svc.graph.get_aspect.assert_not_called()

    # ---------------------------------------------------------------
    # 2. list_columns
    # ---------------------------------------------------------------
    def test_list_columns_returns_fields(self, MockGraph):
        svc = self._make_service(MockGraph)
        schema = SchemaMetadataClass(
            schemaName="t1",
            platform="urn:li:dataPlatform:postgres",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[
                SchemaFieldClass(
                    fieldPath="id",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="bigint",
                    description="Primary key",
                ),
                SchemaFieldClass(
                    fieldPath="name",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="varchar",
                    description="User name",
                ),
            ],
        )
        svc.graph.get_aspect.return_value = schema

        result = json.loads(svc.list_columns("urn:li:dataset:test"))
        self.assertEqual(len(result), 1)
        cols = result[0]["columns"]
        self.assertEqual(len(cols), 2)
        self.assertEqual(cols[0]["name"], "id")
        self.assertEqual(cols[0]["col_type"], "bigint")

    def test_list_columns_no_schema(self, MockGraph):
        svc = self._make_service(MockGraph)
        svc.graph.get_aspect.return_value = None

        result = json.loads(svc.list_columns("urn:li:dataset:test"))
        self.assertIn("error", result[0])

    # ---------------------------------------------------------------
    # 3. get_sql_fragments — Draft filtering
    # ---------------------------------------------------------------
    def test_get_sql_fragments_excludes_draft(self, MockGraph):
        svc = self._make_service(MockGraph)
        svc.graph.get_urns_by_filter.return_value = [
            "urn:li:query:approved_q",
            "urn:li:query:draft_q",
        ]

        draft_tags = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:Draft")])
        approved_tags = GlobalTagsClass(tags=[])
        props = MagicMock(spec=QueryPropertiesClass)
        props.name = "test query"
        props.description = "intent"
        props.statement = MagicMock()
        props.statement.value = "SELECT 1"

        def mock_get_aspect(urn, aspect_cls):
            if aspect_cls == GlobalTagsClass:
                return draft_tags if urn == "urn:li:query:draft_q" else approved_tags
            return props

        svc.graph.get_aspect.side_effect = mock_get_aspect

        result = json.loads(svc.get_sql_fragments("urn:li:dataset:test"))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["sql_fragment"], "SELECT 1")

    # ---------------------------------------------------------------
    # 4. get_query_templates — Template + Draft filtering
    # ---------------------------------------------------------------
    def test_get_query_templates_filters_tags(self, MockGraph):
        svc = self._make_service(MockGraph)
        svc.graph.get_urns_by_filter.return_value = [
            "urn:li:query:template_approved",
            "urn:li:query:template_draft",
            "urn:li:query:not_template",
        ]

        template_approved = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:Template")])
        template_draft = GlobalTagsClass(tags=[
            TagAssociationClass(tag="urn:li:tag:Template"),
            TagAssociationClass(tag="urn:li:tag:Draft"),
        ])
        no_template = GlobalTagsClass(tags=[])

        props = MagicMock(spec=QueryPropertiesClass)
        props.description = "Test template"
        props.statement = MagicMock()
        props.statement.value = "SELECT * FROM t1"

        def mock_get_aspect(urn, cls):
            if cls == GlobalTagsClass:
                if urn == "urn:li:query:template_approved":
                    return template_approved
                elif urn == "urn:li:query:template_draft":
                    return template_draft
                else:
                    return no_template
            return props

        svc.graph.get_aspect.side_effect = mock_get_aspect

        result = json.loads(svc.get_query_templates())
        # Only template_approved should appear (not draft, not non-template)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["parameterized_sql"], "SELECT * FROM t1")

    # ---------------------------------------------------------------
    # 5. get_business_terms — Draft filtering
    # ---------------------------------------------------------------
    def test_get_business_terms_excludes_draft(self, MockGraph):
        svc = self._make_service(MockGraph)
        svc.graph.get_urns_by_filter.return_value = [
            "urn:li:glossaryTerm:approved_term",
            "urn:li:glossaryTerm:draft_term",
        ]

        draft_tags = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:Draft")])
        no_tags = GlobalTagsClass(tags=[])
        info = MagicMock(spec=GlossaryTermInfoClass)
        info.name = "FY starts with feb"
        info.definition = "Fiscal year begins in February"

        def mock_get_aspect(urn, cls):
            if cls == GlobalTagsClass:
                return draft_tags if "draft" in urn else no_tags
            return info

        svc.graph.get_aspect.side_effect = mock_get_aspect

        result = json.loads(svc.get_business_terms())
        self.assertEqual(len(result), 1)
        self.assertIn("FY starts with feb", result[0])


if __name__ == "__main__":
    unittest.main()
