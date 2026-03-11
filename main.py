"""
DataHub Integration — CLI Entry Point

Usage:
    python main.py <command> [options]

Commands:
    ingest-tables       Ingest table/column definitions from YAML
    ingest-templates    Ingest query templates from YAML
    ingest-terms        Ingest business glossary terms from YAML
    list-tables         List tables in a database
    list-columns        List columns of a dataset
    sql-fragments       Get SQL fragments for a dataset
    query-templates     Get global query templates
    business-terms      Get approved business terms
    term-history        Get version history of a glossary term
"""
import argparse
import sys

import config


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="datahub-integration",
        description="DataHub metadata ingestion & query CLI",
    )
    sub = parser.add_subparsers(dest="command", help="Available commands")

    # ----- Ingestion commands -----
    p_it = sub.add_parser("ingest-tables", help="Ingest tables from YAML")
    p_it.add_argument("--file", default="data/table_cols.yaml", help="Path to tables YAML")
    p_it.add_argument("--platform", default=None, help="Data platform (e.g. postgres)")
    p_it.add_argument("--env", default=None, help="Environment (PROD/DEV/STAGING)")
    p_it.add_argument("--db", default=None, help="Database name (e.g. mydb)")
    p_it.add_argument("--schema", default=None, help="Schema name (e.g. public)")
    p_it.add_argument("--dry-run", action="store_true", help="Parse YAML only, don't emit")

    p_iq = sub.add_parser("ingest-templates", help="Ingest query templates from YAML")
    p_iq.add_argument("--file", default="data/query_templates.yaml", help="Path to templates YAML")
    p_iq.add_argument("--dry-run", action="store_true", help="Parse YAML only, don't emit")

    p_ib = sub.add_parser("ingest-terms", help="Ingest business terms from YAML")
    p_ib.add_argument("--file", default="data/business_terms.yaml", help="Path to terms YAML")
    p_ib.add_argument("--dry-run", action="store_true", help="Parse YAML only, don't emit")

    # ----- Fetch commands -----
    p_lt = sub.add_parser("list-tables", help="List tables in a database")
    p_lt.add_argument("--platform", required=True, help="Data platform (e.g. postgres)")
    p_lt.add_argument("--db", default=None, help="Database name (optional filter)")
    p_lt.add_argument("--schema", default=None, help="Schema name (optional filter)")

    p_lc = sub.add_parser("list-columns", help="List columns for a dataset")
    p_lc.add_argument("--urn", required=True, help="Dataset URN")

    p_sf = sub.add_parser("sql-fragments", help="Get SQL fragments for a dataset")
    p_sf.add_argument("--urn", required=True, help="Dataset URN")

    sub.add_parser("query-templates", help="Get global query templates")

    p_bt = sub.add_parser("business-terms", help="Get approved business terms")
    p_bt.add_argument("--group", default=None, help="Filter by term group name (e.g. Finance)")

    p_th = sub.add_parser("term-history", help="Get version history of a term")
    p_th.add_argument("--urn", required=True, help="Glossary term URN")
    p_th.add_argument("--versions", type=int, default=5, help="Max versions to fetch")

    return parser


def main():
    parser = _build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    # ---- Ingestion ----
    if args.command == "ingest-tables":
        from etl.ingest import ingest_tables
        ingest_tables(args.file, platform=args.platform, env=args.env, db=args.db, schema=args.schema, dry_run=args.dry_run)

    elif args.command == "ingest-templates":
        from etl.ingest import ingest_query_templates
        ingest_query_templates(args.file, dry_run=args.dry_run)

    elif args.command == "ingest-terms":
        from etl.ingest import ingest_business_terms
        ingest_business_terms(args.file, dry_run=args.dry_run)

    # ---- Fetch ----
    elif args.command in ("list-tables", "list-columns", "sql-fragments",
                          "query-templates", "business-terms"):
        from services.metadata_service import DataHubMetadataService
        svc = DataHubMetadataService(
            server=config.DATAHUB_GMS_SERVER,
            token=config.DATAHUB_TOKEN or None,
        )
        if args.command == "list-tables":
            print(svc.list_tables(args.platform, db_name=args.db, schema_name=args.schema))
        elif args.command == "list-columns":
            print(svc.list_columns(args.urn))
        elif args.command == "sql-fragments":
            print(svc.get_sql_fragments(args.urn))
        elif args.command == "query-templates":
            print(svc.get_query_templates())
        elif args.command == "business-terms":
            print(svc.get_business_terms(term_group=args.group))

    elif args.command == "term-history":
        from services.version_history import VersionHistoryService
        svc = VersionHistoryService(
            server=config.DATAHUB_GMS_SERVER,
            token=config.DATAHUB_TOKEN or None,
        )
        print(svc.get_term_history(args.urn, max_versions=args.versions))

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
