"""
Version History Service — retrieves historical versions of DataHub aspects.

DataHub is event-sourced: every metadata change is automatically versioned.
This module exposes helpers to inspect that history.
"""
import json
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import GlossaryTermInfoClass


class VersionHistoryService:
    """Retrieve historical aspect versions from DataHub."""

    def __init__(self, server: str, token: str | None = None):
        cfg = DatahubClientConfig(server=server, token=token)
        self.graph = DataHubGraph(cfg)

    def get_term_history(self, term_urn: str, max_versions: int = 5) -> str:
        """
        Fetch the version history of a Glossary Term's definition.

        DataHub stores aspect versions as version 0 (latest), 1 (previous), etc.
        This method walks backwards through versions and returns a list of
        historical definitions.

        Args:
            term_urn: The URN of the glossary term
                      (e.g. urn:li:glossaryTerm:fy_starts_with_feb)
            max_versions: Maximum number of historical versions to retrieve.

        Returns:
            JSON string with an array of version records.
        """
        history = []

        for version in range(max_versions):
            try:
                info: GlossaryTermInfoClass | None = self.graph.get_aspect(
                    entity_urn=term_urn,
                    aspect_type=GlossaryTermInfoClass,
                    version=version,
                )
                if info:
                    history.append({
                        "version": version,
                        "name": info.name,
                        "definition": info.definition,
                        "is_latest": version == 0,
                    })
                else:
                    # No more versions available
                    break
            except Exception:
                # Version doesn't exist — stop iterating
                break

        if not history:
            return json.dumps({"error": f"No history found for {term_urn}"})

        return json.dumps(history, indent=2)
