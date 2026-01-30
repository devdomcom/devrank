import json
import logging
import os

from impact.ingestion.base import Ingestion
from impact.adapters.registry import get_adapter
from impact.domain.models import CanonicalBundle
from impact.exceptions import ManifestError

log = logging.getLogger(__name__)


class DumpIngestion(Ingestion):
    """Ingests data from a filesystem dump directory."""

    def __init__(self, path: str):
        self.path = path

    def ingest(self) -> CanonicalBundle:
        """
        Load and parse a dump directory into a CanonicalBundle.

        Raises:
            ManifestError: If the manifest file is missing or invalid.
        """
        manifest_path = os.path.join(self.path, 'dump_manifest.json')

        if not os.path.exists(manifest_path):
            raise ManifestError(
                f"Manifest file not found at {manifest_path}",
                path=manifest_path
            )

        try:
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)
        except json.JSONDecodeError as e:
            raise ManifestError(
                f"Invalid JSON in manifest file: {e}",
                path=manifest_path
            ) from e

        provider = manifest.get('provider')
        if not provider:
            raise ManifestError(
                "Manifest missing required 'provider' field",
                path=manifest_path
            )

        log.info("Ingesting dump from %s (provider: %s)", self.path, provider)
        adapter = get_adapter(provider)
        return adapter.parse_dump(self.path)
