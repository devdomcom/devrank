import json
import os
from impact.ingestion.base import Ingestion
from impact.adapters.registry import get_adapter
from impact.domain.models import CanonicalBundle


class DumpIngestion(Ingestion):
    def __init__(self, path: str):
        self.path = path

    def ingest(self) -> CanonicalBundle:
        manifest_path = os.path.join(self.path, 'dump_manifest.json')
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)
        provider = manifest['provider']
        adapter = get_adapter(provider)
        return adapter.parse_dump(self.path)