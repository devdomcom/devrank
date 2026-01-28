from abc import ABC, abstractmethod
from impact.domain.models import CanonicalBundle


class Ingestion(ABC):
    @abstractmethod
    def ingest(self) -> CanonicalBundle:
        """Ingest data from the source and return a CanonicalBundle."""
        pass