from abc import ABC, abstractmethod
from impact.domain.models import CanonicalBundle


class ProviderAdapter(ABC):
    @abstractmethod
    def parse_dump(self, dump_path: str) -> CanonicalBundle:
        """Parse the provider-specific dump and return a CanonicalBundle."""
        pass