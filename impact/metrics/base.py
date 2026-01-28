from abc import ABC, abstractmethod
from impact.domain.models import MetricContext, MetricResult


class Metric(ABC):
    @property
    @abstractmethod
    def slug(self) -> str:
        """Unique identifier for the metric."""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name."""
        pass

    @abstractmethod
    def run(self, context: MetricContext) -> MetricResult:
        """Run the metric and return the result."""
        pass