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

    @property
    @abstractmethod
    def description(self) -> str:
        """Short unique purpose (shows in report)."""
        pass

    @property
    @abstractmethod
    def category(self) -> str:
        """Category slug from impact.config.categories.CATEGORY_SLUGS."""
        pass

    @abstractmethod
    def run(self, context: MetricContext) -> MetricResult:
        """Run the metric and return the result."""
        pass
