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

    @property
    def signal_type(self) -> str:
        """Derived from plugin directory: 'authored', 'influence', or 'mixed'."""
        module = type(self).__module__
        if ".mixed." in module:
            return "mixed"
        if ".influence." in module:
            return "influence"
        return "authored"

    @property
    def frameworks(self) -> list[str]:
        """Framework tags for this metric (e.g., ['DORA', 'SPACE', 'Lean']).

        Override in subclasses to specify which frameworks this metric belongs to.
        See METRICS_OVERVIEW.md Framework Legend for valid values.
        """
        return []

    @abstractmethod
    def run(self, context: MetricContext) -> MetricResult:
        """Run the metric and return the result."""
        pass
