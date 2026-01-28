from impact.adapters.base import ProviderAdapter
from impact.adapters.github import GitHubAdapter


def get_adapter(provider: str) -> ProviderAdapter:
    if provider == "github":
        return GitHubAdapter()
    else:
        raise ValueError(f"Unsupported provider: {provider}")