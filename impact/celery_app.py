from celery import Celery

from config import settings

# Celery app uses settings from Pydantic (DEVRANK_CELERY_* env vars standardized).
# This aligns with DRY config and supports docker env overrides.
app = Celery(
    "impact",
    broker=settings.celery_broker_url,
    backend=settings.celery_backend_url,
)
# Explicitly register fetch tasks module
app.autodiscover_tasks(["impact.tasks", "impact.tasks.fetch"])
