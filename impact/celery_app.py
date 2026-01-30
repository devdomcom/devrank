import os
from celery import Celery

# Default host-facing broker ports avoid clashing with any local Redis on 6379.
# Worker overrides via environment (see docker-compose).
broker_url = os.getenv("CELERY_BROKER_URL", "redis://localhost:16379/0")
backend_url = os.getenv("CELERY_BACKEND_URL", "redis://localhost:16379/1")

app = Celery("impact", broker=broker_url, backend=backend_url)
# Explicitly register fetch tasks module
app.autodiscover_tasks(["impact.tasks", "impact.tasks.fetch"])
