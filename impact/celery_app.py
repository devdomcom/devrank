from celery import Celery

from config import CELERY_BACKEND_URL, CELERY_BROKER_URL

app = Celery("impact", broker=CELERY_BROKER_URL, backend=CELERY_BACKEND_URL)
# Explicitly register fetch tasks module
app.autodiscover_tasks(["impact.tasks", "impact.tasks.fetch"])
