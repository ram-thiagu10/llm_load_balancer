import uuid
from django.db import models

class LLMTask(models.Model):
    task_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    model_name = models.CharField(max_length=200)
    request_payload = models.JSONField()
    status = models.CharField(max_length=20, default='PENDING')
    response = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
