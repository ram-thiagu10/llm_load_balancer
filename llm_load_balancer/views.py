import json
from django.http import JsonResponse
from asgiref.sync import sync_to_async
from .models import LLMTask
from .queue import add_to_model_queue

async def submit_request(request):
    body = json.loads(request.body)
    model = body['model']
    task = await sync_to_async(LLMTask.objects.create)(
        model_name=model, request_payload=body
    )
    await add_to_model_queue(model, str(task.task_id))
    return JsonResponse({'task_id': str(task.task_id), 'status': 'accepted'}, status=202)

def callback_status(request):
    from django.shortcuts import get_object_or_404
    task_id = request.GET.get("task_id")
    task = get_object_or_404(LLMTask, task_id=task_id)
    if task.status == "COMPLETED":
        return JsonResponse({"status": "completed", "response": task.response})
    else:
        return JsonResponse({"status": "pending"})
