from collections import defaultdict
from celery import current_app

model_queues = defaultdict(list)

async def add_to_model_queue(model_name, task_id):
    model_queues[model_name].append(task_id)
    if len(model_queues[model_name]) >= 3:
        batch = model_queues[model_name][:3]
        del model_queues[model_name][:3]
        queue_name = f"model_queue_{model_name}"
        current_app.send_task(
            'llm_load_balancer.tasks.process_model_batch',
            args=[model_name, batch],
            queue=queue_name
        )
