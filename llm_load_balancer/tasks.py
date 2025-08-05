from celery import shared_task
from .models import LLMTask

@shared_task(bind=True)
def process_model_batch(self, model_name, task_ids):
    tasks = LLMTask.objects.filter(task_id__in=task_ids)
    prompts = []
    for t in tasks:
        content = next((m["content"] for m in t.request_payload["messages"] if m["role"] == "user"), "")
        prompts.append(content)
    
    combined_prompt = "\n---\n".join(prompts)
    response = call_databricks_model(model_name, combined_prompt)

    for t, res in zip(tasks, split_response(response, len(tasks))):
        t.response = {"output": res}
        t.status = "COMPLETED"
        t.save()

def call_databricks_model(model_name, prompt):
    print(f"[Mock] Calling Databricks {model_name} with prompt:\n{prompt}")
    return {"content": prompt + "\n\n[LLM response here]"}

def split_response(response, count):
    parts = response["content"].split("---")
    return parts[:count]
