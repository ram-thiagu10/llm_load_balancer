## Directory: llm_batcher_project

### 1. app/main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from uuid import uuid4
import json, datetime
import pika
from sqlalchemy.exc import IntegrityError
from app.db import SessionLocal
from app.models import LLMAudit, ModelQueue

app = FastAPI()

class Message(BaseModel):
    role: str
    content: str

class InputRequest(BaseModel):
    messages: list[Message]
    model_name: str

@app.post("/v1/submit-request")
def submit_request(request: InputRequest):
    db = SessionLocal()
    try:
        task_id = str(uuid4())
        payload = {
            "task_id": task_id,
            "messages": [m.dict() for m in request.messages],
            "model_name": request.model_name
        }

        connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        channel = connection.channel()
        channel.queue_declare(queue=request.model_name, durable=True)
        channel.basic_publish(
            exchange='',
            routing_key=request.model_name,
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        connection.close()

        db.add(LLMAudit(
            task_id=task_id,
            model_name=request.model_name,
            request_payload=payload,
            status="PENDING",
            created_at=datetime.datetime.utcnow()
        ))
        try:
            db.add(ModelQueue(model_name=request.model_name))
            db.commit()
        except IntegrityError:
            db.rollback()

        return {"task_id": task_id, "status": "PENDING"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


### 2. app/models.py
from sqlalchemy import Column, String, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from app.db import Base

class LLMAudit(Base):
    __tablename__ = "llm_audit"
    __table_args__ = {'schema': 'swoosh'}

    task_id = Column(String, primary_key=True, index=True)
    model_name = Column(String(100), nullable=False)
    request_payload = Column(JSONB, nullable=False)
    response_payload = Column(JSONB, nullable=True)
    status = Column(String(20), nullable=False)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))

class ModelQueue(Base):
    __tablename__ = "model_queues"
    __table_args__ = {'schema': 'swoosh'}

    model_name = Column(String(100), primary_key=True)
    created_at = Column(DateTime(timezone=True), default=datetime.datetime.utcnow)


### 3. app/db.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import os

SQLALCHEMY_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(SQLALCHEMY_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


### 4. utils/databricks_client.py
import requests
import os

def call_databricks(query):
    payload = {"messages": [{"role": "user", "content": query}]}
    headers = {
        "Authorization": f"Bearer {os.getenv('DATABRICKS_TOKEN')}",
        "Content-Type": "application/json"
    }
    response = requests.post(os.getenv("DATABRICKS_ENDPOINT"), json=payload, headers=headers, verify=False)
    return response.json()


### 5. consumers/batch_consumer.py
import pika
import json
import time
import datetime
from sqlalchemy.orm import sessionmaker
from app.db import engine
from app.models import LLMAudit, ModelQueue
from utils.databricks_client import call_databricks

Session = sessionmaker(bind=engine)
RABBITMQ_HOST = "localhost"


def fetch_known_queues():
    db = Session()
    queues = db.query(ModelQueue).all()
    db.close()
    return [q.model_name for q in queues]

def process_batch(queue_name):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    batched = []

    for _ in range(3):
        method_frame, _, body = channel.basic_get(queue=queue_name, auto_ack=False)
        if method_frame:
            batched.append((method_frame.delivery_tag, json.loads(body)))

    if len(batched) == 3:
        combined_query = "\n---\n".join([m[1]["messages"][0]["content"] for m in batched])
        response = call_databricks(combined_query)

        db = Session()
        for tag, msg in batched:
            task = db.query(LLMAudit).filter(LLMAudit.task_id == msg["task_id"]).first()
            if task:
                task.response_payload = response
                task.status = "COMPLETED"
                task.updated_at = datetime.datetime.utcnow()
            channel.basic_ack(tag)
        db.commit()
        db.close()
    else:
        for tag, _ in batched:
            channel.basic_nack(tag, requeue=True)
    connection.close()

if __name__ == "__main__":
    while True:
        print("[Batch Worker] Polling queues...")
        queues = fetch_known_queues()
        for queue in queues:
            process_batch(queue)
        time.sleep(60)


### 6. requirements.txt
fastapi
uvicorn
sqlalchemy
psycopg2-binary
pika
requests
python-dotenv


### 7. .env (not committed to git)
DB_USER=your_db_user
DB_PASSWORD=your_db_pass
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_db
DATABRICKS_ENDPOINT=https://your-databricks-url
DATABRICKS_TOKEN=your_databricks_token
