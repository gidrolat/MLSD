import asyncio
import json

import aioredis
import yaml
from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from pydantic import BaseModel

with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

KAFKA_BROKER_URL = config["kafka"]["broker_url"]
QUEUE_X = config["kafka"]["queue_x"]
QUEUE_Y = config["kafka"]["queue_y"]
REDIS_URL = config.get("redis", {}).get("url", "redis://localhost")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

consumer = KafkaConsumer(
    QUEUE_Y,
    bootstrap_servers=KAFKA_BROKER_URL,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="fastapi-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

redis = aioredis.from_url(REDIS_URL, decode_responses=True)

app = FastAPI()


class InputData(BaseModel):
    value: str


async def consume_kafka():
    for message in consumer:
        result = message.value
        message_key = result.get("key")
        if message_key:
            await redis.set(message_key, json.dumps(result))


@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_event_loop()
    loop.create_task(consume_kafka())


@app.on_event("shutdown")
async def shutdown_event():
    producer.close()
    consumer.close()
    await redis.close()


@app.post("/process")
async def process_data(data: InputData):
    key = f"request-{hash(data.value)}"

    cached_result = await redis.get(key)
    if cached_result:
        return json.loads(cached_result)

    try:
        producer.send(QUEUE_X, {"key": key, "value": data.value})
        producer.flush()
    except KafkaError as e:
        raise HTTPException(status_code=500, detail=f"Kafka error: {str(e)}")

    for _ in range(config["response_timeout"]):
        cached_result = await redis.get(key)
        if cached_result:
            return json.loads(cached_result)
        await asyncio.sleep(1)

    raise HTTPException(status_code=504, detail="Timeout waiting for response")