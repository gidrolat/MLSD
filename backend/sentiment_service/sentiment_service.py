import asyncio
import hashlib
import json
import logging
import aioredis
import yaml
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


# Функция для хеширования строки
def hash_string(input_string):
    return hashlib.sha256(input_string.encode('utf-8')).hexdigest()


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Загрузка конфигурации
with open("config/config.yaml", "r") as file:
    LOGGER.info("Loading config")
    config = yaml.safe_load(file)

KAFKA_BROKER_URL = config["kafka"]["broker_url"]
OUT_QUERY = config["kafka"]["output_query"]
IN_QUERY = config["kafka"]["input_query"]
RESPONSE_TIMEOUT = config.get("response_timeout", 10)
REDIS_URL = config["redis"]["url"]

app = FastAPI()

producer = None
consumer = None
redis = None  # Redis клиент

# Глобальный словарь для хранения ожиданий запросов
pending_requests = {}


class InputData(BaseModel):
    value: str


@app.on_event("startup")
async def startup_event():
    global producer, consumer, redis
    LOGGER.info("Starting up")

    # Настраиваем Kafka Producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()

    # Настраиваем Kafka Consumer
    consumer = AIOKafkaConsumer(
        IN_QUERY,
        bootstrap_servers=KAFKA_BROKER_URL,
        group_id="sentiment-analysis-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    await consumer.start()

    # Настроим Redis
    redis = aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)

    # Создаем фоновую задачу для обработки сообщений из Kafka
    asyncio.create_task(consume_kafka())


@app.on_event("shutdown")
async def shutdown_event():
    global producer, consumer, redis
    LOGGER.info("Shutting down")

    # Останавливаем Kafka Producer и Consumer
    await producer.stop()
    await consumer.stop()

    # Закрываем соединение с Redis
    await redis.close()


async def consume_kafka():
    """Потребляем сообщения из Kafka и уведомляем запросы."""
    try:
        async for message in consumer:
            LOGGER.info(f"Received message: {message.value}")
            result = message.value
            key = result.get("key")

            if key:
                # Уведомляем ожидающий запрос
                future = pending_requests.pop(key, None)
                if future and not future.done():
                    future.set_result(result)
    except Exception as e:
        LOGGER.error(f"Error in Kafka consumer: {str(e)}")


@app.post("/process")
async def process_data(data: InputData):
    LOGGER.info(f"Processing data: {data.value}")
    key = f"request-{hash_string(data.value)}"

    # Проверяем, есть ли результат в Redis
    cached_result = await redis.get(key)
    if cached_result:
        LOGGER.info("Returning cached result")
        return json.loads(cached_result)

    # Отправляем сообщение в Kafka
    try:
        await producer.send_and_wait(OUT_QUERY, {"key": key, "value": data.value})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kafka error: {str(e)}")

    # Ждем ответа от Kafka через Future
    future = asyncio.get_event_loop().create_future()
    pending_requests[key] = future

    try:
        result = await asyncio.wait_for(future, timeout=RESPONSE_TIMEOUT)

        # Кешируем результат в Redis
        await redis.setex(key, 60, json.dumps(result))  # Кешируем на 60 секунд
        return result
    except asyncio.TimeoutError:
        pending_requests.pop(key, None)
        raise HTTPException(status_code=504, detail="Timeout waiting for response")
