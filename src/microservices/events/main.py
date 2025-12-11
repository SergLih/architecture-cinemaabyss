import asyncio
import json
import logging
import os

from fastapi import FastAPI, Request, HTTPException
from starlette.responses import JSONResponse
from confluent_kafka import Producer, Consumer, KafkaError

app = FastAPI()

# Настройка логов
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("events-service")

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092").split(",")
TOPICS = ["movie-events", "user-events", "payment-events"]
PORT = int(os.environ.get("PORT", 8082))

producer = None
consumer = None
consumer_task = None

# Конфигурации Kafka
producer_conf = {
    'bootstrap.servers': ','.join(KAFKA_BROKERS),
}
consumer_conf = {
    'bootstrap.servers': ','.join(KAFKA_BROKERS),
    'group.id': 'events-service-group',
    'auto.offset.reset': 'earliest'
}

@app.on_event("startup")
async def startup_event():
    global producer, consumer, consumer_task
    # Инициализация Kafka producer
    producer = Producer(producer_conf)
    logger.info("Kafka producer initialized.")

    # Инициализация Kafka consumer
    consumer = Consumer(consumer_conf)
    consumer.subscribe(TOPICS)
    logger.info("Kafka consumer subscribed to topics.")

    # Запуск прослушки сообщений
    consumer_task = asyncio.create_task(consume())

@app.on_event("shutdown")
async def shutdown_event():
    global producer, consumer, consumer_task
    if consumer:
        consumer.close()
    if consumer_task:
        consumer_task.cancel()

async def consume():
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                await asyncio.sleep(0.1)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
            else:
                logger.info(f"Received from Kafka topic '{msg.topic()}': {msg.value().decode('utf-8')}")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Error in Kafka consumer: {e}")

async def send_to_kafka(topic: str, data: dict):
    def delivery_report(err, msg):
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def produce_message():
        producer.produce(topic, json.dumps(data).encode('utf-8'), callback=delivery_report)
        producer.flush()

    await asyncio.get_event_loop().run_in_executor(None, produce_message)

@app.post("/api/events/movie")
async def create_movie_event(request: Request):
    data = await request.json()
    await send_to_kafka("movie-events", data)
    return JSONResponse({"status": "success"}, status_code=201)

@app.post("/api/events/user")
async def create_user_event(request: Request):
    data = await request.json()
    await send_to_kafka("user-events", data)
    return JSONResponse({"status": "success"}, status_code=201)

@app.post("/api/events/payment")
async def create_payment_event(request: Request):
    data = await request.json()
    await send_to_kafka("payment-events", data)
    return JSONResponse({"status": "success"}, status_code=201)

@app.get("/api/events/health")
def health_check():
    return JSONResponse({"status": True})