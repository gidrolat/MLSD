import hashlib
import json
import logging

import nltk
import yaml
from confluent_kafka import Consumer, Producer
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Загрузка словарей VADER
nltk.download("vader_lexicon")

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Инициализация VADER
analyzer = SentimentIntensityAnalyzer()


def hash_string(input_string):
    return hashlib.sha256(input_string.encode('utf-8')).hexdigest()


def load_config(config_path: str) -> dict:
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        logging.error(f"Error loading config: {e}")
        raise


def analyze_sentiment(text: str) -> dict:
    scores = analyzer.polarity_scores(text)
    sentiment = "neutral"
    if scores["compound"] >= 0.05:
        sentiment = "positive"
    elif scores["compound"] <= -0.05:
        sentiment = "negative"
    return {"text": text, "sentiment": sentiment, "scores": scores}


def consume_and_process(svc_config: dict):
    kafka_config = svc_config["kafka"]

    consumer_conf = {
        "bootstrap.servers": kafka_config["broker"],
        "group.id": kafka_config["group_id"],
        "auto.offset.reset": kafka_config["auto_offset_reset"],
    }

    producer_conf = {"bootstrap.servers": kafka_config["broker"]}

    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    consumer.subscribe([kafka_config["topics"]["input"]])

    logging.info("Sentiment analysis service started")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logging.error(f"Kafka error: {msg.error()}")
                continue

            message = msg.value().decode("utf-8")
            logging.info(f"Received message: {message}")
            try:
                data = json.loads(message)
                text = data.get("value", "")
                if not text:
                    logging.warning("Empty text in message")
                    continue

                result = analyze_sentiment(text)
                logging.info(f"Sentiment analysis result: {result}")
                result['key'] = f"request-{hash_string(result['text'])}"
                producer.produce(kafka_config["topics"]["output"], json.dumps(result))
                producer.flush()

            except Exception as e:
                logging.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logging.info("Service interrupted by user")
    finally:
        consumer.close()


if __name__ == "__main__":
    config = load_config("config/config.yaml")
    consume_and_process(config)
