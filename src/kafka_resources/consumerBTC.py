from confluent_kafka import Consumer, KafkaException
import json
import logging

# Configuration
TOPIC = "bitcoin"
BOOTSTRAP_SERVERS = 'localhost:9092'
GROUP_ID = 'mygroup'

# Set up logging
logging.basicConfig(level=logging.INFO)

def create_consumer():
    return Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    })

def consume_message(consumer):
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            logging.error(f"Consumer error: {msg.error()}")
            continue

        try:
            message = json.loads(msg.value().decode('utf-8'))
            logging.info(message)
        except json.JSONDecodeError:
            logging.error("Failed to decode JSON message")

def main():
    consumer = create_consumer()
    consumer.subscribe([TOPIC])
    consume_message(consumer)

if __name__ == "__main__":
    main()