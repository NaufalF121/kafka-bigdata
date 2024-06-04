from confluent_kafka import Consumer, KafkaException
import json
import logging
from connection import Connection
import os

# Configuration
TOPIC = "bitcoin"
BOOTSTRAP_SERVERS = 'localhost:9092'
GROUP_ID = 'mygroup'
QUERY = ""

# Set up logging
logging.basicConfig(level=logging.INFO)

def create_consumer():
    return Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    })

def consume_message(consumer,conn):
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
            conn.execute(QUERY)
            

        except json.JSONDecodeError:
            logging.error("Failed to decode JSON message")
            



def main():
    conn = Connection(host=os.getenv("HOST"), port=os.getenv("PORT"), database=os.getenv("DB_NAME"), user=os.getenv("USER"), password=os.getenv("PASSWORD"))
    conn.connect()
    consumer = create_consumer()
    consumer.subscribe([TOPIC])
    consume_message(consumer, conn=conn)
    conn.close()


if __name__ == "__main__":
    main()