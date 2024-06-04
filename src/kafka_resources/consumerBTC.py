from confluent_kafka import Consumer
import json

# Constants
TOPIC = "test"
BOOTSTRAP_SERVERS = 'localhost:9092'
GROUP_ID = 'mygroup'

def main():
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([TOPIC])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            message = json.loads(msg.value().decode('utf-8'))
            if message["type"] == "cryptocurrency":
                print(message['priceUsd'])
        except json.JSONDecodeError:
            print("Failed to decode JSON message")

if __name__ == "__main__":
    main()