# join a consumer group for dynamic partition assignment and offset commits
from kafka import KafkaConsumer
import json
TOPIC = "test"
consumer = KafkaConsumer(TOPIC)


for msg in consumer:
    print (msg.value.decode('utf-8'))
    message = json.loads(msg.value.decode())
    if message["type"] == "cryptocurrency":
        print("Cryptocurrency")
        print(message['name'])
        print(message['priceUsd'])
    else:
        print("Stock")
        print("Stock BBCA")
        print(message['c'])
    

