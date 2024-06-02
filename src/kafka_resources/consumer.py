# join a consumer group for dynamic partition assignment and offset commits
from kafka import KafkaConsumer
TOPIC = "test"
consumer = KafkaConsumer(TOPIC)


for msg in consumer:
    print (msg.value)
