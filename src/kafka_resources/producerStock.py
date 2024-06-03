import json as json
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from kafka import KafkaProducer
import sys, types
import finnhub
import os


m = types.ModuleType('kafka.vendor.six.moves', 'Mock module')
setattr(m, 'range', range)
sys.modules['kafka.vendor.six.moves'] = m

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092']
    
)
TOPIC = "test"
API_KEY = os.getenv("API_KEY")
finnhub_client = finnhub.Client(api_key=API_KEY)
while True: 

    print("Stock")
    print(finnhub_client.quote('PBCRF'))
    message = finnhub_client.quote('PBCRF')
    message['type'] = 'stock'
    producer.send(TOPIC, json.dumps(message).encode('utf-8'))
    time.sleep(86400)

producer.flush()
