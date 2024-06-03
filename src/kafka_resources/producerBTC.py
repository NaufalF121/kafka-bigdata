import os
import finnhub
import json as json
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from kafka import KafkaProducer
import sys, types


url = 'https://api.coincap.io/v2/assets'
m = types.ModuleType('kafka.vendor.six.moves', 'Mock module')
setattr(m, 'range', range)
sys.modules['kafka.vendor.six.moves'] = m

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092']
    
)
params = {
    'search' : 'BTC',
    'limit': 1
}

TOPIC = "test"

while True: 

    r = requests.get(url, params=params)
    data = r.json()
    print("Cryptocurrency")
    print(data['data'][0]['name'])
    print(data['data'][0]['priceUsd'])
    dataWithType = data['data'][0]
    dataWithType['type'] = 'cryptocurrency'
    producer.send(TOPIC, json.dumps(dataWithType).encode('utf-8'))
    time.sleep(31)

producer.flush()
    
