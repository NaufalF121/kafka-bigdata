import os
import finnhub
import json as json
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from kafka import KafkaProducer
import sys, types

API_KEY = os.getenv("API_KEY")
finnhub_client = finnhub.Client(api_key=API_KEY)
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

while True: 

    r = requests.get(url, params=params)
    data = r.json()
    print("Cryptocurrency")
    print(data['data'][0]['name'])
    print(data['data'][0]['priceUsd'])
    producer.send('test', json.dumps(data['data'][0]).encode('utf-8'))
    # print("Stock")
    # print(finnhub_client.quote('PBCRF'))
    time.sleep(31)
    