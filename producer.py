import json as json
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from kafka import KafkaProducer
import sys, types

m = types.ModuleType('kafka.vendor.six.moves', 'Mock module')
setattr(m, 'range', range)
sys.modules['kafka.vendor.six.moves'] = m

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092']
    
)

producer.send('test', b'Hello, World!')
producer.send('test', b'Hello, Kafka!')
producer.flush()
