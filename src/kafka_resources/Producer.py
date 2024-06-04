import json
import time
import requests
import logging
import datetime
from confluent_kafka import Producer

# Set up logging
logging.basicConfig(level=logging.INFO)

class ProducerCoinDesk:
    def __init__(self, url, params, topic, bootstrap_servers, sleep_time=60):
        self.url = url
        self.params = params
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.sleep_time = sleep_time
        self.producer = Producer({'bootstrap.servers': self.bootstrap_servers})

    def get_data(self):
        try:
            r = requests.get(self.url, params=self.params)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return None

    def process_data(self, data):
        dataWithType = data['data'][0]
        dataWithType['type'] = 'cryptocurrency'
        dataWithType['timestamp'] = datetime.datetime.now().isoformat()
        return json.dumps(dataWithType).encode('utf-8')

    def send_to_kafka(self, message):
        self.producer.produce(self.topic, message)
        self.producer.flush()

    def run(self):
        while True: 
            data = self.get_data()
            if data is not None:
                message = self.process_data(data)
                self.send_to_kafka(message)
            time.sleep(self.sleep_time)