import json
import schedule
import time
import requests
import logging
import datetime
from confluent_kafka import Producer

# Set up logging
logging.basicConfig(level=logging.INFO)

class ProducerCoinDesk:
    """
    This class is used to produce cryptocurrency data from CoinCap API to Kafka.
    """
    def __init__(self, url, params, topic, bootstrap_servers, refresh_time=59):
        self.url = url
        self.params = params
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.refresh_time = refresh_time
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
            current_time = datetime.datetime.now()
            if current_time.second == self.refresh_time: # Run the job every 60 seconds each refresh_time
                data = self.get_data()
                if data is not None:
                    message = self.process_data(data)
                    self.send_to_kafka(message)
                time.sleep(60 - current_time.second)
            else:
                time.sleep(0.1)

class ProducerFrankfurter:
    """
    This class is used to produce exchange rate data from Frankfurter API to Kafka.
    """
    def __init__(self, from_currency, to_currency, ammount, topic, bootstrap_servers, refresh_time=59):
        self.from_currency = from_currency
        self.to_currency = to_currency
        self.ammount = ammount
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.refresh_time = refresh_time
        self.producer = Producer({'bootstrap.servers': self.bootstrap_servers})
    
    def get_data(self):
        try:
            url = f"https://api.frankfurter.app/latest?amount={self.ammount}&from={self.from_currency}&to={self.to_currency}"
            r = requests.get(url)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return None
        
    def process_data(self, data):
        dataWithType = data
        dataWithType['type'] = 'exchange_rate'
        dataWithType['timestamp'] = datetime.datetime.now().isoformat()
        return json.dumps(dataWithType).encode('utf-8')
    
    def send_to_kafka(self, message):
        self.producer.produce(self.topic, message)
        self.producer.flush()

    def job(self):
        data = self.get_data()
        if data is not None:
            message = self.process_data(data)
            self.send_to_kafka(message)

    def run(self):
        self.job()

        schedule.every().day.at("00:00").do(self.job)

        while True:
            schedule.run_pending()
            time.sleep(1)