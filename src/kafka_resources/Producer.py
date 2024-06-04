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
    This class is used to produce cryptocurrency data from the CoinCap API to Kafka.

    Attributes:
        url (str): The URL of the CoinCap API. Default is 'https://api.coincap.io/v2/assets'.
        params (dict): The parameters for the API request. Default is {'search' : 'BTC', 'limit': 1}.
        topic (str): The Kafka topic to send the data to. Default is 'bitcoin'.
        bootstrap_servers (str): The address of the Kafka server. Default is 'localhost:9092'.

    Example:
        ProducerCoinDesk(
            url='https://api.coincap.io/v2/assets',
            params={
                'search' : 'BTC',
                'limit': 1
            },
            topic='bitcoin',
            bootstrap_servers='localhost:9092'
        ).run()

    This will produce a message to Kafka with the following format:
        {
            "data": [
                {
                    "id": "bitcoin",
                    "rank": "1",
                    "symbol": "BTC",
                    "name": "Bitcoin",
                    "supply": "18750712.0000000000000000",
                    "maxSupply": "21000000.0000000000000000",
                    "marketCapUsd": "1091650810954.6500000000000000",
                    "volumeUsd24Hr": "17720200000.0000000000000000",
                    "priceUsd": "58289.0000000000000000",
                    "changePercent24Hr": "1.0572141421282580",
                    "vwap24Hr": "57986.6350136187250000"
                }
            ],
            "timestamp": 1622712000000
        }
    """
    def __init__(self, params, topic, bootstrap_servers, refresh_time=59, url='https://api.coincap.io/v2/assets'):
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

    def job(self):
        data = self.get_data()
        if data is not None:
            message = self.process_data(data)
            self.send_to_kafka(message)
            logging.info(f"Message sent to Kafka: {message}")

    def run(self):
        self.job()

        schedule.every(1).minutes.do(self.job)

        while True:
            schedule.run_pending()
            time.sleep(1)

class ProducerFrankfurter:
    """
    This class is used to produce exchange rate data from the Frankfurter API to Kafka.

    Attributes:
        from_currency (str): The base currency for the exchange rate. Default is 'USD'.
        to_currency (str): The target currency for the exchange rate. Default is 'SGD'.
        amount (float): The amount of the base currency to convert. Default is 1.
        topic (str): The Kafka topic to send the data to. Default is 'exchange_rate'.
        bootstrap_servers (str): The address of the Kafka server. Default is 'localhost:9092'.

    Example:
        ProducerFrankfurter(
            from_currency='USD',
            to_currency='SGD',
            amount=1,
            topic='exchange_rate',
            bootstrap_servers='localhost:9092'
        ).run()

    This will produce a message to Kafka with the following format:
        {
            "amount": 1.0,
            "base": "USD",
            "date": "2024-06-03",
            "rates": {
                "SGD": 1.3501
            }
        }
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
            logging.info(f"Message sent to Kafka: {message}")

    def run(self):
        self.job()

        schedule.every(1).minutes.do(self.job)

        while True:
            schedule.run_pending()
            time.sleep(1)