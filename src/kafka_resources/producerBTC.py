import json
import time
import requests
from confluent_kafka import Producer

# Constants
URL = 'https://api.coincap.io/v2/assets'
PARAMS = {
    'search' : 'BTC',
    'limit': 1
}
TOPIC = "test"
BOOTSTRAP_SERVERS = 'localhost:9092'

def main():
    producer = Producer(
        {
            'bootstrap.servers': BOOTSTRAP_SERVERS,
        }
    )

    while True: 
        try:
            r = requests.get(URL, params=PARAMS)
            r.raise_for_status()
            data = r.json()
            dataWithType = data['data'][0]
            dataWithType['type'] = 'cryptocurrency'
            producer.produce(TOPIC, json.dumps(dataWithType).encode('utf-8'))
            time.sleep(10)
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

    producer.flush()

if __name__ == "__main__":
    main()