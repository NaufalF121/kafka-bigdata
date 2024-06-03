from kafka import KafkaProducer
import requests
import json
import os
from dotenv import load_dotenv


TOPIC_NAME = 'test'
KAFKA_SERVER = 'localhost:9092'

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

load_dotenv()
api_url = os.getenv('ALPHA_API_KEY')


def get_feedback():
  # Send GET request with potential filter parameters
  response = requests.get(api_url)

  if response.status_code == 200:
    data = response.json()  # Assuming JSON response
    return data
  else:
    print(f"Error retrieving feedback: {response.status_code}")
    return None

# Get feedback data (assuming successful retrieval)
feedback_data = get_feedback()

if feedback_data:
  # Prepare and send data to Kafka if retrieved successfully
  payload = json.dumps(feedback_data)
  producer.send(TOPIC_NAME, payload.encode('utf-8'))
else:
  print("No feedback data retrieved")

producer.flush()

# producer.send(TOPIC_NAME, b'${"name": "John Doe", "age": 30}')
# producer.flush()