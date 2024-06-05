from viz import chart
import logging
import json
import streamlit as st
from kafka_resources.Consumer import ConsumerBTC
import time
from confluent_kafka import Consumer

TOPIC = "bitcoin"
BOOTSTRAP_SERVERS = 'localhost:9092'
GROUP_ID = 'mygroup'
data = []

# Set up logging
logging.basicConfig(level=logging.INFO)

def create_consumer():
    return Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    })

def consume_message(consumer):
    st.title("Bitcoin Price")
    st.write("This chart shows the price of Bitcoin over time.")
    placeholder = st.empty()
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            logging.error(f"Consumer error: {msg.error()}")
            continue

        try:
            message = json.loads(msg.value().decode('utf-8'))
            logging.info(message)
            with placeholder.container():
                kpi1, kpi2 = st.columns(2)
                kpi1.metric("Price", message["priceUsd"], "USD")
                kpi2.metric("Market Cap", message["marketCapUsd"], "USD")                
                time.sleep(10)

            
        except json.JSONDecodeError:
            logging.error("Failed to decode JSON message")


def app(data, x: str, y: str):
    chart.LineChart(data, x, y)

if __name__ == "__main__":
    consumer = create_consumer()
    consumer.subscribe([TOPIC])
    consume_message(consumer)