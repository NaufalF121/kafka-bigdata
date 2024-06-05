import streamlit as st
from confluent_kafka import Consumer, KafkaError
import json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import numpy as np

BTC_TOPIC = "bitcoin"
EXCHANGE_TOPIC = "exchange_rate"

def consume_kafka_message(topic):
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic])

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    continue
                else:
                    print("Consumer error: {}".format(message.error()))
                    break

            yield json.loads(message.value().decode('utf-8'))

    except KeyboardInterrupt:
        print("Interrupted by user, closing consumer.")
    finally:
        consumer.close()



def visualize_btc_data():
    btc_data_generator = consume_kafka_message(BTC_TOPIC)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Supply")
        supply = st.empty()
        max_supply = st.empty()
        market_cap_usd = st.empty()
    with col2:
        st.subheader("Volume")
        volume_usd_24hr = st.empty()
        price_usd = st.empty()
        change_percent_24hr = st.empty()
        vwap_24hr = st.empty()

    for btc_data in btc_data_generator:
        if btc_data is None:
            st.write("No data available")
            return

        with col1:
            supply.metric("Supply", btc_data["supply"])
            max_supply.metric("Max Supply", btc_data["maxSupply"])
            market_cap_usd.metric("Market Cap USD", btc_data["marketCapUsd"])
        with col2:
            volume_usd_24hr.metric("Volume USD 24Hr", btc_data["volumeUsd24Hr"])
            price_usd.metric("Price USD", btc_data["priceUsd"])
            change_percent_24hr.metric("Change Percent 24Hr", btc_data["changePercent24Hr"])
            vwap_24hr.metric("VWAP 24Hr", btc_data["vwap24Hr"])


def visualize_history_data():
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(['127.0.0.1'], auth_provider=auth_provider)
    session = cluster.connect()

    rows = session.execute("SELECT timestamp, price_usd, supply, market_cap_usd, exchange_rate FROM crypto.bitcoin_data")

    rows = list(rows)
    if not rows:
        st.write("No data available")
        return

    df = pd.DataFrame(rows, columns=["timestamp", "price_usd", "supply", "market_cap_usd", "exchange_rate"])
    df.set_index('timestamp', inplace=True)
    df.index = pd.to_datetime(df.index)
    df.index = df.index.tz_localize(None)
    df.sort_index(inplace=True, ascending=False)

    st.write(df)

    session.shutdown()
    cluster.shutdown()

def mainbar():
    st.set_page_config(
        page_title="Final Project - Big Data",
        page_icon=":bar_chart:",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    st.title("Streaming Bitcoin Analytics")


def app():
    mainbar()
    visualize_history_data()
    visualize_btc_data()


if __name__ == "__main__":
    app()