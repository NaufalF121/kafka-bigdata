from Producer import ProducerCoinDesk

if __name__ == "__main__":
    bitcoin_producer = ProducerCoinDesk(
        url='https://api.coincap.io/v2/assets',
        params={
            'search' : 'BTC',
            'limit': 1
            },
        topic='bitcoin',
        bootstrap_servers='localhost:9092'
    )

    bitcoin_producer.run()