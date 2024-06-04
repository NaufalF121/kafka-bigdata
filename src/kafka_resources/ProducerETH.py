from Producer import ProducerCoinDesk

if __name__ == "__main__":
    etherium_producer = ProducerCoinDesk(
        url='https://api.coincap.io/v2/assets',
        params={
            'search' : 'ETH',
            'limit': 1
            },
        topic='etherium',
        bootstrap_servers='localhost:9092'
    )

    etherium_producer.run()