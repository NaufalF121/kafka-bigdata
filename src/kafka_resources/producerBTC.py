from Producer import ProducerCoinDesk
import logging

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    logging.info("Bitcoin Producer is running")
    
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