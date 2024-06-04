from Producer import ProducerFrankfurter
import logging

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    logging.info("Exchange Rate Producer is running")
    
    exchange_rate_producer = ProducerFrankfurter(
        from_currency='USD',
        to_currency='IDR',
        ammount=1,
        topic='exchange_rate',
        bootstrap_servers='localhost:9092'
    )

    exchange_rate_producer.run()