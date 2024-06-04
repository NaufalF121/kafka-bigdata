from Producer import ProducerFrankfurter

if __name__ == "__main__":
    exchange_rate_producer = ProducerFrankfurter(
        from_currency='USD',
        to_currency='IDR',
        ammount=1,
        topic='exchange_rate',
        bootstrap_servers='localhost:9092'
    )

    exchange_rate_producer.run()