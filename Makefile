# Run producer for BTC
producer-btc:
	@echo "Running producer for BTC"
	@python ./src/kafka_resources/producerBTC.py

# Run producer for Exchange Rate
producer-exchange-rate:
	@echo "Running producer for Exchange Rate"
	@python ./src/kafka_resources/producerExchangeRate.py

# Run spark job for Data Aggregation
spark-aggregator:
	@echo "Running spark job for Data Aggregation"
	@python ./src/spark_jobs/data_aggregator.py