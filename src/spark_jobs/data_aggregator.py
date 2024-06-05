from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_trunc, udf
import uuid
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

import logging

logging.basicConfig(level=logging.INFO)

def migrate_keyspace(keyspace_name):
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(['127.0.0.1'], auth_provider=auth_provider)
    session = cluster.connect()

    rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
    if keyspace_name not in [row[0] for row in rows]:
        session.execute(f"""
            CREATE KEYSPACE {keyspace_name} WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor' : 3 }}
        """)

    session.shutdown()
    cluster.shutdown()

def create_table(table_name, keyspace_name):
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(['127.0.0.1'], auth_provider=auth_provider)
    session = cluster.connect(keyspace_name)

    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TIMESTAMP,
            id UUID,
            price_usd DOUBLE,
            supply DOUBLE,
            market_cap_usd DOUBLE,
            exchange_rate DOUBLE,
            PRIMARY KEY ((timestamp), id)
        ) WITH CLUSTERING ORDER BY (id ASC)
    """)

    session.shutdown()
    cluster.shutdown()

spark = SparkSession.builder \
    .appName("KafkaDataAggregation") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
    .config("spark.cassandra.output.consistency.level", "ONE") \
    .config("spark.sql.streaming.schemaInference", True) \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True) \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .getOrCreate()

crypto_schema = StructType([
    StructField("id", StringType()),
    StructField("rank", StringType()),
    StructField("symbol", StringType()),
    StructField("name", StringType()),
    StructField("supply", StringType()),
    StructField("maxSupply", StringType()),
    StructField("marketCapUsd", StringType()),
    StructField("volumeUsd24Hr", StringType()),
    StructField("priceUsd", StringType()),
    StructField("changePercent24Hr", StringType()),
    StructField("vwap24Hr", StringType()),
    StructField("timestamp", TimestampType())
])

crypto_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bitcoin") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), crypto_schema).alias("crypto")) \
    .select("crypto.*") \
    .withWatermark("timestamp", "1 minute")

exchange_schema = StructType([
    StructField("amount", DoubleType()),
    StructField("base", StringType()),
    StructField("date", StringType()),
    StructField("rates", StringType()),
    StructField("type", StringType()),
    StructField("timestamp", TimestampType())
])

exchange_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "exchange_rate") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), exchange_schema).alias("exchange")) \
    .select("exchange.*") \
    .withWatermark("timestamp", "1 minute")

exchange_df = exchange_df.withColumn("rates", from_json(col("rates"), StructType([StructField("IDR", DoubleType())]))["IDR"])

crypto_df = crypto_df.withColumn("timestamp", date_trunc("minute", "timestamp"))
exchange_df = exchange_df.withColumn("timestamp", date_trunc("minute", "timestamp"))

agg_df = crypto_df.join(exchange_df, "timestamp")

agg_df = agg_df.select("timestamp", "priceUsd", "supply", "marketCapUsd", "rates")

agg_df = agg_df.withColumnRenamed("priceUsd", "price_usd")
agg_df = agg_df.withColumnRenamed("supply", "supply")
agg_df = agg_df.withColumnRenamed("marketCapUsd", "market_cap_usd")
agg_df = agg_df.withColumnRenamed("rates", "exchange_rate")

uuidUdf= udf(lambda : str(uuid.uuid4()), StringType())
agg_df = agg_df.withColumn("id", uuidUdf())

def write_to_cassandra(df, epoch_id):
    if not df.rdd.isEmpty():
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="bitcoin_data", keyspace="crypto") \
            .mode("append") \
            .save()
        logging.info(f"Data written to Cassandra for epoch_id: {epoch_id}")
    
    else:
        logging.info(f"DataFrame is empty for epoch_id: {epoch_id}")

migrate_keyspace("crypto")
create_table("bitcoin_data", "crypto")

agg_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_cassandra) \
    .trigger(processingTime="10 seconds") \
    .start() \
    .awaitTermination()
