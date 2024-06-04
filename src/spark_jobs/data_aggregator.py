from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_trunc
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("KafkaDataAggregation") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
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

agg_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()


# TODO: Write the data storing into Postgres