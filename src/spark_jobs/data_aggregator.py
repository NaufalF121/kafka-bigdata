from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("KafkaDataAggregation") \
    .master("local[*]") \
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
    StructField("timestamp", StringType())
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
    .withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))


exchange_schema = StructType([
    StructField("amount", DoubleType()),
    StructField("base", StringType()),
    StructField("date", StringType()),
    StructField("rates", StringType()),
    StructField("timestamp", StringType())
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
    .withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))

exchange_schema = StructType([
    StructField("amount", DoubleType()),
    StructField("base", StringType()),
    StructField("date", StringType()),
    StructField("rates", DoubleType()),
    StructField("timestamp", StringType())
])

crypto_df = crypto_df.withWatermark("timestamp", "1 minutes").alias("crypto")
exchange_df = exchange_df.withWatermark("timestamp", "1 minutes").alias("exchange")

joined_df = crypto_df.alias("crypto").join(
    exchange_df.alias("exchange"),
    on=[col("crypto.timestamp") == col("exchange.timestamp")],
    how="leftOuter"
)

windowed_df = joined_df.groupBy(
    window(col("crypto.timestamp"), "1 minutes"),
    col("crypto.id")
).agg({
    "priceUsd": "avg",
    "volumeUsd24Hr": "sum",
    "rates": "avg"
})

query = windowed_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()