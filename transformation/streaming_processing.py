"""
PySpark Structured Streaming Job
Reads from Kafka, performs aggregations, and writes to console/storage.
"""
import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("DataFlow360_Streaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

def get_transaction_schema():
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("amount", FloatType(), True),
        StructField("currency", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("merchant", StringType(), True),
        StructField("status", StringType(), True)
    ])

def process_stream(spark, kafka_bootstrap_servers, topic):
    logger.info(f"Starting stream processing from {topic}...")

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON data
    schema = get_transaction_schema()
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")

    # Filter completed transactions
    filtered_df = parsed_df.filter(col("status") == "completed")

    # Aggregations: Count and Avg Amount per Merchant per 1-minute window
    windowed_counts = filtered_df \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("merchant")
        ) \
        .agg(
            count("transaction_id").alias("transaction_count"),
            avg("amount").alias("avg_amount")
        )

    # Write to Console (for debugging/demo)
    query = windowed_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: streaming_processing.py <kafka_bootstrap_servers> [topic]")
        sys.exit(1)

    kafka_servers = sys.argv[1]
    topic_name = sys.argv[2] if len(sys.argv) > 2 else "transactions"

    spark = create_spark_session()
    try:
        process_stream(spark, kafka_servers, topic_name)
    except KeyboardInterrupt:
        logger.info("Stopping stream...")
    finally:
        spark.stop()
