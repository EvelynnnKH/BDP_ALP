from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def main():
    spark = SparkSession.builder \
        .appName("Evelin-Spark-Raw-Streaming") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=== Memulai Spark Structured Streaming (Evelin) ===")

    kafka_raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "clickstream-events") \
        .load()
    
    clickstream_schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("session_id", IntegerType(), True),
        StructField("country", IntegerType(), True),
        StructField("main_category", IntegerType(), True),       
        StructField("clothing_model", StringType(), True),
        StructField("colour", IntegerType(), True),
        StructField("location", IntegerType(), True),
        StructField("price", IntegerType(), True),
        StructField("price_level", IntegerType(), True),
        StructField("page", IntegerType(), True),
        StructField("order_in_session", IntegerType(), True)
    ])

    parsed_streaming_df = kafka_raw_df \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), clickstream_schema).alias("data")) \
        .select("data.*")

    query = parsed_streaming_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("checkpointLocation", "/opt/alp/checkpoints/raw_streaming") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()