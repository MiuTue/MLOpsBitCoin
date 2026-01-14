import io
import os
import avro.io
import avro.schema
import pyspark.sql.types as T
#from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession, functions as func
import requests
import json

# Define the schema structure explicitly
schema = T.StructType([
    T.StructField("id", T.StringType(), True),
    T.StructField("asset_name", T.StringType(), True),
    T.StructField("open", T.StringType(), True),
    T.StructField("high", T.StringType(), True),
    T.StructField("low", T.StringType(), True),
    T.StructField("close", T.StringType(), True),
    T.StructField("volume", T.StringType(), True),
    T.StructField("quote_volume", T.StringType(), True),
    T.StructField("trades", T.StringType(), True),
    T.StructField("is_closed", T.StringType(), True),
    T.StructField("timestamp", T.StringType(), True),
    T.StructField("close_time", T.StringType(), True),
    T.StructField("collected_at", T.StringType(), True)
])

asset_schema_location = os.environ.get('ASSET_SCHEMA_LOCATION', './schemas/assets.avsc') 

def load_schema(schema_path):
    return avro.schema.parse(open(schema_path).read())

def decode_asset_avro_message(data):
    try:
        asset_avro_schema = load_schema(asset_schema_location)
        bytes_reader = io.BytesIO(data)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(asset_avro_schema)
        decoded = reader.read(decoder)
        # Print is_closed value for debugging
        if 'is_closed' in decoded:
            print(f"Decoded is_closed value: '{decoded['is_closed']}' (type: {type(decoded['is_closed'])})")
        return decoded
    except Exception as e:
        print(f"Error decoding message: {str(e)}")
        return None

def get_prediction(historical_prices):
    try:
        if len(historical_prices) < 10:
            return None
        
        response = requests.post(
            "http://binance-serving:8000/predict",
            json={"data": historical_prices},
            timeout=2
        )
        if response.status_code == 200:
            return response.json().get("prediction")
    except Exception as e:
        print(f"Inference error: {str(e)}")
    return None

class BinanceConsumer:
    def __init__(self) -> None:
        self.spark = SparkSession.builder \
            .appName('binance-consumer') \
            .master(os.environ.get('SPARK_MASTER', 'local[*]')) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.cassandra.connection.host", os.environ.get('ASSET_CASSANDRA_HOST', 'localhost')) \
            .config("spark.cassandra.connection.port", os.environ.get('ASSET_CASSANDRA_PORT', 9042)) \
            .config("spark.cassandra.auth.username", os.environ.get('ASSET_CASSANDRA_USERNAME')) \
            .config("spark.cassandra.auth.password", os.environ.get('ASSET_CASSANDRA_PASSWORD')) \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel('INFO')  # Changed to INFO for better debugging
        print("SparkSession created successfully")

    def read_from_kafka(self, topic: str):
        self.df_raw_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", os.environ.get('REDPANDA_BROKERS', 'localhost:9092')) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
        
        print(f"Created Kafka stream for topic: {topic}")

    def parse_avro_message_4rm_kafka(self):
        decode_asset_avro_message_udf = func.udf(decode_asset_avro_message, returnType=schema)
        get_prediction_udf = func.udf(get_prediction, T.FloatType())

        df_value_stream = self.df_raw_stream.selectExpr('value')

        self.df_pure_stream = df_value_stream \
            .select(decode_asset_avro_message_udf(func.col('value')).alias('decoded_data')) \
            .select(
                func.col('decoded_data.id'),
                func.col('decoded_data.asset_name'),
                func.col('decoded_data.open').cast('float').alias('open'),
                func.col('decoded_data.high').cast('float').alias('high'),
                func.col('decoded_data.low').cast('float').alias('low'),
                func.col('decoded_data.close').cast('float').alias('close'),
                func.col('decoded_data.volume').cast('float').alias('volume'),
                func.col('decoded_data.quote_volume').cast('float').alias('quote_volume'),
                func.col('decoded_data.trades').cast('int').alias('trades'),
                func.when(func.lower(func.col('decoded_data.is_closed')) == 'true', True)
                    .when(func.lower(func.col('decoded_data.is_closed')) == 'false', False)
                    .otherwise(None).alias('is_closed'),
                func.to_timestamp(func.col('decoded_data.timestamp')).alias('timestamp'),
                func.to_timestamp(func.col('decoded_data.close_time')).alias('close_time'),
                func.to_timestamp(func.col('decoded_data.collected_at')).alias('collected_at'),
                func.current_timestamp().alias('consumed_at')
            )
        
        # Add windowing logic to collect last 10 prices for LSTM
        # Note: In a real production scenario, we'd use a windowing function or a StateStore.
        # For simplicity in this MLOps demo, we'll simulate it or just send current price if needed.
        # But since LSTM needs 10 steps, we apply a window here.
        self.df_pure_stream = self.df_pure_stream.withColumn(
            "prediction", 
            get_prediction_udf(func.array([func.col("close")] * 10)) # Placeholder: sending current price 10 times to demonstrate call
        )
        
        print("Created streaming DataFrame with schema and predictions")

    def sink_console(self, output_mode: str = 'append', processing_time: str = '10 seconds'):
        print("Starting console sink...")
        query = self.df_pure_stream.writeStream \
            .outputMode(output_mode) \
            .trigger(processingTime=processing_time) \
            .format("console") \
            .option("truncate", False) \
            .start()
        print("Console sink started")
        return query

    def sink_into_cassandra(self, output_mode: str = 'append'):
        print("Starting Cassandra sink...")
        query = self.df_pure_stream.writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .outputMode(output_mode) \
            .option("checkpointLocation", "/tmp/checkpoint/cassandra") \
            .options(
                table=os.environ.get('ASSET_CASSANDRA_TABLE'), 
                keyspace=os.environ.get('ASSET_CASSANDRA_KEYSPACE')
            ) \
            .start()
        print("Cassandra sink started")
        return query
    
    def waitingForTermination(self):
        print("Waiting for stream termination...")
        self.spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    try:
        print("Starting BinanceConsumer...")
        consumer = BinanceConsumer()

        topic = os.environ.get('ASSET_PRICES_TOPIC', 'data.asset_prices')
        print(f"Reading from topic: {topic}")
        consumer.read_from_kafka(topic=topic)
        
        print("Parsing Avro messages...")
        consumer.parse_avro_message_4rm_kafka()

        # Start both sinks
        console_query = consumer.sink_console(output_mode='append')
        cassandra_query = consumer.sink_into_cassandra(output_mode='append')

        print("All sinks started, waiting for termination...")
        consumer.waitingForTermination()
    except Exception as e:
        print(f"Error in main: {str(e)}")
        raise

    