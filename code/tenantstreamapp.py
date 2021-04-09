 import sys, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests


'''
Step 1: Fill in the config
'''
# SPARK_MASTER = 'spark://35.228.109.23:7077'
TENANT_ID = 'tenant1'
DAAS_API = '35.228.109.23:5000'
MESSAGE_BROKER = '35.228.109.23:9092'
TABLE = {
    'table_name': 'yellow_taxi_trip',
    'fields': ["vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "ratecodeid", "store_and_fwd_flag", "pulocationid", "dolocationid", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "driving_time", "driving_time_per_km", "fare_per_km"]
}
TOPIC = 'tenant1.taxi'


'''
Step 2: Define schema of message for spark
'''
schema = StructType() \
    .add("vendorid", LongType()) \
    .add("tpep_pickup_datetime", TimestampType()) \
    .add("tpep_dropoff_datetime", TimestampType()) \
    .add("passenger_count", IntegerType()) \
    .add("trip_distance", DoubleType()) \
    .add("ratecodeid", LongType()) \
    .add("store_and_fwd_flag", StringType()) \
    .add("pulocationid", LongType()) \
    .add("dolocationid", LongType()) \
    .add("payment_type", IntegerType()) \
    .add("fare_amount", DoubleType()) \
    .add("extra", DoubleType()) \
    .add("mta_tax", DoubleType()) \
    .add("tip_amount", DoubleType()) \
    .add("tolls_amount", DoubleType()) \
    .add("improvement_surcharge", DoubleType()) \
    .add("total_amount", DoubleType())


if __name__ == '__main__':
    '''
    Step 3: Get a SparkSession object
    '''
    spark = SparkSession \
        .builder \
        # .master(SPARK_MASTER) \
        .appName("tenant1_taxi") \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1') \
        .getOrCreate()

    '''
    Step 4: Use SparkSession object to read stream from Kafka, and load it to dataframe
    '''
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", MESSAGE_BROKER) \
        .option("subscribe", TOPIC) \
        .load()

    '''
    Step 5: Define operations on the original dataframe
    '''
    #########################################################################
    # Operation 1: append additinal fields and ingest into mysimbdp-coredms #
    #########################################################################

    # Extract the 'value' column, deserialize it and flatten to dataframe columns. Calculate driving_time_per_km and fare_per_km of each event
    trip_data = trip_data \
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data")).select("data.*")
        .withColumn("driving_time", unix_timestamp("tpep_dropoff_datetime")-unix_timestamp("tpep_pickup_datetime")) \
        .withColumn("driving_time_per_km", col("driving_time")/col("trip_distance")) \
        .withColumn("fare_per_km", col("fare_amount")/col("trip_distance"))
    # Define what to do with each event
    def process_row(row):
        # call daas api to ingest each row
        payload = {'table_name': TABLE['table_name'], 'data': {field: row[field] for field in TABLE['fields']}}
        res = requests.post("http://{}/{}/stream_ingest".format(DAAS_API, TENANT_ID), json=payload)
    # Output: for each row/event in the transformed dataframe, we set the function process_row as callback.
    query = df.writeStream
        .foreach(process_row)
        .start()
    # Output: FOR DEBUG
    # query = trip_data.writeStream.format("console").start()


    #######################################################################
    # Operation 2: do real-time analytics and send results back to tenant #
    #######################################################################

    # Set watermark and sliding window and compute average_driving_time_per_km and average_fare_per_km.
    cal = trip_data \
        .withWatermark("tpep_dropoff_datetime", "10 minutes") \
        .groupBy(window("tpep_dropoff_datetime", "20 minute", "5 minute")) \
        .agg(avg(col('driving_time_per_km')).alias("avg_driving_time_per_km"), avg(col('fare_per_km')).alias("avg_fare_per_km")) \
    # Output: serialize the aggregated results into json, then publish back to kafka for tenant
    cal.select(to_json(struct("avg_driving_time_per_km", "avg_fare_per_km")).alias("payload"))
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", MESSAGE_BROKER) \
        .option("topic", "real-time-analystics") \
        .start()
    # Output: FOR DEBUG
    # cal.writeStream.format("console").start()

    '''
    Step 6: Keep the streaming alive
    '''
    spark.streams.awaitAnyTermination()
