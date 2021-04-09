import os
import json
import pandas as pd
from kafka import KafkaProducer

data_sources = [
    {
        'tenant_id': 'tenant1',
        'file_path': os.getcwd() + '/../data/yellow_taxi_trip_data.csv',
        'table_metadata': {
            "table_name": "yellow_taxi_trip",
            "primary_key": ["vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime"],
            "schema": [
                {"field": "vendorid", "type": "BIGINT"},
                {"field": "tpep_pickup_datetime", "type": "DATETIME"},
                {"field": "tpep_dropoff_datetime", "type": "DATETIME"},
                {"field": "passenger_count", "type": "INT"},
                {"field": "trip_distance", "type": "FLOAT"},
                {"field": "ratecodeid", "type": "BIGINT"},
                {"field": "store_and_fwd_flag", "type": "TEXT"},
                {"field": "pulocationid", "type": "BIGINT"},
                {"field": "dolocationid", "type": "BIGINT"},
                {"field": "payment_type", "type": "INT"},
                {"field": "fare_amount", "type": "FLOAT"},
                {"field": "extra", "type": "FLOAT"},
                {"field": "mta_tax", "type": "FLOAT"},
                {"field": "tip_amount", "type": "FLOAT"},
                {"field": "tolls_amount", "type": "FLOAT"},
                {"field": "improvement_surcharge", "type": "FLOAT"},
                {"field": "total_amount", "type": "FLOAT"}
            ]
        }
    }
]

# create Kafka producer object
producer = KafkaProducer(
    value_serializer=lambda m: json.dumps(m).encode('utf-8'), 
    bootstrap_servers=['35.228.109.23:9092']
)

# read csv and publish to Kafka
reader = pd.read_csv(data_sources[0]['file_path'], chunksize=20)
for chunk_df in reader:
    for row in chunk_df.itertuples():
        # compose json payload
        data = {
            item['field']: getattr(row, item['field']) if not pd.isna(getattr(row, item['field'])) else None for item in data_sources[0]['table_metadata']['schema']
        }
        # send to kafka
        producer.send("tenant1.taxi", value=data)
        producer.flush()
    break
