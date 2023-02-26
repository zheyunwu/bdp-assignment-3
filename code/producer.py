from kafka import KafkaProducer
from json import dumps

producer = KafkaProducer(
    value_serializer=lambda m: dumps(m).encode('utf-8'), 
    bootstrap_servers=['35.228.109.23:9092']
)

producer.send("tenant1.taxi", value={"hello": "producser"})
producer.flush()
# import pandas as pd
# import pika
# import json
# import os

# # connect to rabbitmq
# CRENDENTIALS = pika.PlainCredentials('mysimbdp', 'mysimbdp')
# connection = pika.BlockingConnection(pika.ConnectionParameters(host="35.228.109.23", virtual_host='tenant_1', credentials=CRENDENTIALS))
# channel = connection.channel()