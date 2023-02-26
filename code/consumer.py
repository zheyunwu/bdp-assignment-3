from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
   'tenant1.taxi',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda m: loads(m.decode('utf-8')),
    bootstrap_servers=['35.228.109.23:9092']
)

for m in consumer:
    print(m.value)