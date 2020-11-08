import json
import time

import requests
from kafka import KafkaProducer

my_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def message_sender(m):
    """Send (key, value) to a Kafka producer"""
    print(m)
    my_producer.send("topic2",m)
    return m
if __name__ == "__main__":
    for i in range(100):
        data = requests.get("https://randomuser.me/api/0.8").json()
        message_sender(data)
        time.sleep(10)