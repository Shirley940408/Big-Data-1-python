"""
Sample Kafka consumer, to verify that messages are coming in on the topic we expect.
"""
import sys
from kafka import KafkaConsumer

topic = sys.argv[1]
consumer = KafkaConsumer(topic, bootstrap_servers=['node1.local：9092'],auto_offset_reset='latest')
for msg in consumer:
    print(msg.value.decode('utf-8'))