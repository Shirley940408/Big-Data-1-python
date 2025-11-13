"""
Sample Kafka consumer, to verify that messages are coming in on the topic we expect.
"""
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 job.py
import sys
from kafka import KafkaConsumer
from pyspark.sql import SparkSession

topic = sys.argv[1]
consumer = KafkaConsumer(topic, bootstrap_servers=['node1.local:9092','node2.local:9092'],auto_offset_reset='latest')
for msg in consumer:
    print(msg.value.decode('utf-8'))

# stream = streaming_df.….start()
# stream.awaitTermination(600)
spark = SparkSession.builder.appName('read_stream').getOrCreate()
messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
        .option('subscribe', topic).load()
values = messages.select(messages['value'].cast('string'))

query = (values.writeStream
         .format("console")
         .outputMode("append")
         .start())

query.awaitTermination()