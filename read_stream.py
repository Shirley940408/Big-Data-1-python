"""
Sample Kafka consumer, to verify that messages are coming in on the topic we expect.
"""
# spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0 read_stream.py xy-1
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, count, sum

topic = sys.argv[1]

spark = SparkSession.builder.appName('read_stream').getOrCreate()
messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
        .option('subscribe', topic).load()

values = messages.select(messages['value'].cast('string'))

split_col = split(col("value"), "\s+")
value_with_points = values.withColumn("x", split_col.getItem(0))\
                          .withColumn("y", split_col.getItem(1))\
                          .select("x", "y")

status = (value_with_points.withColumn('n', count(value_with_points.x))
                            .withColumn('sum_x', sum(col('x')))
                            .withColumn('sum_y', sum(col('y')))
                            .withColumn('sum_x*y', sum(col('sum_x')*col('sum_y')))
                            .withColumn('sum_x^2', sum(pow(col('x'),2)))
                            .select("n", "sum_x", "sum_y","sum_x*y","'sum_x^2'"))

beta_alpha_df = status.withColumn('beta', (col('sum_x*y') - col('sum_x')*col('sum_y')/col('n'))/(col('sum_x^2') - pow(col('sum_x'),2)/col('n')))\
                      .withColumn('alpha', col('sum_y')/col('n') - col('beta')*col('sum_x')/col('n')).select('beta', 'alpha')

stream = (beta_alpha_df.writeStream
         .format("console")
         .outputMode("append")
         .start())

stream.awaitTermination()