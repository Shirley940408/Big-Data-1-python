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

status = (value_with_points.withColumn('x_multiply_y', col('x')*col('y'))
                           .withColumn('pow_x', pow(col('x'),2)))

aggregate_status = (status.agg(sum(col('x_multiply_y')).alias('sum_x_multiply_y'),
                                 sum(col('x')).alias('sum_x'),
                                 sum(col('y')).alias('sum_y'),
                                 sum(col('pow_x')).alias('sum_pow_x'),
                                 sum(pow(col('x'),2)).alias('pow_sum_x'),
                                count(col('x')).alias('n')))

beta_alpha_df = aggregate_status.withColumn('beta', (col('sum_x_multiply_y') - col('sum_x')*col('sum_y')/col('n'))/(col('sum_pow_x') - col('pow_sum_x')/col('n')))\
                      .withColumn('alpha', col('sum_y')/col('n') - col('beta')*col('sum_x')/col('n')).select('beta', 'alpha')

stream = (beta_alpha_df.writeStream
         .format("console")
         .outputMode("complete")
         .start())

stream.awaitTermination()