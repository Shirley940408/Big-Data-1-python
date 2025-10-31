from pyspark import SparkConf, SparkContext
import sys

from pyspark.sql import functions, SparkSession

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re

# spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.13:3.5.1 \
#   --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
#   --conf spark.cassandra.connection.host=node1.local,node2.local \
#   correlate_logs_cassandra.py sya236 nasalogs


# host name, the datetime, the requested path, and the number of bytes
# sample_logs = [
# '''
# in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839
# uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] "GET / HTTP/1.0" 304 0
# uplherc.upl.com - - [01/Aug/1995:00:00:08 -0400] "GET /images/ksclogo-medium.gif HTTP/1.0" 304 0
# uplherc.upl.com - - [01/Aug/1995:00:00:08 -0400] "GET /images/MOSAIC-logosmall.gif HTTP/1.0" 304 0
# uplherc.upl.com - - [01/Aug/1995:00:00:08 -0400] "GET /images/USA-logosmall.gif HTTP/1.0" 304 0
# ix-esc-ca2-07.ix.netcom.com - - [01/Aug/1995:00:00:09 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1713
# uplherc.upl.com - - [01/Aug/1995:00:00:10 -0400] "GET /images/WORLD-logosmall.gif HTTP/1.0" 304 0
# slppp6.intermind.net - - [01/Aug/1995:00:00:10 -0400] "GET /history/skylab/skylab.html HTTP/1.0" 200 1687
# piweba4y.prodigy.com - - [01/Aug/1995:00:00:10 -0400] "GET /images/launchmedium.gif HTTP/1.0" 200 11853
# slppp6.intermind.net - - [01/Aug/1995:00:00:11 -0400] "GET /history/skylab/skylab-small.gif HTTP/1.0" 200 9202
# '''
# ]

def main():
    # main logic starts here
    namespace = sys.argv[1]
    tabel_name = sys.argv[2]
    df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=tabel_name, keyspace=namespace).load() \
        .select('host', 'bytes')

    host_byte_df = df.groupby('host').agg(functions.sum('bytes').alias('y_i'), functions.count('host').alias('x_i')) \
        .select(functions.col("host").alias("1"), 'x_i', 'y_i') \
        .withColumn('x_i^2', pow(functions.col('x_i'), 2)) \
        .withColumn('y_i^2', pow(functions.col('y_i'), 2)) \
        .withColumn('x_i*y_i', functions.col('x_i') * functions.col('y_i')) \
        .cache()

    sum_x_i = host_byte_df.agg(functions.sum('x_i')).collect()[0][0]
    sum_y_i = host_byte_df.agg(functions.sum('y_i')).collect()[0][0]
    sum_x_i_pow_2 = host_byte_df.agg(functions.sum('x_i^2')).collect()[0][0]
    sum_y_i_pow_2 = host_byte_df.agg(functions.sum('y_i^2')).collect()[0][0]
    sum_x_i_multiply_y_i = host_byte_df.agg(functions.sum('x_i*y_i')).collect()[0][0]
    n = host_byte_df.count()
    r = (n*sum_x_i_multiply_y_i - sum_x_i*sum_y_i)/(pow(n*sum_x_i_pow_2-pow(sum_x_i,2),1/2)*pow(n*sum_y_i_pow_2-pow(sum_y_i,2),1/2))
    print (f"r = {round(r,6)}")
    print (f"r^2 = {round(pow(r,2),6)}")

if __name__ == '__main__':
    conf = SparkConf().setAppName('Server Log Correlation with Cassandra Data')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('load_logs_with_spark').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    main()
