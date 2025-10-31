import uuid

from pyspark import SparkConf, SparkContext
import sys

from pyspark.sql import SparkSession
from datetime import datetime


assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re
# spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.13:3.5.1 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions load_logs_spark.py /courses/732/nasa-logs-2 <userid> nasalogs


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

def web_server_byte_log(line):
    pattern = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    match = pattern.match(line)
    if match:
        hostname = match.group(1)
        datetimeString = match.group(2)
        path = match.group(3)
        numbers_of_bytes_String = match.group(4)
        naive = datetime.strptime(datetimeString, "%d/%b/%Y:%H:%M:%S")
        yield hostname, naive, path, int(numbers_of_bytes_String), uuid.uuid1()  # hostname, datetime, path, numbers_of_bytes, uuid

def add(x, y):
    return x[0] + y[0], x[1] + y[1]

def main(inputs):
    # main logic starts here
    text = sc.textFile(inputs)
    rdd = text.flatMap(web_server_byte_log)
    # Repartition by host to improve write parallelism (adjust by cluster size 8).
    # rdd = rdd.keyBy(lambda t: t[0]).repartition(8).values()
    df = spark.createDataFrame(
        rdd,
        schema=["host", "datetime", "path", "bytes", "req_id"]
    )
    # df = rdd.toDF(['host', 'datetime', 'path', 'bytes', 'req_id']).cache()
    namespace = sys.argv[2]
    table_name = sys.argv[3]
    df.write.format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace=namespace).save()


if __name__ == '__main__':
    conf = SparkConf().setAppName('Server Log Correlation')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('load_logs_with_spark').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    inputs = sys.argv[1]
    main(inputs)