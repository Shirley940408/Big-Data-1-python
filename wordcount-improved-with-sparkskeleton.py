from collections import Counter

from pyspark import SparkConf, SparkContext
import sys
import re, string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary
pattern = re.compile(rf'[{re.escape(string.punctuation)}\s]+')

def words_once(line):
    count = Counter()
    for w in pattern.split(line):
        if w: count[w.lower()] += 1
    for k, v in count.items():
        yield k, v

def add(x, y):
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    words = text.flatMap(words_once).repartition(128)
    wordcount = words.reduceByKey(add, numPartitions=128)

    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)