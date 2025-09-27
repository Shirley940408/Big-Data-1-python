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


# def words_partition(lines):
#     # 分区内先聚合，显著减少后续按 key 的 shuffle 体积
#     cnt = Counter()
#     for line in lines:
#         for w in pattern.split(line):
#             if w:
#                 cnt[w.lower()] += 1
#     # 吐出本分区的 (word, partial_count)
#     for k, v in cnt.items():
#         yield (k, v)

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
    # pairs = text.mapPartitions(words_partition)
    words = text.flatMap(words_once)
    wordcount = words.reduceByKey(add)
    # wordcount = pairs.reduceByKey(add, numPartitions=32)
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