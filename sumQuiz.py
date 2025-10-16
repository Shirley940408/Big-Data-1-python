from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]

conf = SparkConf().setAppName('sumQuiz')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

pattern = re.compile(rf'[{re.escape(string.punctuation)}\s]+')
def words_once(line):
    for w in pattern.split(line):
        yield (float(w.lower()), 1)

def add(x, y):
    return x + y

text = sc.textFile(inputs)
words = text.flatMap(words_once)
wordcount = words.reduceByKey(add)
print(wordcount.sum())
