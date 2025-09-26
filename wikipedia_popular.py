from pyspark import SparkConf, SparkContext
import sys
import re, csv
inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wikipedia_popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

pattern = re.compile(r'\s+')
def frequency_count(line):
    parts = pattern.split(line, maxsplit=4)
    if len(parts) < 5:
        return
    ts, lang, title, cnts, _bytes = parts
    if lang != 'en' or title == 'Main_Page'or title.startswith('Special:'):
        return
    try:
        views = int(cnts)
    except ValueError:
        return
    # key=时间戳, value=该行的浏览量
    yield ts, views

def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])

def to_csv(kv):
    ts, cnt = kv
    return f"{ts},{cnt}"

text = sc.textFile(inputs)
words = text.flatMap(frequency_count)
max_count = words.reduceByKey(max).sortByKey()
# max_count.map(tab_separated).saveAsTextFile(output)
max_count.map(to_csv).saveAsTextFile(output)