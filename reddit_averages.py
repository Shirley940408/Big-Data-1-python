import json
from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
# 输入：
# {"subreddit": "helloworld", "score": 1}
# {"subreddit": "helloworld", "score": 2}
# ...
# 读每一行 → json.loads(line)
# 取出subreddit和score
# 映射成(subreddit, (1, score))
# reduceByKey(add_pairs)
# 得到(count, score_sum)
# 映射成["subreddit", avg]
# 用json.dumps输出

# add more functions as necessary
def add_pairs(a, b):
    # 题目提示的函数
    # (c1, s1) + (c2, s2) = (c1+c2, s1+s2)
    return a[0] + b[0], a[1] + b[1]

def subreddit_count_score(line):
    try:
        obj = json.loads(line)
        subreddit = obj['subreddit']
        score = obj.get('score', 0)
        yield subreddit, (1, float(score))
    except Exception:
        return #丢弃坏行

def get_average(kv):
    subreddit, (count, sum_score) = kv
    return json.dumps([subreddit, sum_score / count])

def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    words = text.flatMap(subreddit_count_score)
    count_score = words.reduceByKey(add_pairs)
    count_score.map(get_average).saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit averages')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)