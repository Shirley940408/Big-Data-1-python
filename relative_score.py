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

def safe_json(line):
    try:
        obj = json.loads(line)
        # 只保留我们后面要用到的字段，避免超大记录占内存
        return [ {
            'subreddit': obj.get('subreddit'),
            'score': float(obj.get('score', 0)),
            'author': obj.get('author', 'unknown')
        } ] if obj.get('subreddit') is not None else []
    except Exception:
        return []  # 丢掉坏行

def compute_avg(commentdata):
    # (subreddit, (count, sum))
    count_sum = commentdata.map(lambda c: (c['subreddit'], (1, c['score']))).reduceByKey(add_pairs)
    # (subreddit, avg)
    avg = count_sum.mapValues(lambda cs: cs[1] / cs[0])
    # 忽略平均分 <= 0 的 subreddit
    avg_pos = avg.filter(lambda kv: kv[1] > 0)
    return avg_pos

def main(inputs, output):
    text = sc.textFile(inputs)
    commentdata = text.flatMap(safe_json).cache()   # 关键的 cache

    # (subreddit, average_score)
    avg_by_sub = compute_avg(commentdata)

    # (subreddit, comment_dict)
    commentbysub = commentdata.map(lambda c: (c['subreddit'], c))

    # (subreddit, (avg, comment))
    joined = avg_by_sub.join(commentbysub)

    # (relative_score, author)
    rel_author = joined.map(
        lambda kv: (
            float(kv[1][1].get('score', 0.0)) / float(kv[1][0]),
            kv[1][1].get('author', 'unknown')
        )
    )

    # 全局排序（按 key，即 relative_score），降序
    rel_sorted = rel_author.sortByKey(ascending=False)

    # 序列化为 JSON 行，例如: [123.4, "some_author"]
    output_rdd = rel_sorted.map(lambda kv: json.dumps([kv[0], kv[1]]))
    output_rdd.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('relative score')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)