import json
import sys
from pyspark import SparkConf, SparkContext

# 解析每行 JSON；只保留需要的字段，过滤坏行/缺字段
def safe_pick(line):
    try:
        obj = json.loads(line)
        sub = obj.get('subreddit')
        if not sub:
            return []
        # 只留需要的三个字段
        score = obj.get('score', 0)
        author = obj.get('author', 'unknown')
        return [ {'subreddit': sub, 'score': int(score), 'author': author} ]
    except Exception:
        return []  # 丢弃坏行

def main(inputs, output):
    sc = SparkContext(conf=SparkConf().setAppName("reddit_etl"))
    sc.setLogLevel("WARN")

    # 读入原始数据
    text = sc.textFile(inputs)
    # 解析后缓存：后面要用两次（正/负分流）
    rows = text.flatMap(safe_pick).cache()

    # 只保留 subreddit 名里包含字母 'e' 的记录
    has_e = rows.filter(lambda r: 'e' in r['subreddit'])

    # 按分数正负分流
    positive = has_e.filter(lambda r: r['score'] > 0)
    negative = has_e.filter(lambda r: r['score'] <= 0)

    # 序列化为一行一个 JSON 对象
    pos_json = positive.map(lambda r: json.dumps(r))
    neg_json = negative.map(lambda r: json.dumps(r))

    # 写出到子目录（不得使用 coalesce(1)；也不 collect 到 Driver）
    pos_json.saveAsTextFile(f"{output}/positive")
    neg_json.saveAsTextFile(f"{output}/negative")

    sc.stop()

if __name__ == "__main__":
    # 用法：spark-submit reddit_etl.py reddit-2 output
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)