import json
from pyspark import SparkConf, SparkContext
import sys

assert sys.version_info >= (3, 5)

def safe_json(line):
    try:
        obj = json.loads(line)
        sub = obj.get('subreddit')
        if sub is None:
            return []
        score = float(obj.get('score', 0))
        author = obj.get('author', 'unknown')
        return [ {'subreddit': sub, 'score': score, 'author': author} ]
    except Exception:
        return []

def add_pairs(a, b):
    # (c1, s1) + (c2, s2)
    return (a[0] + b[0], a[1] + b[1])

def compute_avg_map(commentdata):
    # (subreddit, (count, sum))
    count_sum = (commentdata
                 .map(lambda c: (c['subreddit'], (1, c['score'])))
                 .reduceByKey(add_pairs))
    # (subreddit, avg)
    avg = count_sum.mapValues(lambda cs: cs[1] / cs[0])
    # 只保留 avg>0；collect 成 Python dict（很小，适合广播）
    return dict(avg.filter(lambda kv: kv[1] > 0).collect())

# —— 关键：写一个使用“广播对象”的函数（在 executor 上运行）——
def relative_from_bcast(bcast_avg, c):
    """
    bcast_avg: Broadcast 对象（必须 .value 才能取出字典）
    c: {'subreddit', 'score', 'author'}
    return: (relative_score, author) 或 None
    """
    avgs = bcast_avg.value
    sub = c['subreddit']
    avg = avgs.get(sub)
    if avg is None or avg <= 0:
        return None
    return (c['score'] / avg, c['author'])

def main(inputs, output, mode="full-sorted", topN=1000, parts=256):
    text = sc.textFile(inputs)
    commentdata = text.flatMap(safe_json).cache()  # 解析后缓存：后面要用两次

    # 1) 小 RDD → Python dict
    avg_map = compute_avg_map(commentdata)

    # 2) 广播到 executors
    bcast_avg = sc.broadcast(avg_map)

    # 3) 在“大 RDD（评论）”上直接算相对分数：不做 join
    rel_author = (commentdata
                  .map(lambda c: relative_from_bcast(bcast_avg, c))
                  .filter(lambda x: x is not None))

    if mode == "top":
        # 更快更省内存：只要 Top N
        top = rel_author.takeOrdered(topN, key=lambda x: -x[0])
        sc.parallelize(top, 1).map(lambda kv: json.dumps([kv[0], kv[1]])).saveAsTextFile(output)
    elif mode == "full-sorted":
        # 全量按相对分数降序输出（分区内排序更稳）
        # 用负号把降序变成升序排序键
        keyed = rel_author.map(lambda kv: (-kv[0], kv[1]))
        sorted_parted = keyed.repartitionAndSortWithinPartitions(parts)
        sorted_parted.map(lambda kv: json.dumps([-kv[0], kv[1]])).saveAsTextFile(output)
    else:
        # 不排序，直接输出
        rel_author.map(lambda kv: json.dumps([kv[0], kv[1]])).saveAsTextFile(output)

if __name__ == '__main__':
    conf = (SparkConf()
            .setAppName('relative score (broadcast join)')
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.shuffle.compress", "true")
            .set("spark.shuffle.spill.compress", "true"))
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'

    inputs = sys.argv[1]
    output = sys.argv[2]
    mode = sys.argv[3] if len(sys.argv) > 3 else "full-sorted"
    arg4 = int(sys.argv[4]) if len(sys.argv) > 4 else None

    if mode == "top":
        topN = arg4 if arg4 is not None else 1000
        main(inputs, output, mode="top", topN=topN)
    elif mode == "full-sorted":
        parts = arg4 if arg4 is not None else 256
        main(inputs, output, mode="full-sorted", parts=parts)
    else:
        main(inputs, output, mode="raw")

    sc.stop()  # 良好习惯