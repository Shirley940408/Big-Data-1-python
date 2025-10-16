# relative_score.py
import json, sys
from pyspark import SparkConf, SparkContext

def safe_parse(line):
    try:
        obj = json.loads(line)
        # 只要是合法 JSON 就保留；score/author/subreddit 缺失后面兜底
        return [obj]
    except Exception:
        return []

def add_count_sum(a, b):
    # (c1,s1) + (c2,s2)
    return (a[0] + b[0], a[1] + b[1])

def main(inputs, output):
    conf = SparkConf().setAppName("relative_score").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # 1) 读入 & 解析 & cache（题面要求）
    text = sc.textFile(inputs)
    commentdata = text.flatMap(safe_parse).cache()   # RDD[dict]

    # 2) 题面格式：以子版块为 key、评论为 value
    #    commentbysub: RDD[(subreddit, comment_dict)]
    commentbysub = commentdata.map(lambda c: (c.get('subreddit'), c)) \
                              .filter(lambda kv: kv[0] is not None)

    # 3) 由 commentbysub 求每个子版块的平均分，并筛掉 avg<=0
    #    (sub, (1, score)) -> reduceByKey -> (sub, (count,sum)) -> (sub, avg)
    count_sum = commentbysub.mapValues(lambda c: (1, float(c.get('score', 0.0)))) \
                            .reduceByKey(add_count_sum)
    avgbysub  = count_sum.mapValues(lambda cs: cs[1] / cs[0]) \
                         .filter(lambda kv: kv[1] > 0)   # 只保留 avg>0 的子版块

    # 4) join 平均分与评论本身（题面要求用 .join）
    #    joined: (sub, (comment_dict, avg))
    joined = commentbysub.join(avgbysub)

    # 5) 形成 (relative_score, author)
    #    注意字段兜底，防止缺失
    rel_author = joined.map(
        lambda kv: (
            float(kv[1][0].get('score', 0.0)) / kv[1][1],
            kv[1][0].get('author', 'unknown')
        )
    )

    # 6) 按相对分数降序排序并输出（不 coalesce，题面要求）
    rel_sorted = rel_author.sortBy(lambda t: t[0], ascending=False)
    # 输出成简单的 TSV（也可以改成 json.dumps(t)）
    rel_sorted.map(lambda t: f"{t[0]}\t{t[1]}").saveAsTextFile(output)

    sc.stop()

if __name__ == "__main__":
    inputs, output = sys.argv[1], sys.argv[2]
    main(inputs, output)