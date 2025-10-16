# relative_score_bcast.py
import json, sys
from pyspark import SparkConf, SparkContext

def safe_parse(line):
    try:
        return [json.loads(line)]
    except Exception:
        return []

def add_count_sum(a, b):
    return (a[0] + b[0], a[1] + b[1])

def rel_score_from_bcast(b_avgs, comment):
    # Access broadcast INSIDE the executor function
    avgs = b_avgs.value #{"subreddit": "datascience", "score": 4, "author": "bob"}
    sub = comment.get('subreddit')
    if not sub:
        return None
    avg = avgs.get(sub)
    if not avg or avg <= 0:
        return None
    score = float(comment.get('score', 0.0))
    author = comment.get('author', 'unknown')
    return score / avg, author  #[(2.0, "alice"), (0.48, "bob"), (1.23, "carol"， None...]

def main(inputs, output):
    conf = SparkConf().setAppName("relative_score_bcast").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # 1) Read & parse & cache (expensive step)
    text = sc.textFile(inputs)
    commentdata = text.flatMap(safe_parse).cache()

    # 2) (subreddit, comment)
    commentbysub = commentdata.map(lambda c: (c.get('subreddit'), c)) \
                              .filter(lambda kv: kv[0] is not None)

    # 3) averages-by-subreddit (only avg > 0)
    count_sum = commentbysub.mapValues(lambda c: (1, float(c.get('score', 0.0)))) \
                            .reduceByKey(add_count_sum)
    avgbysub  = count_sum.mapValues(lambda cs: cs[1] / cs[0]) \
                         .filter(lambda kv: kv[1] > 0)

    # 4) Collect small averages and broadcast
    count = avgbysub.count()
    print("Number of subreddits:", count)
    if count > 50000:
        raise RuntimeError("Too many subreddits to broadcast safely!")
    # NOTE: This is safe because the number of subreddits is small (e.g., <= ~50k).
    averages_dict = dict(avgbysub.collect())   # ← add a size-bound comment in your code
    b_averages = sc.broadcast(averages_dict)
    # {"python": 12.5, "datascience": 8.3, "machinelearning": 15.2}

    # 5) Map over comments using the broadcast to compute (relative_score, author)
    rel_author = commentdata.map(lambda c: rel_score_from_bcast(b_averages, c)) \
                            .filter(lambda x: x is not None)

    # 6) Sort by relative score (descending) and write (don’t coalesce)
    rel_author.sortBy(lambda t: t[0], ascending=False) \
              .map(lambda t: f"{t[0]}\t{t[1]}") \
              .saveAsTextFile(output)
                # 2.0 alice
                # 1.23 carol
                # 0.48 bob
    sc.stop()

if __name__ == "__main__":
    inputs, output = sys.argv[1], sys.argv[2]
    main(inputs, output)