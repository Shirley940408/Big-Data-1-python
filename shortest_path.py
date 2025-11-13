# shortest_path_example.py
# 用法:
#   spark-submit shortest_path.py <input_dir> <output_dir> <src> <dst>
# 例如:/courses/732/graph-1
#   spark-submit shortest_path.py /courses/732/graph-1 output 1 4
import sys
import re

from cffi.model import StructType
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row, functions, types
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructField, IntegerType


def get_src_dst_pair(line):
    pattern = re.compile(r'^(\d+):\s*(.*)$')
    match = pattern.match(line)
    if match:
        for value in match.group(2).split():
            yield int(match.group(1)), int(value)

def main():
    input_dir, out_dir, start, end = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
    text = sc.textFile(input_dir)
    src_dst_pair = text.flatMap(get_src_dst_pair)
    graph_sides = src_dst_pair.toDF(['src','dst'])
    graph_sides_weight = graph_sides.withColumn('weight', lit(1.0)).cache()

    schema = types.StructType([
        StructField("node", IntegerType(), True),
        StructField("dist", IntegerType(), True),
        StructField("prev", IntegerType(), True),
    ])

    node_distance_prev = spark.createDataFrame([], schema=schema)
    frontier = spark.createDataFrame([Row(start, 0, -1)], schema=schema)

    # bad example below, just for review purpose
    # while(frontier.count()):
    #     cur_df = node_distance_prev.unionAll(frontier)
    #     # Step 1: 找出每个 node 的最小距离
    #     best_dist = cur_df.groupBy("node").agg(functions.min("min_dst").alias('best_dist'))
    #
    #     # Step 2: 回连拿 prev 去除多余非最优解
    #     node_distance_prev = (
    #         cur_df.join(best_dist, (cur_df.node == best_dist.node) &
    #                     (cur_df.min_dst == best_dist.best_dist))
    #        .select(cur_df["node"], cur_df["min_dst"], cur_df["prev"])
    #     )
    #
    #     frontier_valid = (
    #         node_distance_prev.join(frontier, (node_distance_prev.src == frontier.node) &
    #                                 (node_distance_prev.min_dst == frontier.min_dst))
    #         .select(node_distance_prev["node"], node_distance_prev["min_dst"], node_distance_prev["prev"])
    #     )
    #     best_dist_in_valid = frontier_valid.agg(functions.min("min_dst"))
    #     next_edge = frontier_valid.filter(col('min_dst') == best_dist_in_valid).limit(1)
    #
    #     new_links = graph_sides_weight.join(next_edge, graph_sides_weight.src == next_edge.node)
    #     frontier = new_links.select(
    #         col('dst').alias('node'),
    #         (col('min_dst') + col('weight')).alias('min_dst'),
    #         col('node').alias('prev'),
    #     )

    MAX_ITERS = 6
    for i in range(MAX_ITERS):
        # 1) 合并“已有最优”与“本轮候选”
        cur_df = node_distance_prev.unionByName(frontier)

        # 2) 对每个 node 只保留最小 dist（带对应 prev）
        best = cur_df.groupBy("node").agg(functions.min("dist").alias("best_dist"))
        new_paths = (cur_df.join(best, on="node", how="inner")
                     .filter(col("dist") == col("best_dist"))
                     .select("node", "dist", "prev")).cache()

        # 3) 生成“本轮新变好的节点” → 下一轮要扩的 frontier
        old = node_distance_prev.select(
            "node", col("dist").alias("old_dist"))

        next_frontier = (new_paths.alias("n").join(old, on="node", how="left")
                         .filter(col("old_dist").isNull() | (col("n.dist") < col("old_dist"))) #filter(col("old_dist").isNull() 代表新出现的点，(col("n.dist") < col("old_dist")代表更新过的点
                         .select(col("n.node").alias("node"),
                                 col("n.dist").alias("dist"),
                                 col("n.prev").alias("prev"))).cache()

        # 4) 早停：没有任何新出现/变短
        if len(next_frontier.head(1)):
            node_distance_prev = new_paths
            break

        # 5) 扩展所有“本轮新变好的节点”以产生 candidates（不要只挑一个）
        candidates = (next_frontier.alias("p")
                      .join(graph_sides_weight.alias("e"), col("p.node") == col("e.src"))
                      .select(col("e.dst").alias("node"),
                              (col("p.dist") + col("e.weight")).alias("dist"),
                              col("p.node").alias("prev")))

        # 6) 更新状态，进入下一轮
        node_distance_prev = new_paths
        frontier = candidates
        new_paths.write.mode("overwrite").csv(out_dir + f"/iter-{i}")

    # 最后输出完整路径表
    # node_distance_prev.write.mode("overwrite").csv(out_dir + "/path")
    # 结束后 node_distance_prev 是 DataFrame(node, dist, prev)
    mapping = {row['node']: row['prev'] for row in node_distance_prev.collect()}

    path = []
    cur = end
    while cur != -1:
        path.append(cur)
        cur = mapping.get(cur, -1)

    path.reverse()

    # 输出成 DataFrame（每行一个节点）
    path_df = spark.createDataFrame([(n,) for n in path], ["node"])
    path_df.write.mode("overwrite").text(out_dir + "/path")

if __name__ == "__main__":
    conf = SparkConf().setAppName('dijkstra shortest path')
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName('dijkstra shortest path').getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    main()