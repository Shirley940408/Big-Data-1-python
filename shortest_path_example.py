# shortest_path_example.py
# 用法:
#   spark-submit shortest_path_example.py <input_dir> <output_dir> <src> <dst>
# 例如:
#   spark-submit shortest_path_example.py graph-1 output 1 4

import sys
from pyspark.sql import SparkSession, functions as F, Window

def parse_edges(spark, input_dir):
    # 只读 links-simple-sorted.txt，每行形如 "1: 3 5"
    lines = spark.read.text(f"{input_dir}/links-simple-sorted.txt")

    src_col  = F.regexp_extract("value", r"^(\d+)\s*:", 1).alias("src")
    nbrs_str = F.regexp_extract("value", r":\s*(.*)$", 1).alias("nbrs")

    edges = (
        lines.select(src_col, nbrs_str)
             .withColumn("nbr_arr", F.split(F.col("nbrs"), r"\s+"))
             .withColumn("dst", F.explode("nbr_arr"))
             .filter(F.col("dst") != "")
             .withColumn("dst", F.col("dst"))
             .select("src", "dst")
             .withColumn("w", F.lit(1.0))     # 无权图: 权重=1
             .cache()
    )
    return edges

def keep_min_per_node(df):
    # 对每个 node 仅保留 dist 最小的那行 (保留相应 prev)
    w = Window.partitionBy("node").orderBy(F.col("dist").asc())
    return df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

def main():
    if len(sys.argv) != 5:
        print("Usage: spark-submit shortest_path_example.py <input_dir> <output_dir> <src> <dst>")
        sys.exit(1)

    input_dir, out_dir, SRC, DST = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
    MAX_ITERS = 6  # 题目示例的小图固定 6 步上限

    spark = SparkSession.builder.appName("ShortestPath-DF").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    edges = parse_edges(spark, input_dir)  # columns: src,dst,w
    # 当前最优路径集合: (node, dist, prev). 起点只有一行
    paths = spark.createDataFrame([(SRC, 0.0, None)], ["node","dist","prev"])
    frontier = paths

    for i in range(1, MAX_ITERS + 1):
        # 1) frontier 扩展一跳 → 候选
        candidates = (
            frontier.alias("p")
                    .join(edges.alias("e"), F.col("p.node") == F.col("e.src"))
                    .select(F.col("e.dst").alias("node"),
                            (F.col("p.dist") + F.col("e.w")).alias("dist"),
                            F.col("p.node").alias("prev"))
        )

        # 2) 合并并对每个 node 取最小 dist
        all_paths = paths.unionByName(candidates)
        new_paths = keep_min_per_node(all_paths)

        # 3) 生成下一轮 frontier（这轮变小/新出现的）
        old = paths.select("node", F.col("dist").alias("old_dist"))
        frontier = (
            new_paths.alias("n").join(old, on="node", how="left")
                     .filter(F.col("old_dist").isNull() | (F.col("n.dist") < F.col("old_dist")))
                     .select("node","dist","prev")
        )

        # 输出 iter-i（文本: node\t dist\t prev(无则-1)）
        (new_paths
            .select(F.expr("concat(node,'\t',dist,'\t',coalesce(prev,-1)) as line"))
            .coalesce(1).write.mode("overwrite").text(f"{out_dir}/iter-{i}")
        )

        # 若无改进则提前结束
        if frontier.rdd.isEmpty():
            paths = new_paths
            break

        paths = new_paths

    # —— 回溯 DST → ... → SRC ，写 path/ —— #
    prev_map = {r["node"]: r["prev"] for r in paths.select("node","prev").collect()}
    dist_map = {r["node"]: r["dist"] for r in paths.select("node","dist").collect()}

    path_nodes = []
    cur = DST
    if cur in dist_map:  # 可达
        while cur is not None:
            path_nodes.append(cur)
            if cur == SRC:
                break
            cur = prev_map.get(cur)
        path_nodes.reverse()
    else:
        path_nodes = []  # 不可达：输出空

    (spark.createDataFrame([(str(x),) for x in path_nodes], ["line"])
          .coalesce(1).write.mode("overwrite").text(f"{out_dir}/path"))

    spark.stop()

if __name__ == "__main__":
    main()