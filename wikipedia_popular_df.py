from pyspark.sql import SparkSession, functions, types, Row
import sys
import re, os

from pyspark.sql.types import IntegerType

inputs = sys.argv[1]
output = sys.argv[2]
spark = SparkSession.builder.appName('wikipedia_popular_dataframe').getOrCreate()
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def extract_hour_from_path(path: str) -> str|None:
    """从文件路径中提取 hour 信息"""
    # 去掉目录和协议
    basename = os.path.basename(path)  # 取出最后部分，如 pagecounts-20160801-120000.gz

    # 匹配文件名模式
    m = re.search(r'pagecounts-(\d{8})-(\d{6})', basename)
    if not m:
        return None
    date, hour = m.groups()
    # 返回你需要的格式，比如 "20160801-12"
    return f"{date}-{hour[:2]}"

hour_udf = functions.udf(extract_hour_from_path, types.StringType())

def main(inputs, output):
    raw = (spark.read.text(inputs).withColumn('filename', functions.input_file_name()).cache())
    parts = functions.split(functions.col('value'), r'\s+')
    # 1) 先丢弃坏行：确保至少有 4 列
    df = raw.where(functions.size(parts) == 4).select(parts.getItem(0).alias('lang'),
                    parts.getItem(1).alias('title'),
                    parts.getItem(2).alias('views_str'),
                    hour_udf(functions.col('filename')).alias('hour'),
                    )
    # 2) 过滤语言为 en
    df = df.where(functions.col('lang') == 'en')

    # 3) 去掉 Main_Page 和 Special:
    df = df.where((functions.col('title') != 'Main_Page')&
                  (~functions.col('title').startswith('Special')))

    # 4) views/bytes 转数值，并过滤非数字:
    df = df.withColumn('views', functions.col('views_str').cast(IntegerType())).filter(functions.col('views').isNotNull()).cache()

    # 5) get the max_per_hour dataframe result
    max_per_hour = df.groupby("hour").agg(functions.max("views").alias("max_views")).cache()

    # 6) join with the previous dataframe
    # joined = df.join(max_per_hour, on="hour")
    joined_broadcast = df.join(functions.broadcast(max_per_hour), on="hour", how='inner')

    # 7) 只保留 views == max_views 的行（自动保留并列）
    top_view = (joined_broadcast
           .where(joined_broadcast['views'] == joined_broadcast["max_views"])
           .select("hour", "title", "views"))

    # 8) write the result with json
    top_view.orderBy('hour', 'title').write.json(output, mode ='overwrite')
    top_view.explain(True)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)