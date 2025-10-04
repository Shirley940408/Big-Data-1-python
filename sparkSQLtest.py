from pyspark.shell import spark
from pyspark.sql import functions, types
inputs = '/courses/732/reddit-1/' # or other path on your computer
comments = spark.read.json(inputs)
averages = comments.groupby('subreddit').agg(functions.avg(comments['score']))
averages.show()