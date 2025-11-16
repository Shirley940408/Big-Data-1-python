import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
# command:  spark-submit colour_predict.py /courses/732/colour-words-1
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').config("spark.sql.catalogImplementation", "in-memory").getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from colour_tools import colour_schema, rgb2lab_query, plot_predictions


def main(inputs):
    data = spark.read.csv(inputs, schema=colour_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    
    # TODO: create a pipeline to predict RGB colours -> word
    rgb_assembler = VectorAssembler(
        inputCols=["R", "G", "B"],
        outputCol="features"
    )
    word_indexer = StringIndexer(inputCol="word", outputCol="label")
    classifier = MultilayerPerceptronClassifier(
        layers=[3, 30, 11], # 输入 3，隐藏层 30，输出 11（对应 11 种颜色）
        featuresCol="features",
        labelCol="label",
        maxIter=100,
        seed=42)

    rgb_pipeline = Pipeline(stages=[rgb_assembler, word_indexer, classifier])
    rgb_model = rgb_pipeline.fit(train)

    # TODO: create an evaluator and score the validation data
    # Make predictions
    predictions = rgb_model.transform(validation)

    # Initialize the evaluator for accuracy
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    )

    # Compute the validation score (e.g., accuracy)
    score = evaluator.evaluate(predictions)

    plot_predictions(rgb_model, 'RGB', labelCol='word')
    print('Validation score for RGB model: %g' % (score, ))
    

    # TODO: create a pipeline RGB colours -> LAB colours -> word; train and evaluate.
    rgb_to_lab_query = rgb2lab_query(passthrough_columns=[])
    lab_sql = SQLTransformer(
        statement=rgb_to_lab_query,
    )
    lab_assembler = VectorAssembler(
        inputCols=["labL", "labA", "labB"],
        outputCol="features"
    )
    lab_word_indexer = StringIndexer(inputCol="word", outputCol="label")

    lab_classifier = MultilayerPerceptronClassifier(
        layers=[3, 30, 11], # 输入 3，隐藏层 30，输出 11（对应 11 种颜色）
        featuresCol="features",
        labelCol="label",
        maxIter=100,
        seed=42)

    lab_pipeline = Pipeline(stages=[
        lab_sql,
        lab_assembler,
        lab_word_indexer,
        lab_classifier
    ])

    lab_model = lab_pipeline.fit(train)
    # Make predictions
    lab_predictions = lab_model.transform(validation)

    # Initialize the evaluator for accuracy
    lab_evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    )
    # Compute the validation score (e.g., accuracy)
    score = lab_evaluator.evaluate(lab_predictions)
    plot_predictions(lab_model, 'LAB', labelCol='word')
    print('Validation score for LAB model: %g' % (score, ))

if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
