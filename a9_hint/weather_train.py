# spark-submit weather_train.py tmax-1 weather-model
from pyspark.sql import SparkSession, types
import sys
assert sys.version_info >= (3, 5)

from pyspark.ml import Pipeline
from pyspark.ml.feature import SQLTransformer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather training').getOrCreate()

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])

def main(inputs, output):
    data = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    # TODO: SQLTransformer: convert date -> day-of-year
    sql_day_of_year = SQLTransformer(
        statement="""
            SELECT latitude, longitude, elevation, dayofyear(date) as day_of_year, tmax FROM __THIS__
        """
    )
    weather_assembler = VectorAssembler(
        inputCols=["latitude", "longitude", "elevation", "day_of_year"],
        outputCol="features"
    )
    # RandomForest
    rf_regressor = RandomForestRegressor(
        featuresCol="features",
        labelCol="tmax",
        numTrees=50,
        maxDepth=10,
        seed=42
    )

    pipeline = Pipeline(stages=[sql_day_of_year, weather_assembler, rf_regressor])
    model = pipeline.fit(train)

    # Make predictions
    predictions = model.transform(validation)

    # Save model
    model.write().overwrite().save(output)

    # Evaluation
    def evaluate(predictions, method = "rmse"):
        evaluator_rmse = RegressionEvaluator(
            labelCol="tmax",
            predictionCol="prediction",
            metricName= method
        )

        # Compute the validation score
        score = evaluator_rmse.evaluate(predictions)
        print(f'Validation {method} for random forest model: %g' % (score,))

    evaluate(predictions)
    evaluate(predictions, "r2")

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)
