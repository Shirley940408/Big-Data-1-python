# spark-submit weather_train.py /courses/732/tmax-1 weather-model
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
        SELECT
            station,
            date,
            latitude,
            longitude,
            elevation,
            tmax,
            dayofyear(date) AS day_of_year
        FROM __THIS__       
        """
    )
    sql_add_yesterday = SQLTransformer(
        statement="""
            SELECT
                today.station,
                today.date,
                today.latitude,
                today.longitude,
                today.elevation,
                today.tmax,
                today.day_of_year,
                yesterday.tmax AS yesterday_tmax
            FROM __THIS__ AS today
            INNER JOIN __THIS__ AS yesterday
              ON date_sub(today.date, 1) = yesterday.date
             AND today.station = yesterday.station
        """
    )
    weather_assembler = VectorAssembler(
        inputCols=["latitude", "longitude", "elevation", "day_of_year"],
        outputCol="features"
    )
    weather_assembler_add_yesterday = VectorAssembler(
        inputCols=["latitude", "longitude", "elevation", "day_of_year", "yesterday_tmax"],
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

    # RandomForest
    rf_regressor_add_yesterday = RandomForestRegressor(
        featuresCol="features",
        labelCol="tmax",
        numTrees=50,
        maxDepth=10,
        seed=42
    )
    pipeline = Pipeline(stages=[sql_day_of_year, weather_assembler, rf_regressor])
    pipeline_add_yesterday = Pipeline(stages=[sql_day_of_year, sql_add_yesterday,  weather_assembler_add_yesterday, rf_regressor_add_yesterday])
    model = pipeline.fit(train)
    model_add_yesterday = pipeline_add_yesterday.fit(train)
    # Make predictions
    predictions = model.transform(validation)
    prediction_add_yesterday = model_add_yesterday.transform(validation)
    # Save model
    # model.write().overwrite().save(output)
    model_add_yesterday.write().overwrite().save(output) # If you need to test the previous model, please comment model_add_yesterday and uncomment model line
    # Evaluation
    def evaluate(preds, name):
        for metric in ["rmse", "r2"]:
            evaluator = RegressionEvaluator(
                labelCol="tmax",
                predictionCol="prediction",
                metricName=metric
            )
            score = evaluator.evaluate(preds)
            print(f"Validation {metric} for {name}: {score:g}")

    evaluate(predictions, "baseline RF")
    evaluate(prediction_add_yesterday, "RF with yesterday_tmax")

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)
