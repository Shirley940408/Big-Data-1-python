# spark-submit weather_tomorrow.py weather-model
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, types
import sys
assert sys.version_info >= (3, 5)

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import SQLTransformer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather tomorrow').getOrCreate()

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])

def main(model_path):
    model = PipelineModel.load(model_path)

    station = 'sfu_campus'
    latitude = 49.2771
    longitude = -122.9146
    elevation = 330.0
    tmax = 12.0
    tmax_tomorrow_temp = 0.0
    due_date_obj = datetime(2025, 11, 21)
    due_date = due_date_obj.date()
    tomorrow_date = (due_date_obj + timedelta(days=1)).date()

    # station, date, latitude, longitude ,elevation, tmax
    data = [
        (station, due_date, latitude, longitude, elevation, tmax),
        (station, tomorrow_date, latitude, longitude, elevation, tmax_tomorrow_temp),
    ]

    df = spark.createDataFrame(data, schema=tmax_schema)
    prediction = model.transform(df)
    prediction.printSchema()
    prediction.show()
    # select the prediction col where date is tomorrow
    tomorrow_pred = prediction.filter(functions.col('date') == tomorrow_date).select('prediction').collect()[0][0]
    print('Predicted tmax tomorrow:', tomorrow_pred)

if __name__ == '__main__':
    model_file = sys.argv[1]
    main(model_file)