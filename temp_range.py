from pyspark.sql import SparkSession, functions, types
import sys

# Explicit schema for GHCN daily files
observation_schema = types.StructType([
    types.StructField('station',     types.StringType()),
    types.StructField('date',        types.StringType()),   # yyyymmdd
    types.StructField('observation', types.StringType()),   # e.g., TMAX
    types.StructField('value',       types.IntegerType()),  # tenths of °C
    types.StructField('mflag',       types.StringType()),
    types.StructField('qflag',       types.StringType()),   # quality flag
    types.StructField('sflag',       types.StringType()),
    types.StructField('obstime',     types.StringType()),
])

# add more functions as necessary
def main(inputs, output):
    spark = (SparkSession
             .builder
             .appName("temp_range")
             .getOrCreate())

    # 1) Read CSV with schema with unqualified row removed
    weather = spark.read.csv(inputs, schema=observation_schema).filter((functions.col('qflag').isNull()) | (functions.col('qflag') == ''))
    # 2) Filter to two df we care
    #    - observation == 'TMAX' (max temperature)
    #    - observation == 'TMIN' (min temperature)
    tmax = weather.filter(weather['observation'] == 'TMAX').select(
        functions.col('date'),
        functions.col('station'),
        functions.col('value').alias('tmax')
    )
    tmin = weather.filter(weather['observation'] == 'TMIN').select(
        functions.col('date'),
        functions.col('station'),
        functions.col('value').alias('tmin')
    )
    # 3) Join to new df and Convert tenths of °C
    range_weather = tmax.join(tmin, on = ['date', 'station'])\
        .withColumn('range', ((functions.col('tmax')-functions.col('tmin'))/functions.lit(10.0)).cast('double'))\
        .select(functions.col('date'), functions.col('station'), functions.col('range')).cache()

    range_weather.orderBy('date', 'station').write.csv(output, mode='overwrite',header=True)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
    main(inputs, output)