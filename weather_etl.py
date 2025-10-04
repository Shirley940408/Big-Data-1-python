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
             .appName("weather_etl")
             .getOrCreate())

    # 1) Read CSV with schema
    weather = spark.read.csv(inputs, schema=observation_schema)

    # 2) Filter rows we care about:
    #    - qflag is null (good quality)
    #    - station starts with 'CA' (Canada)
    #    - observation == 'TMAX' (max temperature)
    good = (weather
            .where(functions.col('qflag').isNull())
            .where(functions.col('station').startswith('CA'))
            .where(functions.col('observation') == functions.lit('TMAX')))

    # 3) Convert tenths of °C → °C, keep required columns
    etl_data = (good
                .withColumn('tmax', (functions.col('value') / functions.lit(10.0)).cast('double'))
                .select('station', 'date', 'tmax'))

    # 4) Write as gzip-compressed JSON (one object per line)
    etl_data.write.json(output, compression='gzip', mode='overwrite')

    spark.stop()
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
    main(inputs, output)