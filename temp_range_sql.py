from pyspark.sql import SparkSession, types
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
             .appName("temp_range_sql")
             .getOrCreate())

    spark.read.csv(inputs, schema=observation_schema).createOrReplaceTempView("weather")

    spark.sql("""
            SELECT station, date, observation, value
            FROM weather
            WHERE qflag IS NULL or qflag = ''
            """).createOrReplaceTempView("good")

    spark.sql("""
            SELECT station, date, value AS tmax
            FROM good 
            WHERE observation = 'TMAX'
            """).createOrReplaceTempView("tmax")

    spark.sql("""
            SELECT station, date, value AS tmin
            FROM good
            WHERE observation = 'TMIN'
            """).createOrReplaceTempView("tmin")

    spark.sql("""
              SELECT tmax.date, tmax.tmax, tmax.station, ROUND((tmax.tmax - tmin.tmin)/10.0, 1) AS range
              FROM tmax 
                       
                       tmin ON tmax.station = tmin.station AND tmin.date = tmax.date
              """).createOrReplaceTempView("range_weather")

    result = spark.sql("""
            SELECT  date, station, range
            FROM range_weather
            ORDER BY range_weather.date, range_weather.station
            """)

    result.write.csv(output, mode='overwrite',header=True)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
    main(inputs, output)
