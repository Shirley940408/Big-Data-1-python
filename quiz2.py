from pyspark.sql import SparkSession, functions
from pyspark.sql.connect.functions import aggregate

spark = SparkSession.builder.appName("readings pivot example").getOrCreate()

data = [
    ('2025-10-01', 'Surrey', 17.5 ),
    ('2025-10-01', 'Burnaby',  18.9 ),
    ('2025-10-02', 'Surrey', 16.0),
    ('2025-10-02', 'Burnaby', 17.2)
]



columns = ['timestamp','sensor', 'temp']
readings = spark.createDataFrame(data, columns)

holiday_data = []
columns_holiday = ['date', 'holiday']
holidays = spark.createDataFrame(holiday_data, columns_holiday)

surrey = readings.filter(readings.sensor == 'Surrey').select('timestamp', functions.col('temp').alias('surrey'))
burnaby = readings.filter(readings.sensor == 'Burnaby').select('timestamp', functions.col('temp').alias('burnaby'))
combined = surrey.join(burnaby, on='timestamp')
combined.show()

joined_df = readings.join(holidays, holidays['date'] =from pyspark.sql import SparkSession, functions

spark = SparkSession.builder.appName("readings pivot example").getOrCreate()

data = [
    ('2025-10-01', 'Surrey', 17.5 ),
    ('2025-10-01', 'Burnaby',  18.9 ),
    ('2025-10-02', 'Surrey', 16.0),
    ('2025-10-02', 'Burnaby', 17.2)
]



columns = ['timestamp','sensor', 'temp']
readings = spark.createDataFrame(data, columns)

holiday_data = []
columns_holiday = ['date', 'holiday']
holidays = spark.createDataFrame(holiday_data, columns_holiday)

surrey = readings.filter(readings.sensor == 'Surrey').select('timestamp', functions.col('temp').alias('surrey'))
burnaby = readings.filter(readings.sensor == 'Burnaby').select('timestamp', functions.col('temp').alias('burnaby'))
combined = surrey.join(burnaby, on='timestamp')
combined.show()

features = readings.join(holidays, holidays['date'] == readings['timestamp'],how ='left')\
    .select('timestamp', 'sensor', 'temp',
            (functions.col('temp') * 9/5 + 32).alias('temp_F'),
            ((functions.dayofweek('timestamp') % 7 == 1) ｜ (functions.dayofweek('timestamp') % 7 == 7)).alias('is_weekend')
            (functions.col('holiday').isNotNull().alias('is_holiday'))
            )
non_holidays = features.filter(features['is_holiday'] == False).select('sensor', functions.weekofyear('timestamp').alias('week'), 'temp')
average_temp_per_week = non_holidays.groupBy(['sensor', 'week'])).agg(functions.avg('temp').alias('avg_temp_per_week'))

max_avg_temp = (average_temp_per_week.groupBy('sensor').agg(functions.max('avg_temp_per_week').alias('max_avg_temp'))
result_week = max_avg_temp.join(average_temp_per_week, on=['sensor', max_avg_temp['max_avg_temp'] == average_temp_per_week['avg_temp_per_week']]).select('sensor','week', 'max_avg_temp'))