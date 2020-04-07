from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

spark = SparkSession\
    .builder\
    .appName("Airlines Part2 Data Engineering ")\
    .config("spark.executor.memory","8g")\
    .config("spark.executor.cores","4")\
    .config("spark.driver.memory","6g")\
    .config("spark.yarn.access.hadoopFileSystems","s3a://jf-workshop-mod-env-cdp-bucket/")\
    .getOrCreate()
    
flights_path="s3a://jf-workshop-mod-env-cdp-bucket/data/airlines/csv/*"

# ## Read Data from file

schema = StructType([StructField("FL_DATE", TimestampType(), True),
    StructField("OP_CARRIER", StringType(), True),
    StructField("OP_CARRIER_FL_NUM", StringType(), True),
    StructField("ORIGIN", StringType(), True),
    StructField("DEST", StringType(), True),
    StructField("CRS_DEP_TIME", StringType(), True),
    StructField("DEP_TIME", StringType(), True),
    StructField("DEP_DELAY", DoubleType(), True),
    StructField("TAXI_OUT", DoubleType(), True),
    StructField("WHEELS_OFF", StringType(), True),
    StructField("WHEELS_ON", StringType(), True),
    StructField("TAXI_IN", DoubleType(), True),
    StructField("CRS_ARR_TIME", StringType(), True),
    StructField("ARR_TIME", StringType(), True),
    StructField("ARR_DELAY", DoubleType(), True),
    StructField("CANCELLED", DoubleType(), True),
    StructField("CANCELLATION_CODE", StringType(), True),
    StructField("DIVERTED", DoubleType(), True),
    StructField("CRS_ELAPSED_TIME", DoubleType(), True),
    StructField("ACTUAL_ELAPSED_TIME", DoubleType(), True),
    StructField("AIR_TIME", DoubleType(), True),
    StructField("DISTANCE", DoubleType(), True),
    StructField("CARRIER_DELAY", DoubleType(), True),
    StructField("WEATHER_DELAY", DoubleType(), True),
    StructField("NAS_DELAY", DoubleType(), True),
    StructField("SECURITY_DELAY", DoubleType(), True),
    StructField("LATE_AIRCRAFT_DELAY", DoubleType(), True)])


flight_raw_df = spark.read.csv(
    path=flights_path,
    header=True,
    schema=schema,
    sep=',',
    nullValue='NA'
)

#from pyspark.sql.types import StringType	
#from pyspark.sql.functions import udf,weekofyear

flight_raw_df = flight_raw_df.withColumn('WEEK',weekofyear('FL_DATE').cast('double'))

smaller_data_set = flight_raw_df.select(	
  "WEEK",	
  "FL_DATE",
  "OP_CARRIER",
  "OP_CARRIER_FL_NUM",
  "ORIGIN",
  "DEST",
  "CRS_DEP_TIME",
  "CRS_ARR_TIME",
  "CANCELLED",
  "CRS_ELAPSED_TIME",
  "DISTANCE"
)

# #### Commented out as it has already been run
# smaller_data_set.write.parquet(
#  path="s3a://jf-workshop-mod-env-cdp-bucket/data/airlines/airline_parquet",
#  mode='overwrite',
#  compression="snappy")
#
#smaller_data_set.write.saveAsTable(
#  'default.smaller_flight_table',
#   format='parquet', 
#   mode='overwrite')

spark.sql("select * from default.smaller_flight_table limit 10").show()

#spark.stop()
