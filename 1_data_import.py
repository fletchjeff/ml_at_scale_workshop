## Import the data
# This file was downloaded from Kaggle as a CSV and upload to S3 in the previous step. Since we know the schema already, we can make its 
# correct by defining the schema for the import rather than relying on inferSchema. Its also faster!

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

storage = os.getenv("STORAGE")

spark = SparkSession\
    .builder\
    .appName("Data Import")\
    .config("spark.executor.memory","8g")\
    .config("spark.executor.cores","4")\
    .config("spark.driver.memory","6g")\
    .config("spark.yarn.access.hadoopFileSystems",storage)\
    .getOrCreate()
    
flights_path = storage + "/datalake/data/airlines/csv/*"

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

# Add in a colum for the week of the year

### Create the Hive table
# This is here to create the table in Hive used be the other parts of the project, if it
# does not already exist.

spark.sql("show databases").show()
spark.sql("show tables in default").show()

if ('full_flight_table' not in list(spark.sql("show tables in default").toPandas()['tableName'])):
    print("creating the full_flight_table table")
    flight_raw_df\
        .write.format("parquet")\
        .mode("overwrite")\
        .saveAsTable(
            'default.full_flight_table',
        )

        
spark.sql("select * from default.full_flight_table limit 10").show()