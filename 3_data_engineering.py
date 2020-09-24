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
    .appName("Data Engineering")\
    .config("spark.executor.memory","8g")\
    .config("spark.executor.cores","4")\
    .config("spark.driver.memory","6g")\
    .config("spark.yarn.access.hadoopFileSystems",storage)\
    .getOrCreate()

### Create the smaller hive table used for model training

flight_raw_df = spark.sql("select * from default.full_flight_table")

## Add in Week of year
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

if ('smaller_flight_table' not in list(spark.sql("show tables in default").toPandas()['tableName'])):
    print("creating the smaller_flight_table table")
    smaller_data_set\
        .write.format("parquet")\
        .mode("overwrite")\
        .saveAsTable(
            'default.smaller_flight_table',
        )


spark.sql("select * from default.smaller_flight_table limit 10").show()

#spark.stop()
