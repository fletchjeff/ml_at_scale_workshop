import cdsw
import os

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, OneHotEncoderEstimator
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf,substring,weekofyear,concat,col,when,length,lit
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

storage = os.getenv("STORAGE")

spark = SparkSession\
    .builder\
    .appName("Model Training")\
    .config("spark.executor.memory","8g")\
    .config("spark.executor.cores","4")\
    .config("spark.driver.memory","6g")\
    .config("spark.yarn.access.hadoopFileSystems",storage)\
.getOrCreate()

flight_df_original = spark.sql("select * from smaller_flight_table")

flight_df = flight_df_original.na.drop()
flight_df.persist()

flight_df = flight_df\
   .withColumn(
       'CRS_DEP_HOUR',
       when(
           length(col("CRS_DEP_TIME")) == 4,col("CRS_DEP_TIME")
       )\
       .otherwise(concat(lit("0"),col("CRS_DEP_TIME")))
   )

flight_df = flight_df.withColumn('CRS_DEP_HOUR',col('CRS_DEP_HOUR').cast('double'))
flight_df = flight_df.withColumn('WEEK',weekofyear('FL_DATE').cast('double'))


numeric_cols = ["CRS_ELAPSED_TIME","DISTANCE","WEEK","CRS_DEP_HOUR"]

op_carrier_indexer = StringIndexer(inputCol ='OP_CARRIER', outputCol = 'OP_CARRIER_INDEXED',handleInvalid="keep")

origin_indexer = StringIndexer(inputCol ='ORIGIN', outputCol = 'ORIGIN_INDEXED',handleInvalid="keep")

dest_indexer = StringIndexer(inputCol ='DEST', outputCol = 'DEST_INDEXED',handleInvalid="keep")


indexer_encoder = OneHotEncoderEstimator(
    inputCols = ['OP_CARRIER_INDEXED','ORIGIN_INDEXED','DEST_INDEXED'],
    outputCols= ['OP_CARRIER_ENCODED','ORIGIN_ENCODED','DEST_ENCODED']
)

input_cols=[
    'OP_CARRIER_ENCODED',
    'ORIGIN_ENCODED',
    'DEST_ENCODED'] + numeric_cols

assembler = VectorAssembler(
    inputCols = input_cols,
    outputCol = 'features')

if len (sys.argv) == 4:
  try:
    maxIter = int(sys.argv[1])
    elasticNetParam = float(sys.argv[2])
    regParam = float(sys.argv[3])    
  except:
    sys.exit("Invalid Arguments passed to Experiment")
else:
  maxIter=100
  elasticNetParam = 0.0
  regParam = 0.01


lr = LogisticRegression(
  featuresCol = 'features', 
  labelCol = 'CANCELLED', 
  maxIter=maxIter, 
  elasticNetParam = elasticNetParam,
  regParam = regParam
)

pipeline = Pipeline(
  stages=[        
    op_carrier_indexer,
    origin_indexer,
    dest_indexer,
    indexer_encoder,
    assembler,
    lr
  ]
)

(train, test) = flight_df.randomSplit([0.7, 0.3])

lrModel = pipeline.fit(train)

predictionslr = lrModel.transform(test)
evaluator = BinaryClassificationEvaluator(labelCol="CANCELLED",metricName="areaUnderROC")

AUROC_val = evaluator.evaluate(predictionslr)
AUROC_val

# Uncomment if running in an experiment
cdsw.track_metric("maxIter", maxIter)
cdsw.track_metric("elasticNetParam", elasticNetParam)
cdsw.track_metric("regParam", regParam)
cdsw.track_metric("AUROC", round(float(AUROC_val),3))


## Commented out as its already aone
#lrModel.write().overwrite().save(storage + "/datalake/data/airlines/lr-model")
