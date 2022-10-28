import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score, log_loss
# import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
# from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler,Imputer,StringIndexer, OneHotEncoder
# from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.ml.feature import StandardScaler
from pyspark.sql.functions import split,from_json,col
# from pyspark.sql.types import StringType,DoubleType,LongType
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, TimestampType
import warnings
warnings.filterwarnings('ignore')

######### spark settings:
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

schema = StructType([StructField('id', IntegerType(), True), StructField('loanAmnt', DoubleType(), True), StructField('term', IntegerType(), True), StructField('interestRate', DoubleType(), True), StructField('installment', DoubleType(), True), StructField('grade', StringType(), True), StructField('subGrade', StringType(), True), StructField('employmentTitle', DoubleType(), True), StructField('employmentLength', StringType(), True), StructField('homeOwnership', IntegerType(), True), StructField('annualIncome', DoubleType(), True), StructField('verificationStatus', IntegerType(), True), StructField('issueDate', TimestampType(), True), StructField('isDefault', IntegerType(), True), StructField('purpose', IntegerType(), True), StructField('postCode', DoubleType(), True), StructField('regionCode', IntegerType(), True), StructField('dti', DoubleType(), True), StructField('delinquency_2years', DoubleType(), True), StructField('ficoRangeLow', DoubleType(), True), StructField('ficoRangeHigh', DoubleType(), True), StructField('openAcc', DoubleType(), True), StructField('pubRec', DoubleType(), True), StructField('pubRecBankruptcies', DoubleType(), True), StructField('revolBal', DoubleType(), True), StructField('revolUtil', DoubleType(), True), StructField('totalAcc', DoubleType(), True), StructField('initialListStatus', IntegerType(), True), StructField('applicationType', IntegerType(), True), StructField('earliesCreditLine', StringType(), True), StructField('title', DoubleType(), True), StructField('policyCode', DoubleType(), True), StructField('n0', DoubleType(), True), StructField('n1', DoubleType(), True), StructField('n2', DoubleType(), True), StructField('n3', DoubleType(), True), StructField('n4', DoubleType(), True), StructField('n5', DoubleType(), True), StructField('n6', DoubleType(), True), StructField('n7', DoubleType(), True), StructField('n8', DoubleType(), True), StructField('n9', DoubleType(), True), StructField('n10', DoubleType(), True), StructField('n11', DoubleType(), True), StructField('n12', DoubleType(), True), StructField('n13', DoubleType(), True), StructField('n14', DoubleType(), True)])

print("\n Starting receving streaming data and prediction: \n")
# receivedStreamingData = spark.readStream \
#           .format("kafka") \
#           .option("kafka.bootstrap.servers", "localhost:9092") \
#           .option("subscribe", "creditPrediction") \
#           .load()

receivedStreamingData = spark.readStream \
          .format("kafka") \
          .option("kafka.bootstrap.servers", "localhost:9092") \
          .option("subscribe", "creditPrediction") \
          .load().select(from_json(col("value").cast("string"),schema).alias("parsed_value")).select(col("parsed_value.*"))

print("All features of received data are given as follows: \n")
receivedStreamingData.printSchema()

num_features_without_label = ["id","loanAmnt", "term", "interestRate", "installment", "homeOwnership",
       "annualIncome", "verificationStatus", "dti", "delinquency_2years",
       "ficoRangeLow", "ficoRangeHigh", "openAcc", "pubRec", "revolBal", "totalAcc", "grade"]
# features_from_received_to_trainForm.append("grade")

features_from_received_to_trainForm = num_features_without_label
features_from_received_to_trainForm.insert(1,"isDefault")

oneUser = receivedStreamingData.select(*features_from_received_to_trainForm)
print("After features filtering, use the following features to predict the probability:\n")
oneUser.printSchema()

# Load the pipeline saved with spark batch
model = PipelineModel.load("./data/modelResults")
predictions = model.transform(oneUser)
query = predictions.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
