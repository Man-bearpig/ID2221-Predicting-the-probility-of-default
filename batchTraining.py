import pandas as pd
# from sklearn.model_selection import train_test_split
# from sklearn.metrics import accuracy_score, f1_score, roc_auc_score, log_loss
# import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
# from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler,Imputer,StringIndexer, OneHotEncoder
# from pyspark.sql.functions import col
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler
# from pyspark.sql.functions import split,from_json,col
# from pyspark.sql.types import StringType,DoubleType,LongType
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, TimestampType


import warnings
warnings.filterwarnings('ignore')

# read the data as pandas
whole_data_pandas = pd.read_csv('data/batchTrainingData.csv')
data_pandas = whole_data_pandas[["id","loanAmnt", "term", "interestRate", "installment", "homeOwnership",
       "annualIncome", "verificationStatus", "dti", "delinquency_2years",
       "ficoRangeLow", "ficoRangeHigh", "openAcc", "pubRec", "revolBal", "totalAcc","isDefault", "grade"]]
data_pandas = data_pandas.rename(columns={"isDefault":"label"})

num_features = list(data_pandas.select_dtypes(exclude="object").columns)
cat_features = list(filter(lambda x: x not in num_features, list(data_pandas.columns)))
print(f"num_features is {num_features}, \n \n",
      f"cat_features is {cat_features}")

######### spark settings:
spark = SparkSession.builder.getOrCreate()
data_train_spark = spark.createDataFrame(data_pandas)
spark.sparkContext.setLogLevel('WARN')

########### training the model:
## with a pipeline model:

# filling non
imputer = Imputer(inputCols=["dti"], outputCols=["dti"])

# String to Index to OneHot
indexer = StringIndexer(inputCol="grade", outputCol="gradeIndex")
onehoter = OneHotEncoder(inputCols=["gradeIndex"],outputCols=["gradeIndexVect"])

# aggregate all number features into a vector to scale
num_features_without_label = [col for col in num_features if col != "label"]
num_features_without_label_and_id = [col for col in num_features_without_label if col!="id"]

num_features_assembler = VectorAssembler(inputCols=num_features_without_label_and_id, outputCol="num_features_vec")
scaler = StandardScaler(inputCol="num_features_vec", outputCol="scaled_num_features_vec")

# aggregate scaled continuous and categorial features into a vector to train.
scaled_cate_assembler = VectorAssembler(inputCols=["scaled_num_features_vec","gradeIndexVect"],outputCol="features")

# LR model
lr = LogisticRegression(maxIter=100,regParam=0.3)
# lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

pipeline = Pipeline(stages=[imputer, indexer, onehoter, num_features_assembler, scaler, scaled_cate_assembler,lr])
model = pipeline.fit(data_train_spark)
predictions = model.transform(data_train_spark)

# ### Print the coefficients and intercept for logistic regression
lrModel = model.stages[-1]
# print("Coefficients: " + str(lrModel.coefficients))
# print("Intercept: " + str(lrModel.intercept))
trainingSummary = lrModel.summary
# Obtain the objective per iteration
# # objectiveHistory = trainingSummary.objectiveHistory
# print("objectiveHistory:")
# for objective in objectiveHistory:
#     print(objective)

######### Show trainging Results:
print("The model training on the batch data has been completed and the following is some metrics:\n")

### Get the metrics of the model:
acc = predictions.filter(predictions.label == predictions.prediction).count() / float(data_train_spark.count())
print("Accuracy of the model: %.3f"%acc)

# Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
trainingSummary.roc.show()
print("areaUnderROC: " + str(trainingSummary.areaUnderROC))

# Save the model so that streaming process can use it to preprocess and predict.
model.write().overwrite().save("./data/modelResults")
