from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import col
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder\
        .appName("Mower linear regression")\
        .master("spark://spark-master:7077")\
        .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.options(inferSchema='True').csv("data/mower/mower-900.csv", header=True)

df.show()

string_indexer = StringIndexer(inputCol='state', outputCol='index').setHandleInvalid("keep")
model = string_indexer.fit(df)
df = model.transform(df).select('speed', 'vibration', col('index').cast('int'), 'state')
df.show()

# # Create a feature vector
feature_cols = ['speed', 'vibration']
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df = assembler.transform(df)

df.printSchema()
df.show()   

# # Split data into training and testing sets
train_data, test_data = df.randomSplit([0.8, 0.2], seed=123)

# # Initialize the Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="index")

# # Fit the model to the training data
model_lr = lr.fit(train_data)

# model_lr.save("data/models/iris_linear_regression_model")

# Make predictions on the test data
predictions_lr = model_lr.transform(test_data)

predictions_lr.show()

evaluator = RegressionEvaluator(labelCol="index", predictionCol="prediction", metricName="rmse")

rmse = evaluator.evaluate(predictions_lr)

print("Root Mean Squared Error:", rmse)