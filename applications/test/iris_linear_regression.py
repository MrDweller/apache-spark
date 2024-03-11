from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import col
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SQLContext

spark = SparkSession.builder\
        .appName("Iris linear regression")\
        .master("spark://spark-master:7077")\
        .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
data = spark.read.options(inferSchema='True').csv("data/iris_train.data", header=True)
data.show(n=5)

string_indexer = StringIndexer(inputCol='label', outputCol='index').setHandleInvalid("keep")
model = string_indexer.fit(data)
data = model.transform(data).select('c0', 'c1', 'c2', 'c3', col('index').cast('int'), 'label')

# Create a feature vector
feature_cols = ['c0', 'c1', 'c2', 'c3']
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
data = assembler.transform(data)

data.printSchema()
data.show()   

# Split data into training and testing sets
train_data, test_data = data.randomSplit([0.8, 0.2], seed=123)

# Initialize the Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="index")

# Fit the model to the training data
model = lr.fit(train_data)

model.save("data/models/iris_linear_regression_model")

test_data.show()
# Make predictions on the test data
predictions = model.transform(test_data)

predictions.show()

evaluator = RegressionEvaluator(labelCol="index", predictionCol="prediction", metricName="rmse")

rmse = evaluator.evaluate(predictions)

print("Root Mean Squared Error:", rmse)

data = "4.6,3.4,1.4,0.3"
features = data.split(",")

# Convert each element in the list to a float
features = [float(x) for x in features]

cols = ['0', '1', '2', '3']

sqlContext = SQLContext(spark.sparkContext)
df = sqlContext.createDataFrame(
        [
                features
        ],
        cols  # add your column names here
)
assembler = VectorAssembler(inputCols=cols, outputCol="features")
df = assembler.transform(df)
df.show()

predictions = model.transform(df)

predictions.show()
