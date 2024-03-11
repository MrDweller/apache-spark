from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import col
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import DecisionTreeClassifier

spark = SparkSession.builder\
        .appName("Mower decision tree")\
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

dt = DecisionTreeClassifier(labelCol="index", featuresCol="features")

model_dt = dt.fit(train_data)

model_dt.save("data/models/mower_decision_tree")

predictions_dt = model_dt.transform(test_data)

predictions_dt.where(predictions_dt.index==0).show(5)
predictions_dt.where(predictions_dt.index==1).show(5)
predictions_dt.where(predictions_dt.index==2).show(5)
predictions_dt.where(predictions_dt.index==3).show(5)


evaluator=MulticlassClassificationEvaluator(labelCol="index", predictionCol="prediction", metricName="accuracy")
acc = evaluator.evaluate(predictions_dt)
print("Prediction Accuracy: ", acc)