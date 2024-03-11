from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder\
        .appName("Show iris")\
        .master("spark://spark-master:7077")\
        .getOrCreate()

data = spark.read.csv("data/iris.data", header=True)
data.show(n=5)