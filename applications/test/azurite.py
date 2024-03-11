from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("Read azurite")\
    .config("spark.jars", "applications/jars/hadoop-azure-3.2.0.jar,applications/jars/azure-storage-7.0.0.jar,applications/jars/jetty-util-ajax-11.0.8.jar") \
    .getOrCreate()

df = spark.read.csv("wasb://mycontainer@storageemulator/iris.data")
df.show()