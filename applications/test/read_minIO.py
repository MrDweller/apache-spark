from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import Window, functions as F
spark = SparkSession.builder\
    .config("spark.jars", "./jars/hadoop-aws-3.3.6.jar,./jars/aws-java-sdk-bundle-1.12.367.jar") \
    .appName("MinioTest")\
    .getOrCreate()

spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
spark.conf.set("spark.hadoop.fs.s3a.access.key", "admin")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "password" )
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
df = spark.read.csv('s3a://test/iris.data',header=True)
df.show()