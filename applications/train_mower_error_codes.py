from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder\
        .appName("Mower error codes")\
        .master("spark://spark-master:7077")\
        .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

input_df = spark.read.options(inferSchema='True').csv("data/mower/error_codes/train.csv", header=True)

input_df.show()
dict = {row['MowerApp_Error_errorCode']:row['state'] for row in input_df.collect()}
print(dict)
pdf = pd.DataFrame(dict, index=[0])
df = spark.createDataFrame(pdf)
df.show()
df.write.format("parquet").mode("overwrite").save('data/mappings/error_codes')

mapping = spark.read.format("parquet").load('data/mappings/error_codes')
dict = mapping.toPandas().to_dict()

    
test_df = input_df.select('id','timestamp','MowerApp_Error_errorCode','pos-x','pos-y')
test_df.show()

states = []
for row in test_df.collect():
        states.append(dict[row['MowerApp_Error_errorCode']][0])

print(states)
pdf = test_df.toPandas()
pdf.insert(5, "state", states)

print(pdf)
df = spark.createDataFrame(pdf)
df.show()