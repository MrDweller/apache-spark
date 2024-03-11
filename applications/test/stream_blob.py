
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import VectorAssembler
import pandas as pd 


if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("Stream csv data")\
        .master("spark://spark-master:7077")\
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

                                  
    streamingDF = spark.readStream.schema('c0 FLOAT, c1 FLOAT, c2 FLOAT, c3 FLOAT').csv("data/stream")
    model = LinearRegressionModel.load("data/models/iris_linear_regression_model")
    
    streamingDF.printSchema()

    assembler = VectorAssembler(inputCols=["c0", "c1", "c2", "c3"], outputCol="features")
    data = assembler.transform(streamingDF)

    predictionsDF = model.transform(data)

    predictedLabel = predictionsDF.select("prediction")  # Adjust based on your model output

    # Apply additional transformations or filter if needed
    finalDF = predictedLabel.withColumnRenamed("prediction", "predicted_label")

    # Start the streaming query
    query = finalDF \
        .writeStream \
        .format("console") \
        .option("truncate", False) \
        .start()

    query.awaitTermination()