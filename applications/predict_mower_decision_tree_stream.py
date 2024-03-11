from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.ml.feature import VectorAssembler


if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("Predict mower stream data")\
        .master("spark://spark-master:7077")\
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

                                  
    streamingDF = spark.readStream\
        .schema('id STRING, timestamp STRING, speed FLOAT, vibration FLOAT, `pos-x` INT, `pos-y` INT, state STRING')\
        .format("csv")\
        .option("header", "true")\
        .load("data/stream/mower")
    
    model = DecisionTreeClassificationModel.load("data/models/mower_decision_tree")
    
    streamingDF.printSchema()

    assembler = VectorAssembler(inputCols=['speed', 'vibration'], outputCol="features")
    data = assembler.transform(streamingDF)

    finalDF = model.transform(data)

    # Start the streaming query
    query = finalDF \
        .writeStream \
        .format("console") \
        .option("truncate", False) \
        .start()

    query.awaitTermination()