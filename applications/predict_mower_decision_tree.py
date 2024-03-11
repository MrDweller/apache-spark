import sys
from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.ml.feature import VectorAssembler
import pandas as pd


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print("Usage: predict_mower_decision_tree.py <blob_path>")
        exit(1)

    blob_path = sys.argv[1]
    
    spark = SparkSession.builder\
        .appName("Predict mower data")\
        .master("spark://spark-master:7077")\
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    pandas_df = pd.read_csv(
        f"abfs://{blob_path}",
        storage_options={
            "connection_string": "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://172.26.160.1:10000/devstoreaccount1;QueueEndpoint=http://172.26.160.1:10001/devstoreaccount1;TableEndpoint=http://172.26.160.1:10002/devstoreaccount1;"
    })
    df = spark.createDataFrame(pandas_df)
    
    model = DecisionTreeClassificationModel.load("data/models/mower_decision_tree")
    
    df.printSchema()

    assembler = VectorAssembler(inputCols=['speed', 'vibration'], outputCol="features")
    data = assembler.transform(df)

    finalDF = model.transform(data)

    finalDF.show()
