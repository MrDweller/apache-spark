from pyspark.sql import SparkSession
import pandas as pd

# account_name = 'devstoreaccount1'
# account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
# container_name = 'mycontainer'

        # .config(f"spark.hadoop.fs.azure.account.key.{account_name}.blob.core.windows.net", account_key) \
spark = SparkSession.builder\
        .appName("Read blob")\
        .getOrCreate()

# spark.conf.set(
#     f"fs.azure.account.key.{account_name}.blob.core.windows.net", f"{account_key}"
# )
sc = spark.sparkContext
sc.setLogLevel("WARN")

df=spark.read.csv(f"wasb://mycontainer@storageemulator/iris.data")
df.show()

# Configure Spark for WASB access
# spark.conf.set("fs.azure.account.key.devstoreaccount1.blob.core.windows.net", 
#     "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
# spark.conf.set("fs.azure.account.name", "devstoreaccount1")
# spark.conf.set("fs.azure.use.https", "true")

# Sample path to your data file
# data_path = "wasb://mycontainer@devstoreaccount1.blob.core.windows.net/iris.data"

# Read data from WASB
# df = spark.read.csv(f"wasbs://{container_name}@{account_name}.blob.core.windows.net/iris.data")
# df.show()

# spark_df = spark.read.format('csv').\
#     option('header', True).\
#     load(f"wasbs://{container_name}@{account_name}.blob.core.windows.net/iris.data")
# print(spark_df.show())

# blob_path = "mycontainer/iris.data"
# df = pd.read_csv(
#         f"abfs://{blob_path}",
#         storage_options={
#             "connection_string": "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://172.26.160.1:10000/devstoreaccount1;QueueEndpoint=http://172.26.160.1:10001/devstoreaccount1;TableEndpoint=http://172.26.160.1:10002/devstoreaccount1;"
#     })
# print(df)