from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.ml.feature import StringIndexerModel
from pyspark.ml.feature import IndexToString

from pyspark.ml.feature import VectorAssembler
import requests
import os

import arrowhead.serviceregistry
import arrowhead.orchestrator


CERT_FILE_PATH = os.environ["ERROR_CODES_CERT_FILE_PATH"]
KEY_FILE_PATH = os.environ["ERROR_CODES_KEY_FILE_PATH"]

ADDRESS=os.environ["DOMAIN_ADDRESS"]
PORT=os.environ["DOMAIN_PORT"]
SYSTEM_NAME="anomaly-detector-error-codes"
SERVICE_REGISTRY_ADDRESS = os.environ["SERVICE_REGISTRY_ADDRESS"]
SERVICE_REGISTRY_PORT = serviceregistry_port=os.environ["SERVICE_REGISTRY_PORT"]
SERVICE_REGISTRY_SECURITY_MODE = serviceregistry_security_mode=os.environ["SERVICE_REGISTRY_SECURITY_MODE"]

SERVICE_REGISTRY_CONFIG = serviceregistry_config=arrowhead.serviceregistry.ServiceRegistryConfig(
    serviceregistry_address=SERVICE_REGISTRY_ADDRESS,
    serviceregistry_port=SERVICE_REGISTRY_PORT,
    serviceregistry_security_mode=arrowhead.serviceregistry.security.SecurityMode.from_str(SERVICE_REGISTRY_SECURITY_MODE),
)

def handle_stream_prediction(df, epoch_id):
    df.show()

    list_of_predictions = [row.asDict() for row in df.collect()]

    for prediction in list_of_predictions:
        if prediction["state"] != "normal":
            state = prediction["state"] 
            id = prediction["id"]
            time = prediction["timestamp"]
            print(f"found error state: {state}, for {id} at time {time}")

            orchestration_response = arrowhead.orchestrator.orchestration(requested_service_definition=state, requester_system_address=ADDRESS, requester_system_port=PORT, requester_system_name=SYSTEM_NAME, serviceregistry_config=SERVICE_REGISTRY_CONFIG, cert=(CERT_FILE_PATH, KEY_FILE_PATH))

            print("\n", orchestration_response)

            for response in orchestration_response["response"]:
                if (response["provider"]["systemName"] == id):
                    address = response["provider"]["address"]
                    port = response["provider"]["port"]
                    uri = response["serviceUri"]

                    requests.post(f"https://{address}:{port}{uri}", cert=(CERT_FILE_PATH, KEY_FILE_PATH), verify=False)

def predict(df, epoch_id):
    df.show()

    mapping = spark.read.format("parquet").load('data/mappings/error_codes')
    dict = mapping.toPandas().to_dict()

    states = []
    for row in df.collect():
            states.append(dict[row['MowerApp_Error_errorCode']][0])

    print(states)

    pdf = df.toPandas()
    pdf.insert(5, "state", states)

    df = spark.createDataFrame(pdf)

    df.show()

    handle_stream_prediction(df=df, epoch_id=epoch_id)

if __name__ == "__main__":

    # Register this anomaly detector in the arrowhead service registry
    registration_response = arrowhead.serviceregistry.register_system(
        address=ADDRESS, 
        port=PORT, 
        system_name=SYSTEM_NAME, 
        serviceregistry_config=SERVICE_REGISTRY_CONFIG, 
        cert=(CERT_FILE_PATH, KEY_FILE_PATH)
    )
    print(F"\n {registration_response}")

    stream_directory =  "data/stream/mower" # stream data from a local directory 
    if not os.path.exists(stream_directory):
        os.makedirs(stream_directory)

    spark = SparkSession.builder\
        .appName("Predict mower error codes")\
        .master("spark://spark-master:7077")\
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

                                  
    streamingDF = spark.readStream\
        .schema('id STRING, timestamp STRING, `MowerApp_Error_errorCode` STRING, `pos-x` INT, `pos-y` INT')\
        .format("csv")\
        .option("header", "true")\
        .load(stream_directory)
    
    
    streamingDF.printSchema()

    # Start the streaming query
    query = streamingDF \
        .writeStream \
        .format("console") \
        .foreachBatch(predict) \
        .option("truncate", False) \
        .start()

    query.awaitTermination()