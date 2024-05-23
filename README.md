This repository makes it possible to start apache spark applications for making predictions on given data points. Applications can be registerd to Arrowhead service registry, in order for insights to be shared to external systems.

##  Requirements

* **Docker 24.0**, Other versions should also work.

Some applications also require Arrowhead framework, as those applications will notify external systems of gained insights. 

## Setup 

Create an `.env` file,

```
DOMAIN_ADDRESS=<address that will be registered in service registry>
DOMAIN_PORT=<port that will be registered in service registry>

DECISION_TREE_CERT_FILE_PATH=<path to cert .pem file, for the predict_mower_decision_tree_stream.py application> 
DECISION_TREE_KEY_FILE_PATH=<path to cert .pem file, for the predict_mower_decision_tree_stream.py application>

ERROR_CODES_CERT_FILE_PATH=<path to cert .pem file, for the predict_mower_decision_tree_stream.py application>
ERROR_CODES_KEY_FILE_PATH=<path to cert .pem file, for the predict_mower_decision_tree_stream.py application>
```

Add necessary certificates, that are valid for the Arrowhead cloud that you are connecting to.

## Start an application 

To start an analytics application, the model must first be trained and saved.
Navigate into the `apache-spark` directory and run, 

```
sh runApplication.sh applications/<train_script.py>
```

Choose the desired training application and enter it instead of `<train_script.py>`.

Start the prediction application by running, 

```
sh runApplication.sh applications/<prediction_script.py>
```
Choose the desired prediction application and enter it instead of `<prediction_script.py>`.

**NOTE:** If you are running the system on a Linux machine, it might be necessary to change the access for the stream directory. 
