import requests
import os

def register_system(address, port, system_name, authentication_info="", metadata={}, cert=None):
    request_body = {
        "address": address,
        "port": port,
        "systemName": system_name,
        "authenticationInfo": authentication_info,
        "metadata": metadata
    }

    sr_address = os.environ["SERVICE_REGISTRY_ADDRESS"]
    sr_port = os.environ["SERVICE_REGISTRY_PORT"]

    if (os.environ["SERVER_MODE"] == "secure"):
        response = requests.post(f"https://{sr_address}:{sr_port}/serviceregistry/register-system", cert=cert, verify=False, json=request_body)
    else:
        response = requests.post(f"http://{sr_address}:{sr_port}/serviceregistry/register-system", json=request_body)

    return response.json()

def query(service_definition_requirement, cert=None):
    request_body = {
        "interfaceRequirements": [
            "HTTP-SECURE-JSON"
        ],
        "serviceDefinitionRequirement": service_definition_requirement,
    }

    sr_address = os.environ["SERVICE_REGISTRY_ADDRESS"]
    sr_port = os.environ["SERVICE_REGISTRY_PORT"]

    if (os.environ["SERVER_MODE"] == "secure"):
        response = requests.post(f"https://{sr_address}:{sr_port}/serviceregistry/query", cert=cert, verify=False, json=request_body)
    else:
        response = requests.post(f"http://{sr_address}:{sr_port}/serviceregistry/query", json=request_body)

    return response.json()