import requests
import os
import arrowhead.serviceregistry

def orchestration(requested_service_definition, requester_system_address, requester_system_port, requester_system_name, requester_system_authentication_info="", cert=None):
    query_result = arrowhead.serviceregistry.query("orchestration-service", cert=cert)

    print(query_result)

    orchestrator = query_result["serviceQueryData"][0]["provider"]
    orchestrator_address = orchestrator["address"]
    orchestrator_port = orchestrator["port"]

    request_body = {
        "requestedService": {
            "interfaceRequirements": [
                "HTTP-SECURE-JSON"
            ],
            "serviceDefinitionRequirement": requested_service_definition
        },
        "requesterSystem": {
            "address": requester_system_address,
            "authenticationInfo": requester_system_authentication_info,
            "port": requester_system_port,
            "systemName": requester_system_name
        }
    }
    if (os.environ["SERVER_MODE"] == "secure"):
        response = requests.post(f"https://{orchestrator_address}:{orchestrator_port}/orchestrator/orchestration", cert=cert, verify=False, json=request_body)
    else:
        response = requests.post(f"http://{orchestrator_address}:{orchestrator_port}/orchestrator/orchestration", json=request_body)

    return response.json()