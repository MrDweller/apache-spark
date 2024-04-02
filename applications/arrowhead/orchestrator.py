import requests
import os
import arrowhead.serviceregistry
import arrowhead.security

def orchestration(requested_service_definition, requester_system_address, requester_system_port, requester_system_name, serviceregistry_config: arrowhead.serviceregistry.ServiceRegistryConfig, requester_system_authentication_info="", cert=None):
    query_result = arrowhead.serviceregistry.query("orchestration-service", serviceregistry_config=serviceregistry_config, cert=cert)

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
    security = arrowhead.security.SecurityMode.from_str(query_result["serviceQueryData"][0]["secure"])
    match security:
        case arrowhead.security.SecurityMode.CERTIFICATE:
            response = requests.post(f"https://{orchestrator_address}:{orchestrator_port}/orchestrator/orchestration", cert=cert, verify=False, json=request_body)
        case arrowhead.security.SecurityMode.NOT_SECURE:
            response = requests.post(f"http://{orchestrator_address}:{orchestrator_port}/orchestrator/orchestration", json=request_body)
        case _:
            raise NotImplementedError(f"a consumer for {security} level security is not implemented")
        
    return response.json()