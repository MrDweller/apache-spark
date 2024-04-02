import requests
import os
import arrowhead.security as security

class ServiceRegistryConfig:
    def __init__(self, serviceregistry_address: str, serviceregistry_port: int, serviceregistry_security_mode: security.SecurityMode) -> None:
        self.serviceregistry_address = serviceregistry_address
        self.serviceregistry_port = serviceregistry_port
        self.serviceregistry_security_mode = serviceregistry_security_mode



def register_system(address, port, system_name, serviceregistry_config: ServiceRegistryConfig, authentication_info="", metadata={}, cert=None):
    request_body = {
        "address": address,
        "port": port,
        "systemName": system_name,
        "authenticationInfo": authentication_info,
        "metadata": metadata
    }

    sr_address = serviceregistry_config.serviceregistry_address
    sr_port = serviceregistry_config.serviceregistry_port


    match serviceregistry_config.serviceregistry_security_mode:
        case security.SecurityMode.CERTIFICATE:
            response = requests.post(f"https://{sr_address}:{sr_port}/serviceregistry/register-system", cert=cert, verify=False, json=request_body)
        case security.SecurityMode.NOT_SECURE:
            response = requests.post(f"http://{sr_address}:{sr_port}/serviceregistry/register-system", json=request_body)
        case _:
            raise NotImplementedError(f"a consumer for {serviceregistry_config.serviceregistry_security_mode} level security is not implemented")

    return response.json()

def query(service_definition_requirement, serviceregistry_config: ServiceRegistryConfig, cert=None):
    request_body = {
        "interfaceRequirements": [
            "HTTP-SECURE-JSON"
        ],
        "serviceDefinitionRequirement": service_definition_requirement,
    }

    sr_address = serviceregistry_config.serviceregistry_address
    sr_port = serviceregistry_config.serviceregistry_port

    match serviceregistry_config.serviceregistry_security_mode:
        case security.SecurityMode.CERTIFICATE:
            response = requests.post(f"https://{sr_address}:{sr_port}/serviceregistry/query", cert=cert, verify=False, json=request_body)
        case security.SecurityMode.NOT_SECURE:
            response = requests.post(f"http://{sr_address}:{sr_port}/serviceregistry/query", json=request_body)
        case _:
            raise NotImplementedError(f"a consumer for {serviceregistry_config.serviceregistry_security_mode} level security is not implemented")
        

    return response.json()