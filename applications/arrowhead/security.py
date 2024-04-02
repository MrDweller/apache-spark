from enum import Enum

class SecurityMode(Enum):
    CERTIFICATE = "CERTIFICATE"
    NOT_SECURE = "NOT_SECURE"

    @staticmethod
    def from_str(securityMode):
        match securityMode:
            case SecurityMode.CERTIFICATE.value:
                return SecurityMode.CERTIFICATE
            case SecurityMode.NOT_SECURE.value:
                return SecurityMode.NOT_SECURE
            case _:
                raise NotImplementedError