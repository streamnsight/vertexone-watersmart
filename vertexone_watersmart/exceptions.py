class UnauthorizedException(Exception):
    def __init__(self):
        super().__init__("Unauthorized")


class NotAuthenticatedException(Exception):
    def __init__(self):
        super().__init__("Not authenticated")


class ServerErrorException(Exception):
    def __init__(self):
        super().__init__("Server error")


class UnknownException(Exception):
    def __init__(self, message):
        super().__init__(message)


class UnknownProviderException(Exception):
    def __init__(self, message):
        super().__init__(f"Unknown provider: {message}")
