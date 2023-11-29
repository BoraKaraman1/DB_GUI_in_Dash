# the file where the access tokens are preserved for security reasons

class DBXEnvironment:
    def __init__(self, env: str):
        self.host = self._get_host(env=env)
        self.token = self._get_token(env=env)

    def _get_host(self, env: str):
        if env == "dev":
            return #host adress of yours
        else:
            return ""

    def _get_token(self, env: str):
        if env == "dev":
            return # access token of yours
        else:
            return ""