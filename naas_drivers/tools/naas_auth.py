from naas_drivers.driver import InDriver, OutDriver
import requests as r
import os

AUTH_API_FQDN = "auth.naas.ai"
AUTH_API_PROTOCOL = "https"


class NaasAuth(InDriver, OutDriver):
    __access_token = None
    __headers = None

    def __init__(self):
        self.user = self.Users(self)
        self.bearer = self.Bearer(self)

    @property
    def access_token(self):
        return self.__access_token

    @property
    def headers(self):
        return self.__headers

    def connect(self, token=None):
        if not token:
            token = os.environ.get("JUPYTERHUB_API_TOKEN")
        res = r.get(
            f"{AUTH_API_PROTOCOL}://{AUTH_API_FQDN}/bearer/jupyterhubtoken",
            params={"token": token},
        )
        res.raise_for_status()
        if res.status_code == 200:
            self.__access_token = res.json().get("access_token")
            self.__headers = {"Authorization": f"Bearer {self.__access_token}"}
        return self

    class __InnerBase:
        """__InnerBase class"""

        def __init__(self, parent):
            self.__parent = parent

        @property
        def headers(self):
            return self.__parent.headers

    class Users(__InnerBase):
        """Users inner class"""

        def me(self):
            res = r.get(
                f"{AUTH_API_PROTOCOL}://{AUTH_API_FQDN}/users/me/", headers=self.headers
            )
            res.raise_for_status()
            if res.status_code == 200:
                return res.json()

    class Bearer(__InnerBase):
        """Bearer inner class"""

        def validate(self):
            res = r.get(
                f"{AUTH_API_PROTOCOL}://{AUTH_API_FQDN}/bearer/validate",
                headers=self.headers,
            )
            res.raise_for_status()
            if res.status_code == 200:
                return res.json()
