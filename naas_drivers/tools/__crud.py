from naas_drivers.driver import ConnectDriver
import requests


class CRUD(ConnectDriver):
    req_headers = {"Accept": "application/json", "Content-Type": "application/json"}
    base_public_url = None
    endpoint = None
    key = None
    raise_error = False

    def __init__(
        self,
        base_url,
        endpoint,
        auth,
    ):
        self.base_public_url = base_url
        self.endpoint = endpoint
        self.key = auth

    def get(self, uid):
        self.check_connect()
        req = requests.get(
            url=f"{self.base_public_url}/{self.endpoint}/{uid}",
            headers=self.req_headers,
            auth=self.key,
            allow_redirects=False,
        )
        if self.raise_error:
            req.raise_for_status()
        return req

    def send(self, data):
        self.check_connect()
        req = requests.post(
            url=f"{self.base_public_url}/{self.endpoint}",
            auth=self.key,
            headers=self.req_headers,
            json=data,
            allow_redirects=False,
        )
        if self.raise_error:
            req.raise_for_status()
        return req

    def update(self, data):
        self.check_connect()
        _id = data.get("_id")
        req = requests.put(
            url=f"{self.base_public_url}/{self.endpoint}/{_id}",
            auth=self.key,
            headers=self.req_headers,
            json=data,
            allow_redirects=False,
        )
        if self.raise_error:
            req.raise_for_status()
        return req

    def delete(self, data):
        self.check_connect()
        _id = data.get("_id")
        req = requests.delete(
            url=f"{self.base_public_url}/{self.endpoint}/{_id}",
            auth=self.key,
            headers=self.req_headers,
            allow_redirects=False,
        )
        if self.raise_error:
            req.raise_for_status()
        return req
