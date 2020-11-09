import requests


class CRUD:
    req_headers = {"Accept": "application/json", "Content-Type": "application/json"}
    base_public_url = None
    endpoint = None
    auth = None

    def __init__(
        self,
        base_url,
        endpoint,
        auth,
    ):
        self.base_public_url = base_url
        self.endpoint = endpoint
        self.auth = auth

    def get(self, uid):
        req = requests.get(
            url=f"{self.base_public_url}/{self.endpoint}/{uid}",
            headers=self.req_headers,
            auth=self.auth,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req

    def send(self, data):
        req = requests.post(
            url=f"{self.base_public_url}/{self.endpoint}",
            auth=self.auth,
            headers=self.req_headers,
            json=data,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req

    def update(self, data):
        _id = data.get("_id")
        req = requests.put(
            url=f"{self.base_public_url}/{self.endpoint}/{_id}",
            auth=self.auth,
            headers=self.req_headers,
            json=data,
            allow_redirects=False,
        )
        req.raise_for_status()

    def delete(self, data):
        _id = data.get("_id")
        req = requests.delete(
            url=f"{self.base_public_url}/{self.endpoint}/{_id}",
            auth=self.auth,
            headers=self.req_headers,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req
