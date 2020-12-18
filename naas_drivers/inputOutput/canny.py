from naas_drivers.driver import InDriver, OutDriver
from .__crud import CRUD
import requests
import os


class Users(CRUD):
    def get_by_email(self, email):
        data = {
            "apiKey": self.auth,
            "email": email,
        }
        req = requests.post(
            url=f"{self.base_public_url}/{self.endpoint}/retrieve",
            headers=self.req_headers,
            json=data,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req

    def get(self, uid):
        data = {
            "apiKey": self.auth,
            "id": uid,
        }
        req = requests.post(
            url=f"{self.base_public_url}/{self.endpoint}/retrieve",
            headers=self.req_headers,
            json=data,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req

    def send(self, email, name=None):
        data = {
            "apiKey": self.auth,
            "email": email,
        }
        if name:
            data["name"] = name
        req = requests.post(
            url=f"{self.base_public_url}/{self.endpoint}/find_or_create",
            headers=self.req_headers,
            json=data,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req

    def delete(self, uid):
        data = {
            "apiKey": self.auth,
            "id": uid,
        }
        req = requests.post(
            url=f"{self.base_public_url}/{self.endpoint}/delete",
            headers=self.req_headers,
            json=data,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req


class Canny(InDriver, OutDriver):

    base_url = os.environ.get("CANNY_URL", "https://canny.io")
    api_url = None

    def __init__(self):
        self.api_url = f"{self.base_url}/api/v1"

    def connect(self, api_token):
        self.key = api_token
        self.connected = True
        self.users = Users(self.api_url, "users", self.key)
        return self
