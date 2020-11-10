from naas_drivers.driver import InDriver, OutDriver
from .__crud import CRUD
import requests
import os


class TKCRUD(CRUD):
    def __init__(self, base_url, endpoint, auth):
        self.req_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {auth}",
        }
        CRUD.__init__(self, base_url, endpoint, auth)

    def get_all(self):
        req = requests.get(
            url=f"{self.base_public_url}/{self.endpoint}",
            headers=self.req_headers,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req

    def get(self, uid):
        req = requests.get(
            url=f"{self.base_public_url}/{self.endpoint}/{uid}",
            headers=self.req_headers,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req

    def send(self, data):
        req = requests.post(
            url=f"{self.base_public_url}/{self.endpoint}",
            headers=self.req_headers,
            json=data,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req

    def delete(self, uid):
        req = requests.delete(
            url=f"{self.base_public_url}/{self.endpoint}/{uid}",
            headers=self.req_headers,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req


class Thinkific(InDriver, OutDriver):

    base_url = os.environ.get(
        "THINKIFIC_URL", "https://cashstory-education.thinkific.com/"
    )
    api_url = None
    token = None
    users = None
    enrollments = None

    def __init__(self):
        self.api_url = f"{self.base_url}api/private/v1"

    def connect(self, api_token):
        self.token = api_token
        self.connected = True
        self.users = TKCRUD(self.api_url, "users", self.token)
        self.enrollments = TKCRUD(self.api_url, "enrollments", self.token)
        return self
