from requests.auth import HTTPBasicAuth
from escapism import escape
import requests
import string
import base64
import json
import os

_docker_safe_chars = set(string.ascii_letters + string.digits)
_docker_escape_char = "-"


def _escape(s):
    """Escape a string to docker-safe characters"""
    return escape(
        s,
        safe=_docker_safe_chars,
        escape_char=_docker_escape_char,
    )


class Me:
    base_public_url = None
    endpoint = "me"
    auth = None
    req_headers = {"Accept": "application/json", "Content-Type": "application/json"}

    def __init__(self, base_url, auth, endpoint="me"):
        self.base_public_url = base_url
        self.auth = auth
        self.endpoint = endpoint

    def get(self):
        req = requests.get(
            url=f"{self.base_public_url}/{self.endpoint}",
            headers=self.req_headers,
            auth=self.auth,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req

    def update(self, data):
        req = requests.put(
            url=f"{self.base_public_url}/{self.endpoint}",
            headers=self.req_headers,
            auth=self.auth,
            json=data,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req

    def help(self):
        print("=== Me === \n")
        print(".get() => get my user data\n")
        print(
            ".update(data) => update my user data (only few fields are allow if your are not admin)\n"
        )
        print(".delete(data) => delete one data by id (should be in the doc)\n")


class CRUD:
    base_public_url = None
    endpoint = None
    auth = None
    req_headers = {"Accept": "application/json", "Content-Type": "application/json"}

    def __init__(self, base_url, endpoint, auth):
        self.base_public_url = base_url
        self.endpoint = endpoint
        self.auth = auth

    def get_all(self, search=None, sort=None, limit=20, skip=0):
        params = {
            "limit": limit,
            "skip": skip,
        }
        if search:
            params["search"] = json.dumps(search)
        if sort:
            params["sort"] = json.dumps(sort)
        req = requests.get(
            url=f"{self.base_public_url}/{self.endpoint}",
            params=params,
            headers=self.req_headers,
            auth=self.auth,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req

    def get(self, id):
        req = requests.get(
            url=f"{self.base_public_url}/{self.endpoint}/{id}",
            headers=self.req_headers,
            auth=self.auth,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req

    def insert(self, data):
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
        id = data.get("_id")
        req = requests.put(
            url=f"{self.base_public_url}/{self.endpoint}/{id}",
            auth=self.auth,
            headers=self.req_headers,
            json=data,
            allow_redirects=False,
        )
        req.raise_for_status()

    def delete(self, data):
        id = data.get("_id")
        req = requests.delete(
            url=f"{self.base_public_url}/{self.endpoint}/{id}",
            auth=self.auth,
            headers=self.req_headers,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req

    def help(self):
        print("=== CRUD === \n")
        print(
            ".get_all(search=None, sort=None, limit=20, skip=0) => get all data in collection with optional filters\n"
        )
        print(".get(id) => get one data by id\n")
        print(".insert(data) => insert one data\n")
        print(".update(data) => update one data by id (should be in the doc)\n")
        print(".delete(data) => delete one data by id (should be in the doc)\n")


class SmartTable(CRUD):
    database = None
    collection = None

    def __init__(self, base_url, database, collection, auth):
        self.database = database
        self.collection = collection
        endpoint = f"smarttables/{database}/{collection}"
        CRUD.__init__(self, base_url, endpoint, auth)

    def allowed(self, database, collection):
        req = requests.post(
            url=f"{self.base_public_url}/{self.endpoint}/allowed",
            auth=self.auth,
            headers=self.req_headers,
            allow_redirects=False,
        )
        req.raise_for_status()
        req_json = req.json()
        return True if req_json.allowed else False

    def delete_all(self):
        req = requests.delete(
            url=f"{self.base_public_url}/{self.endpoint}/all",
            auth=self.auth,
            headers=self.req_headers,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req

    def help(self):
        print("=== Internals === \n")
        print(
            ".allowed(database, collection) => check if you are allowed to get data from this database and collection\n"
        )
        print(".delete_all() => delete all data in a collection\n")
        super().help()


class DarkKnight:
    """BOB lib"""

    base_public_url = None
    smart_tables = []
    __auth = None
    req_headers = {"Accept": "application/json", "Content-Type": "application/json"}

    def __init__(self, api_key=None, user=None, PUBLIC_DK_API=None):
        """
        Description: This class connect you to a darknight instance
        """
        self.user = user if user else os.environ.get("JUPYTERHUB_USER", user)
        self.base_public_url = (
            PUBLIC_DK_API
            if PUBLIC_DK_API
            else os.environ.get("PUBLIC_DK_API", PUBLIC_DK_API)
        )
        self.__auth = HTTPBasicAuth(self.user, api_key)
        self.users = CRUD(self.base_public_url, "users", self.__auth)
        self.workspaces = CRUD(self.base_public_url, "workspaces", self.__auth)
        self.me = Me(self.base_public_url, self.__auth)

    def get_notebook_public_url(self, token=""):

        client = self.user
        clientEncoded = _escape(client)
        message_bytes = clientEncoded.encode("ascii")
        base64_bytes = base64.b64encode(message_bytes)
        username_base64 = base64_bytes.decode("ascii")
        return f"{self.base_public_url}/notebook/{username_base64}/{token}"

    def init_smarttable(self, database, collection):
        return SmartTable(self.base_public_url, database, collection, self.__auth)

    def notification(self, email, subject, content, image_url, link_url):
        data = {
            "email": email,
            "object": subject,
            "content": content,
            "image_url": image_url,
            "link_url": link_url,
        }
        req = requests.post(
            url=f"{self.base_public_url}/notifications/send",
            auth=self.__auth,
            headers=self.req_headers,
            json=data,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req

    def help(self):
        print("=== DarkKnight === \n")
        print(
            ".init_smarttable(database, collection) => initialise smart table with database and collection\n"
        )
        print(
            ".init_smarttable(None, None).help() => show the helper of init_smarttable class\n"
        )
        print(".users.help() => show the helper of user class\n")
        print(".workspaces.help() => show the helper of workspaces class\n")
        print(".me.help() => show the helper of me class\n")
