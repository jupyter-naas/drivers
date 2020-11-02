from naas_drivers.driver import In_Driver, Out_Driver
from requests.auth import HTTPBasicAuth
import requests
import json
import re
import os


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


class CRUD:
    base_public_url = None
    endpoint = None
    auth = None
    req_headers = {"Accept": "application/json", "Content-Type": "application/json"}

    def __init__(
        self,
        base_url,
        endpoint,
        auth,
    ):
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


class Bobapp(In_Driver, Out_Driver):

    base_public_url = None
    smart_tables = []
    __auth = None
    req_headers = {"Accept": "application/json", "Content-Type": "application/json"}
    users = None
    workspaces = None
    me = None

    def connect(self, api_key=None, user=None, PUBLIC_DK_API=None):
        """
        Description: This class connect you to a Bobapp instance
        """
        self.user = user if user else os.environ.get("JUPYTERHUB_USER", user)
        self.base_public_url = (
            PUBLIC_DK_API
            if PUBLIC_DK_API
            else os.environ.get("PUBLIC_DK_API", PUBLIC_DK_API)
        )
        if api_key:
            self.__auth = HTTPBasicAuth(self.user, api_key)
        self.users = CRUD(self.base_public_url, "users", self.__auth)
        self.workspaces = CRUD(self.base_public_url, "workspaces", self.__auth)
        self.me = Me(self.base_public_url, self.__auth)
        self.connected = True
        return self

    def connect_smarttable(self, database, collection):
        self.check_connect()
        return SmartTable(self.base_public_url, database, collection, self.__auth)

    def validate_email(self, email):
        regex_email = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"
        return not re.match(regex_email, email)

    def get_user(self, email):
        # Get user
        search = {"email": email}
        req = self.users.get_all(search=search, limit=1)
        users = req.json()
        if len(users) == 0:
            return None
        return users[0]

    def update_user(
        self,
        email,
        password,
        first_name,
        last_name,
        role="user",
        phone_number=None,
        user_role=None,
    ):

        # Init variable
        check_role = False
        check_user = False

        # -> email
        # Delete space: email must have no space at the begining or end
        email = email.strip().lower()

        # -> role
        if role not in ["user", "admin"]:
            check_role = False
            print(f"Role {role} not recognized in Bobapp")
        else:
            check_role = True

        # Create user in Bobapp
        if check_role and self.validate_email(email):
            check_user = True
            # Init user info
            new_user = {
                "email": email,
                "password": password,
                "firstName": first_name,
                "lastName": last_name,
                "role": role,
                "phoneNumber": phone_number,
                "userRole": user_role,
            }

            # Get user in Bobapp
            users = self.get_user(email)

            # If user does not exist => create user
            if len(users) == 0:
                user = new_user
                self.users.insert(user)
                print(f"User {email} created in Bobapp, password: {password}.")
            else:
                # user = users[0]
                new_user["_id"] = users[0]["_id"]
                self.users.update(new_user)
                print(f"User {email} updated in Bobapp, password: {password}.")
        return check_user

    def update_user_workspace(
        self, email, workspace_id=None, workspace_name=None, default=False
    ):
        # -> email
        # Delete space: email must have no space at the begining or end
        email = email.strip().lower()

        if not self.validate_email(email):
            raise ValueError(f"User {email} not valid")

        user = self.get_user(email)
        if not user:
            raise ValueError(f"User {email} does not exist in Bobapp")

        # Init workspace info
        workspace_param = {
            "news": {
                "sources": [],
                "categories": [],
                "name": "News",
                "class": "",
                "lang": "en",
            },
            "favorites": {"boxes": [], "name": "Favoris"},
            "name": workspace_name,
        }

        # Create workspace section
        if not user.get("workspaces"):
            user["workspaces"] = {}
        # Update workspace
        if workspace_id is not None and not user["workspaces"].get(workspace_id, None):
            user["workspaces"] = {}
            user["workspaces"][workspace_id] = workspace_param
            self.users.update(user)
            print(
                f"Workspace {workspace_id} : '{workspace_name}' added to user {email}"
            )
        elif user["workspaces"].get(workspace_id, None):
            print(f"Workspace already exists for user {email}")

        # Set workspace default
        if default:
            user["workspaceCurrent"] = {
                "section": 2,
                "id": workspace_id,
            }
            self.users.update(user)

    def update_user_service(self, serv, email, password):

        # Check service
        serv_list = ["ftp", "jupyter", "wekan"]
        if serv not in serv_list:
            raise ValueError(
                f"Service {serv} does not exist. Please change it to {serv_list}."
            )

        # -> email
        email = email.strip().lower()
        if not self.validate_email(email):
            raise ValueError(f"User {email} not valid")

        user = self.get_user(email)
        if not user:
            raise ValueError(f"User {email} does not exist in Bobapp")
        # Init service
        service = {
            "authMethod": serv,
            "login": {
                "username": email,
                "password": password,
            },
        }

        # Create service section
        if not user.get("services"):
            user["services"] = {}
        if not user["services"].get(serv, None):
            user["services"][serv] = service
            self.users.update(user)
            print(f"Service {serv} Added to user", email)
        else:
            print(f"Service already exists in Bobapp for {email}")

    def create_user_service(self, serv, email, password):

        # Create login
        email = email.strip().lower()
        if not self.validate_email(email):
            raise ValueError(f"User {email} not valid")

        login = {
            "username": email,
            "password": password,
        }

        # Init services
        serv_list = ["ftp", "jupyter"]
        if serv not in serv_list:
            raise ValueError(
                f"Service {serv} does not exist. Please change it to {serv_list}."
            )

        if serv == "ftp":
            # Variable FTP
            AUTH_TOKEN_FTP = (
                "8b262ed2e3d8aae269c421babed50c48141e110ddc16ee3782929f4f46a8"
            )
            URI_FTP = "https://ftp.naas.ai/user/add"

            # Create user on service ftp
            headers = {"X-Api-Key": AUTH_TOKEN_FTP}
            requests.post(URI_FTP, json=login, headers=headers)

        if serv == "jupyter":
            # Jupyter
            AUTH_TOKEN_JUP = (
                "xQqs3X7zEkAhsQmnoeQAepoB4irB"  # admin of your galaxy to get it
            )
            URI_JUP = "https://app.naas.ai/hub/signup"

            # Create user on service jupyter
            headers = {"Authorization": AUTH_TOKEN_JUP}
            requests.post(URI_JUP, data=login, headers=headers).json()

    def create_user(
        self,
        email,
        password,
        first_name,
        last_name,
        role,
        workspace_id,
        workspace_name,
        services,
    ):
        # Create user in Bobapp
        check_dk = self.update_user(email, password, first_name, last_name, role)

        # Add workspace default to user
        if check_dk:
            self.update_user_workspace(email, workspace_id, workspace_name, True)

            # Create service
            for serv in services:
                self.create_user_service(serv, email, password)
                self.update_user_service(serv, email, password)
