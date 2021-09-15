from naas_drivers.driver import InDriver, OutDriver
from requests.auth import HTTPBasicAuth
from .__crud import CRUD
import requests
import json
import re
import os

req_headers = {"Accept": "application/json", "Content-Type": "application/json"}


class Me(CRUD):
    base_public_url = None
    endpoint = "me"
    key = None

    def __init__(self, base_url, auth, endpoint="me"):
        self.base_public_url = base_url
        self.key = auth
        self.endpoint = endpoint

    def get(self):
        self.check_connect()
        req = requests.get(
            url=f"{self.base_public_url}/{self.endpoint}",
            headers=req_headers,
            auth=self.key,
            allow_redirects=False,
        )
        if self.raise_error:
            req.raise_for_status()
        return req

    def update(self, data):
        self.check_connect()
        req = requests.put(
            url=f"{self.base_public_url}/{self.endpoint}",
            headers=req_headers,
            auth=self.key,
            json=data,
            allow_redirects=False,
        )
        if self.raise_error:
            req.raise_for_status()
        return req


class CRUDBOB(CRUD):
    def get_all(self, search=None, sort=None, limit=20, skip=0):
        params = {
            "limit": limit,
            "skip": skip,
        }
        if search:
            params["search"] = json.dumps(search)
        if sort:
            params["sort"] = json.dumps(sort)
        self.check_connect()
        req = requests.get(
            url=f"{self.base_public_url}/{self.endpoint}",
            params=params,
            headers=req_headers,
            auth=self.auth,
            allow_redirects=False,
        )
        if self.raise_error:
            req.raise_for_status()
        return req


class SmartTable(CRUDBOB):
    database = None
    collection = None

    def __init__(self, base_url, database, collection, auth):
        self.database = database
        self.collection = collection
        endpoint = f"smarttables/{database}/{collection}"
        CRUDBOB.__init__(self, base_url, endpoint, auth)

    def allowed(self):
        self.check_connect()
        req = requests.post(
            url=f"{self.base_public_url}/{self.endpoint}/allowed",
            auth=self.auth,
            headers=req_headers,
            allow_redirects=False,
        )
        if self.raise_error:
            req.raise_for_status()
        req_json = req.json()
        return True if req_json.allowed else False

    def delete_all(self):
        self.check_connect()
        req = requests.delete(
            url=f"{self.base_public_url}/{self.endpoint}/all",
            auth=self.auth,
            headers=req_headers,
            allow_redirects=False,
        )
        if self.raise_error:
            req.raise_for_status()
        return req


class Users(CRUDBOB):
    def __init__(self, base_url, auth):
        CRUDBOB.__init__(self, base_url, "users", auth)

    def validate_email(self, email):
        regex_email = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"
        return re.match(regex_email, email)

    def get_by_email(self, email):
        # Get user
        search = {"email": email}
        req = self.get_all(search=search, limit=1)
        users = req.json()
        if len(users) == 0:
            return None
        return users[0]

    def create_or_update(
        self,
        email,
        password,
        first_name,
        last_name,
        role="user",
        phone_number=None,
        user_role=None,
    ):

        # -> email
        # Delete space: email must have no space at the begining or end
        email = email.strip().lower()

        # -> role
        if role not in ["user", "admin"]:
            error_text = f"Role {role} not recognized in Bobapp"
            self.print_error(error_text)
            return None

        if not self.validate_email(email):
            error_text = f"User {email} not valid"
            self.print_error(error_text)
            return None

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
        user = self.get_by_email(email)

        # If user does not exist => create user
        if not user:
            user = new_user
            self.send(user)
            print(f"User {email} created in Bobapp, password: {password}")
        else:
            new_user["_id"] = user["_id"]
            self.update(new_user)
            print(f"User {email} updated in Bobapp, password: {password}")
        return True

    def update_workspace(
        self, email, workspace_id=None, workspace_name=None, set_default=False
    ):
        # -> email
        # Delete space: email must have no space at the begining or end
        email = email.strip().lower()

        if not self.validate_email(email):
            error_text = f"User {email} not valid"
            self.print_error(error_text)
            return None

        user = self.get_by_email(email)
        if not user:
            error_text = f"User {email} does not exist in Bobapp"
            self.print_error(error_text)
            return None

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
            self.update(user)
            print(
                f"Workspace {workspace_id} : '{workspace_name}' added to user {email}"
            )
        elif user["workspaces"].get(workspace_id, None):
            error_text = f"Workspace already exists for user {email}"
            self.print_error(error_text)
            return None

        # Set workspace default
        if set_default:
            user["workspaceCurrent"] = {
                "section": 2,
                "id": workspace_id,
            }
            self.update(user)

    def update_service(self, serv, email, password):

        # Check service
        serv_list = ["ftp", "jupyter", "wekan"]
        if serv not in serv_list:
            error_text = (
                f"Service {serv} does not exist. Please change it to {serv_list}"
            )
            self.print_error(error_text)
            return None

        # -> email
        email = email.strip().lower()
        if not self.validate_email(email):
            error_text = f"User {email} not valid"
            self.print_error(error_text)
            return None

        user = self.get_by_email(email)
        if not user:
            error_text = f"User {email} does not exist in Bobapp"
            self.print_error(error_text)
            return None
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
            self.update(user)
            print(f"Service {serv} Added to user", email)
        else:
            error_text = f"Service already exists in Bobapp for {email}"
            self.print_error(error_text)
            return None

    def create_service(self, serv, email, password, token):

        # Create login
        email = email.strip().lower()
        if not self.validate_email(email):
            error_text = f"User {email} not valid"
            self.print_error(error_text)
            return None

        login = {
            "username": email,
            "password": password,
        }

        # Init services
        serv_list = ["ftp", "jupyter"]
        if serv not in serv_list:
            error_text = (
                f"Service {serv} does not exist. Please change it to {serv_list}"
            )
            self.print_error(error_text)
            return None

        if serv == "ftp":
            # Variable FTP
            AUTH_TOKEN_FTP = token
            URI_FTP = "https://ftp.naas.ai/user/add"

            # Create user on service ftp
            headers = {"X-Api-Key": AUTH_TOKEN_FTP}
            r = requests.post(URI_FTP, json=login, headers=headers)
            if self.raise_error:
                r.raise_for_status()

        if serv == "jupyter":
            # Jupyter
            AUTH_TOKEN_JUP = token
            URI_JUP = "https://app.naas.ai/hub/signup"

            # Create user on service jupyter
            headers = {"Authorization": AUTH_TOKEN_JUP}
            r = requests.post(URI_JUP, data=login, headers=headers)
            if self.raise_error:
                r.raise_for_status()

    def create(
        self,
        email,
        password,
        first_name,
        last_name,
        role,
        workspace_id,
        workspace_name,
        services,
        tokens={},
    ):
        # Create user in Bobapp
        check_dk = self.create_or_update(email, password, first_name, last_name, role)

        # Add workspace default to user
        if check_dk:
            self.update_workspace(email, workspace_id, workspace_name, True)

            # Create service
            for serv in services:
                token = tokens.get(serv)
                self.create_service(serv, email, password, token)
                self.update_service(serv, email, password)


class Bobapp(InDriver, OutDriver):

    base_public_url = None
    smart_tables = []
    __auth = None
    users = None
    workspaces = None
    me = None

    def connect(self, api_key=None, user=None, api_url=None):
        """
        Description: This class connect you to a Bobapp instance
        """
        self.user = user if user else os.environ.get("JUPYTERHUB_USER", user)
        self.base_public_url = (
            api_url if api_url else os.environ.get("BOBAPP_API", api_url)
        )
        if api_key:
            self.__auth = HTTPBasicAuth(self.user, api_key)
        self.users = Users(self.base_public_url, self.__auth)
        self.workspaces = CRUD(self.base_public_url, "workspaces", self.__auth)
        self.me = Me(self.base_public_url, self.__auth)
        self.connected = True
        return self

    def raise_for_error(self, raise_error=True):
        self.raise_error = raise_error
        self.users.raise_for_error(raise_error)
        self.workspaces.raise_for_error(raise_error)
        self.me.raise_for_error(raise_error)

    def connect_smarttable(self, database, collection):
        self.check_connect()
        return SmartTable(self.base_public_url, database, collection, self.__auth)
