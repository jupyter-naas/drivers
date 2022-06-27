from naas_drivers.driver import InDriver, OutDriver
from dateutil.parser import parse
import pandas as pd
import requests
import os

current_token = os.environ.get("JUPYTERHUB_API_TOKEN", None)


class Jupyter(InDriver, OutDriver):
    base_url = os.environ.get("JUPYTERHUB_URL", "https://app.naas.ai")
    api_url = None
    token = None

    def __init__(self):
        self.api_url = f"{self.base_url}/hub/api"

    def connect(self, token=current_token):
        self.token = token
        self.connected = True
        return self

    def create_user(self, username, password):
        signup_url = f"{self.base_url}/hub/signup"
        login = {
            "username": username,
            "password": password,
        }
        headers = {"Authorization": f"token {self.token}"}
        r = requests.post(signup_url, data=login, headers=headers)
        r.raise_for_status()
        return r.json()

    def get_authorize_user(self, username):
        signup_url = f"{self.base_url}/hub/authorize/{username}"
        headers = {
            "Authorization": f"token {self.token}",
            "Content-type": "application/json",
        }
        r = requests.get(signup_url, headers=headers)
        r.raise_for_status()
        return r.json()

    def change_authorize_user(self, username, is_authorized):
        signup_url = f"{self.base_url}/hub/authorize/{username}"
        headers = {"Authorization": f"token {self.token}"}
        data = {"is_authorized": is_authorized}
        r = requests.post(signup_url, data=data, headers=headers)
        r.raise_for_status()
        return r.json()

    def change_password_user(self, username, password):
        signup_url = f"{self.base_url}/hub/change-password"
        login = {
            "username": username,
            "password": password,
        }
        headers = {"Authorization": f"token {self.token}"}
        r = requests.put(signup_url, data=login, headers=headers)
        r.raise_for_status()
        return r.json()

    def list_users(self):
        signup_url = f"{self.base_url}/hub/signup"
        headers = {"Authorization": f"token {self.token}"}
        r = requests.get(signup_url, headers=headers)
        r.raise_for_status()
        df = pd.DataFrame.from_records(r.json().get("data"))
        return df

    def delete_user(self, username):
        signup_url = f"{self.base_url}/hub/signup"
        login = {
            "username": username,
        }
        headers = {"Authorization": f"token {self.token}"}
        r = requests.delete(signup_url, data=login, headers=headers)
        r.raise_for_status()
        return r.json()

    def get_me(self):
        return self.get_user(os.environ.get("JUPYTERHUB_USER"))

    def get_me_session(self):
        return self.get_user_session(os.environ.get("JUPYTERHUB_USER"))

    def get_me_uptime(self):
        me = self.get_user(os.environ.get("JUPYTERHUB_USER"))
        return self.get_server_uptime(me)

    def restart_me(self):
        username = os.environ.get("JUPYTERHUB_USER")
        return self.stop_user(username)

    def get_users(self):
        self.check_connect()
        r = requests.get(
            f"{self.api_url}/users",
            headers={
                "Authorization": f"token {self.token}",
            },
        )

        r.raise_for_status()
        return r.json()

    def get_user(self, username):
        self.check_connect()
        r = requests.get(
            f"{self.api_url}/users/{username}",
            headers={
                "Authorization": f"token {self.token}",
            },
        )

        r.raise_for_status()
        return r.json()

    def delete_user_terminal(self, username, termId):
        self.check_connect()
        r = requests.delete(
            f"{self.base_url}/user/{username}/api/terminals/{termId}",
            headers={
                "Authorization": f"token {self.token}",
            },
        )

        r.raise_for_status()
        return r.json()

    def get_user_terminal(self, username):
        self.check_connect()
        r = requests.get(
            f"{self.base_url}/user/{username}/api/terminals",
            headers={
                "Authorization": f"token {self.token}",
            },
        )

        r.raise_for_status()
        return r.json()

    def delete_user_session(self, username, sessionId):
        self.check_connect()
        r = requests.delete(
            f"{self.base_url}/user/{username}/api/sessions/{sessionId}",
            headers={
                "Authorization": f"token {self.token}",
            },
        )

        r.raise_for_status()
        return r.json()

    def get_user_session(self, username):
        self.check_connect()
        r = requests.get(
            f"{self.base_url}/user/{username}/api/sessions",
            headers={
                "Authorization": f"token {self.token}",
            },
        )

        r.raise_for_status()
        return r.json()

    def is_user_active(self, username):
        self.check_connect()
        user = self.get_user(username)
        servers = user.get("servers")
        keys = servers.keys()
        if len(keys) > 0:
            return True
        else:
            return False

    def get_server_uptime(self, username):
        self.check_connect()
        user = self.get_user(username)
        servers = user.get("servers")
        keys = servers.keys()
        all_duration = None
        for key in keys:
            server = servers[key]
            then = parse(server.get("started"))
            now = parse(server.get("last_activity"))
            duration = now - then
            if all_duration is None:
                all_duration = duration
            else:
                all_duration += duration
        return all_duration

    def stop_user(self, username):
        self.check_connect()
        r = requests.delete(
            f"{self.api_url}/users/{username}/server",
            headers={
                "Authorization": f"token {self.token}",
            },
        )
        r.raise_for_status()
        return r

    def start_user(self, username, user_options={}):
        self.check_connect()
        r = requests.post(
            f"{self.api_url}/users/{username}/server",
            headers={
                "Authorization": f"token {self.token}",
            },
            json=user_options,
        )
        r.raise_for_status()
        return r

    def restart_user(self, username):
        self.check_connect()
        if self.is_user_active(username):
            user = self.get_user(username)
            user_options = user.get("servers").get("").get("user_options")
            self.stop_user(username)
            self.start_user(username, user_options)

    def get_jobs(self, username):
        self.check_connect()
        try:
            headers = {"Authorization": f"token {self.token}"}
            url = f"https://app.naas.ai/user/{username}/proxy/5000/job"
            res = requests.get(url, headers=headers)
            return res.json()
        except ValueError:
            return []
