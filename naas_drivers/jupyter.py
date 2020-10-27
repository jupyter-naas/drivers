from dateutil.parser import parse
import requests
import os


class Jupyter:
    base_url = os.environ.get("JUPYTERHUB_URL", "https://app.naas.ai")
    api_url = None
    token = None

    def __init__(self):
        self.api_url = f"{self.base_url}/hub/api"

    def connect(self, token):
        self.token = token

    def create_user(self, username, password, super_admin_token):
        signup_url = f"{self.base_url}hub/signup"
        login = {
            "username": username,
            "password": password,
        }
        headers = {"Authorization": super_admin_token}
        r = requests.post(signup_url, data=login, headers=headers)
        r.raise_for_status()
        return r.json()

    def get_users(self):
        r = requests.get(
            f"{self.api_url}/users",
            headers={
                "Authorization": f"token {self.token}",
            },
        )

        r.raise_for_status()
        return r.json()

    def get_me(self):
        return self.get_user(os.environ.get("JUPYTERHUB_USER"))

    def get_me_uptime(self):
        me = self.get_user(os.environ.get("JUPYTERHUB_USER"))
        return self.get_server_uptime(me)

    def get_user(self, username):
        r = requests.get(
            f"{self.api_url}/users/{username}",
            headers={
                "Authorization": f"token {self.token}",
            },
        )

        r.raise_for_status()
        return r.json()

    def is_user_active(self, user):
        servers = user.get("servers")
        keys = servers.keys()
        if len(keys) > 0:
            return True
        else:
            return False

    def get_server_uptime(self, user):
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
        r = requests.delete(
            f"{self.api_url}/users/{username}/server",
            headers={
                "Authorization": f"token {self.token}",
            },
        )
        r.raise_for_status()
        return r

    def start_user(self, username):
        r = requests.post(
            f"{self.api_url}/users/{username}/server",
            headers={
                "Authorization": f"token {self.token}",
            },
        )
        r.raise_for_status()
        return r

    def restart_user(self, username):
        user = self.get_user(username)
        if user and self.is_user_active(user):
            self.stop_user(username)
            self.start_user(username)
