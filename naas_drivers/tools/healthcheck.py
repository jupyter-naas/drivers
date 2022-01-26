from naas_drivers.driver import OutDriver
import requests
from datetime import date
import os


class Healthcheck(OutDriver):
    healthUrl = None
    healthkey = None

    def connect(self, healthkey, url=None):
        self.healthUrl = url if url else os.environ.get("HC_API", None)
        self.healthkey = healthkey
        self.connected = True
        return self

    def send(self, mode=""):
        """send (mode) to healthcheck (healthkey)"""
        self.check_connect()
        try:
            url = (
                f"{self.healthUrl}{self.healthkey}/{mode}"
                if mode != ""
                else f"{self.healthUrl}{self.healthkey}"
            )
            requests.get(url)
            return f'{mode if mode != "" else "done"} ==> send to {self.healthUrl}{self.healthkey}, {date.today()}'
        except requests.exceptions.RequestException:
            return f"Error ==> cannot get health server {self.healthUrl}{self.healthkey}, {date.today()}"

    def check_up(self, url, auth=None, verify=True):
        """check if url is reachable (url, healthkey, auth=None, verify=True)"""
        self.check_connect()
        self.send("start")
        try:
            r = requests.get(url, auth=auth, verify=verify)
            if r.status_code == 200:
                self.send()
                return f"{url} is heathy send to {self.healthUrl}{self.healthkey} {date.today()}"
            else:
                self.send("fail")
                return f"===>Fail {url} send to {self.healthUrl}{self.healthkey}, {date.today()}"
        except requests.exceptions.RequestException:
            return f"Error ==> cannot get health server {self.healthUrl}{self.healthkey}, {date.today()}"
