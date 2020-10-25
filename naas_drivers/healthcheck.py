import requests
from datetime import date
import os


class HealthCheck:
    healthUrl = os.environ.get("HC_API", None)

    def connect(self, url):
        self.healthUrl = url

    def start(self, healthkey):
        """ send start to healthcheck (healthkey)"""
        try:
            requests.get(f"{self.healthUrl}{healthkey}/start")
            return f"Start ==> send to {self.healthUrl}{healthkey}, {date.today()}"
        except requests.exceptions.RequestException:
            return f"Error ==> cannot get health server {self.healthUrl}{healthkey}, {date.today()}"

    def done(self, healthkey):
        """ send done to healthcheck (healthkey)"""
        try:
            requests.get(f"{self.healthUrl}{healthkey}")
            return f"Done ==> send to {self.healthUrl}{healthkey}, {date.today()}"
        except requests.exceptions.RequestException:
            return f"Error ==> cannot get health server {self.healthUrl}{healthkey}, {date.today()}"

    def fail(self, healthkey):
        """ send fail to healthcheck (healthkey)"""
        try:
            requests.get(f"{self.healthUrl}{healthkey}/fail")
            return f"Fail ==> send to {self.healthUrl}{healthkey}, {date.today()}"
        except requests.exceptions.RequestException:
            return f"Error ==> cannot get health server {self.healthUrl}{healthkey}, {date.today()}"

    def check_up(self, url, healthkey, auth=None, verify=True):
        """ check if url is reachable (url, healthkey, auth=None, verify=True)"""
        self.start(healthkey)
        try:
            r = requests.get(url, auth=auth, verify=verify)
            if r.status_code == 200:
                self.done(healthkey)
                return f"{url} is heathy send to {self.healthUrl}{healthkey} {date.today()}"
            else:
                self.fail(healthkey)
                return f"===>Fail {url} send to {self.healthUrl}{healthkey}, {date.today()}"
        except requests.exceptions.RequestException:
            return f"Error ==> cannot get health server {self.healthUrl}{healthkey}, {date.today()}"
