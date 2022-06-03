from naas_drivers.driver import InDriver, OutDriver
from naas_drivers.tools.naas_auth import NaasAuth
import requests
from typing import List

EVENTS_API_FQDN = "events.naas.ai"


class NaasEvents(InDriver, OutDriver):
    __headers = None

    def connect(self, token=None) -> "NaasEvents":
        naas_auth = NaasAuth().connect(token)
        self.__headers = naas_auth.headers
        return self

    def add_events(self, events: List[any]):
        res = requests.post(
            f"https://{EVENTS_API_FQDN}/events", headers=self.__headers, json=events
        )
        res.raise_for_status()
        return res.json()

    def user_me(self):
        res = requests.get(f"https://{EVENTS_API_FQDN}/user/me", headers=self.__headers)
        res.raise_for_status()
        return res.json()
