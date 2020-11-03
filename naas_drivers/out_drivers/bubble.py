from naas_drivers.driver import Out_driver
import requests


class Bubble(Out_driver):

    _key = None

    def connect(self, key):
        self._key = key
        self.connected = True
        return self

    def send(self, url, data=None):
        self.check_connect()
        headers = {"Authorization": f"Bearer {self._key}"}
        r = requests.post(
            headers=headers,
            url=url,
            json=data,
        )
        r.raise_for_status()
        r.json()
