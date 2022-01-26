from naas_drivers.driver import OutDriver
import requests


class Zapier(OutDriver):
    def connect(self, url):
        self._key = url
        self.connected = True
        return self

    def send(self, data=None):
        self.check_connect()
        r = requests.post(
            url=self._key,
            json=data,
        )
        r.raise_for_status()
        return r.json()
