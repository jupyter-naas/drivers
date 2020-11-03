from naas_drivers.driver import OutDriver
import requests


class Ifttt(OutDriver):
    def connect(self, key):
        self._key = key
        self.connected = True
        return self

    def send(self, event, data=None):
        self.check_connect()
        url = f"https://maker.ifttt.com/trigger/{event}/with/key/{self._key}"
        r = requests.post(
            url=url,
            json=data,
        )
        r.raise_for_status()
        return r
