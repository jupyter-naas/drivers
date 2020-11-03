from naas_drivers.driver import OutDriver
import requests


class Zappier(OutDriver):
    def webhook(self, url, data=None):
        r = requests.post(
            url=url,
            json=data,
        )
        r.raise_for_status()
        return r.json()
