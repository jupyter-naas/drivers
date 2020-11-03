from naas_drivers.driver import Out_driver
import requests


class Zappier(Out_driver):
    def webhook(self, url, data=None):
        r = requests.post(
            url=url,
            json=data,
        )
        r.raise_for_status()
        return r.json()
