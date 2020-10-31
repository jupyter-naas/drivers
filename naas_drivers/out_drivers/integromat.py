from naas_drivers.driver import Out_Driver
import requests


class Integromat(Out_Driver):
    def send(self, url, data=None):
        r = requests.post(
            url=url,
            json=data,
        )
        r.raise_for_status()
        return r.json()
