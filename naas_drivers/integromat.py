import requests


class Integromat:
    def webhook(self, url, data=None):
        r = requests.post(
            url=url,
            json=data,
        )
        r.raise_for_status()
        return r.json()