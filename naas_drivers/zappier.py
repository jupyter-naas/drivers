import requests


class Zappier:
    def webhook(self, url, data=None):
        r = requests.post(
            url=url,
            json=data,
        )
        r.raise_for_status()
        r.json()
