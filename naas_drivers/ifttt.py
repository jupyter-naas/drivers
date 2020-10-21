import requests


class Ifttt:
    def webhook(self, event, key, data=None):
        url = f"https://maker.ifttt.com/trigger/{event}/with/key/{key}"
        r = requests.post(
            url=url,
            json=data,
        )
        r.raise_for_status()
        return r
