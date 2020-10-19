import requests


class Bubble:
    def run_workflow(self, url, token, data=None):
        headers = {"Authorization": f"Bearer {token}"}
        r = requests.post(
            headers=headers,
            url=url,
            json=data,
        )
        r.raise_for_status()
        r.json()
