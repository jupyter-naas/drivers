from naas_drivers.driver import InDriver
import requests
import os


class Pdf(InDriver):
    """ PDF generator lib"""

    api_url = None
    connected = True

    def connect(self, api_url=None):
        self.api_url = (
            api_url
            if api_url
            else os.environ.get("SCREENSHOT_API", "http://naas-screenshot:9000")
        )
        self.connected = True
        return self

    def get(self, url=None, html=None, filename="generated.pdf"):
        """ generate pdf from html or url with optional filename"""
        self.check_connect()
        json = {
            "output": "pdf",
            "emulateScreenMedia": True,
            "ignoreHttpsErrors": True,
            "scrollPage": False,
            "pdf": {"width": "20.5cm", "height": "36.5cm"},
        }
        if url:
            json["url"] = url
        elif html:
            json["html"] = html

        r = requests.get(
            url=f"{self.api_url}/api/render",
            json=json,
        )
        r.raise_for_status()
        open(filename, "wb").write(r.content)
        print(f"file from {url if url else 'html'} saved in {filename}")
