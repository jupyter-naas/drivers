import requests
import os


class Pdf:
    """ PDF generator lib"""

    def generate(self, url, filename="generated.pdf"):
        """ generate pdf from html url with optional filename"""
        json = {
            "output": "pdf",
            "url": url,
            "emulateScreenMedia": True,
            "ignoreHttpsErrors": True,
            "scrollPage": False,
            "pdf": {"width": "20.5cm", "height": "36.5cm"},
        }
        r = requests.get(
            url=f"{os.environ.get('SCREENSHOT_API', 'http://naas-screenshot:9000')}/api/render",
            json=json,
        )
        r.raise_for_status()
        open(filename, "wb").write(r.content)
