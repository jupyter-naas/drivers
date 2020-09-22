import requests
import os


class Pdf:
    """ PDF generator lib"""

    __url_pdf_api = os.environ.get("CS_API_PDF", "http://cashstory-pdf:9000")

    def generate(self, url, filename="generated.pdf"):
        """ generate pdf from html url with optional filename"""
        r = requests.get(
            f"{self.__url_pdf_api}/api/render/?url={url}&pdf.width=20.5cm&pdf.height=36.5cm"
        )
        open(filename, "wb").write(r.content)
