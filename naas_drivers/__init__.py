from .in_drivers import (
    YahooFinance,
    Cityfalcon,
    Geolocator,
    Newsapi,
    Prediction,
    Sentiment,
    Pdf,
    Plotly,
    Html,
)
from .out_drivers import Bubble, Email, Healthcheck, Ifttt, Integromat, Zappier
from .in_out_drivers import Bobapp, Airtable, Jupyter, Ftp, Git, Gsheet, Mongo, Toucan
import requests
import os

__version__ = "0.24.20"

__github_repo = "jupyter-naas/drivers"

if os.environ.get("NAAS_DRIVER_LIGHT_INIT"):
    exit()

# In drivers
cityfalcon = Cityfalcon()
geolocator = Geolocator()
newsapi = Newsapi()
prediction = Prediction()
sentiment = Sentiment()
yahoofinance = YahooFinance()
pdf = Pdf()
plotly = Plotly()
html = Html()

# Out drivers
bubble = Bubble()
email = Email()
healthcheck = Healthcheck()
ifttt = Ifttt()
integromat = Integromat()
zappier = Zappier()

# InOut drivers
bobapp = Bobapp()
airtable = Airtable()
jupyter = Jupyter()
ftp = Ftp()
git = Git()
gsheet = Gsheet()
mongo = Mongo()
toucan = Toucan()

__doc_url = "https://naas.gitbook.io/drivers/"


def doc():
    return __doc_url


def version():
    print(__version__)


def get_last_version():
    url = f"https://api.github.com/repos/{__github_repo}/tags"
    response = requests.get(url, headers={"Accept": "application/vnd.github.v3+json"})
    return response.json()[0]["name"]


def up_to_date():
    return get_last_version() == version()
