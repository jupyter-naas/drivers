from .in_drivers.yahoofinance import YahooFinance
from .in_drivers.cityfalcon import Cityfalcon
from .in_drivers.geolocator import Geolocator
from .in_drivers.newsapi import Newsapi
from .in_drivers.prediction import Prediction
from .in_drivers.sentiment import Sentiment
from .in_drivers.pdf import Pdf
from .in_drivers.plotly import Plotly
from .in_drivers.html import Html
from .out_drivers.bubble import Bubble
from .out_drivers.email import Email
from .out_drivers.healthcheck import Healthcheck
from .out_drivers.ifttt import Ifttt
from .out_drivers.integromat import Integromat
from .out_drivers.zappier import Zappier
from .in_out_drivers.bobapp import Bobapp
from .in_out_drivers.airtable import Airtable
from .in_out_drivers.jupyter import Jupyter
from .in_out_drivers.ftp import Ftp
from .in_out_drivers.git import Git
from .in_out_drivers.gsheet import Gsheet
from .in_out_drivers.mongo import Mongo
from .in_out_drivers.Toucan import Toucan
import requests
import os

__version__ = "0.24.18"

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
