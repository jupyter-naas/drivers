from .newsapi import NewsApi
from .integromat import Integromat
from .darkknight import DarkKnight
from .ftp import Ftp
from .geolocator import GeoLocator
from .git import Git
from .gsheet import Gsheet
from .healthcheck import HealthCheck
from .email import Email
from .mongo import Mongo
from .pdf import Pdf
from .plotly import Plotly
from .html import Html
from .jupyter import Jupyter
from .cityfalcon import Cityfalcon
from .prediction import Prediction
from .sentiment_analysis import SentimentAnalysis
from .toucan import Toucan
from .yahoo import Yahoo
from .airtable import Airtable
from .zappier import Zappier
from .ifttt import Ifttt
import requests
import os

__version__ = "0.23.4"

__github_repo = "jupyter-naas/drivers"

if os.environ.get("NAAS_DRIVER_LIGHT_INIT"):
    exit()

darkknight = DarkKnight()
airtable = Airtable()
zappier = Zappier()
jupyter = Jupyter()
integromat = Integromat()
ifttt = Ifttt()
yahoo = Yahoo()
ftp = Ftp()
git = Git()
geoLocator = GeoLocator()
gsheet = Gsheet()
health_check = HealthCheck()
email = Email()
mongo = Mongo()
pdf = Pdf()
plotly = Plotly()
html = Html()
cityfalcon = Cityfalcon()
newsapi = NewsApi()
prediction = Prediction()
sentiment_analysis = SentimentAnalysis()
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
