from naas_drivers.input.yahoofinance import Yahoofinance
from naas_drivers.input.cityfalcon import Cityfalcon
from naas_drivers.input.geolocator import Geolocator
from naas_drivers.input.newsapi import Newsapi
from naas_drivers.input.prediction import Prediction
from naas_drivers.input.sentiment import Sentiment
from naas_drivers.input.optimise import Optimise
from naas_drivers.input.pdf import Pdf
from naas_drivers.input.plotly import Plotly
from naas_drivers.input.html import Html
from naas_drivers.output.bubble import Bubble
from naas_drivers.output.email import Email
from naas_drivers.output.healthcheck import Healthcheck
from naas_drivers.output.ifttt import Ifttt
from naas_drivers.output.integromat import Integromat
from naas_drivers.output.zappier import Zappier
from naas_drivers.inputOutput.bobapp import Bobapp
from naas_drivers.inputOutput.airtable import Airtable
from naas_drivers.inputOutput.jupyter import Jupyter
from naas_drivers.inputOutput.ftp import Ftp
from naas_drivers.inputOutput.git import Git
from naas_drivers.inputOutput.gsheet import Gsheet
from naas_drivers.inputOutput.mongo import Mongo
from naas_drivers.inputOutput.toucan import Toucan
import requests
import os

__version__ = "0.34.5"

__github_repo = "jupyter-naas/drivers"

if os.environ.get("NAAS_DRIVER_LIGHT_INIT"):
    exit()

# In drivers
optimise = Optimise()
cityfalcon = Cityfalcon()
geolocator = Geolocator()
newsapi = Newsapi()
prediction = Prediction()
sentiment = Sentiment()
yahoofinance = Yahoofinance()
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
