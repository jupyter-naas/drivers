from naas_drivers.tools.awesomenotebook import AwesomeNotebooks
from naas_drivers.tools.yahoofinance import Yahoofinance
from naas_drivers.tools.cityfalcon import Cityfalcon
from naas_drivers.tools.geolocator import Geolocator
from naas_drivers.tools.newsapi import Newsapi
from naas_drivers.tools.prediction import Prediction
from naas_drivers.tools.sentiment import Sentiment
from naas_drivers.tools.optimise import Optimise
from naas_drivers.tools.pdf import Pdf
from naas_drivers.tools.plotly import Plotly
from naas_drivers.tools.emailbuilder import EmailBuilder
from naas_drivers.tools.qonto import Qonto
from naas_drivers.tools.slack import Slack
from naas_drivers.tools.teams import Teams
from naas_drivers.tools.bubble import Bubble
from naas_drivers.tools.healthcheck import Healthcheck
from naas_drivers.tools.ifttt import Ifttt
from naas_drivers.tools.integromat import Integromat
from naas_drivers.tools.zapier import Zapier
from naas_drivers.tools.email import Email
from naas_drivers.tools.bobapp import Bobapp
from naas_drivers.tools.airtable import Airtable
from naas_drivers.tools.jupyter import Jupyter
from naas_drivers.tools.ftp import Ftp
from naas_drivers.tools.git import Git
from naas_drivers.tools.gsheet import Gsheet
from naas_drivers.tools.mongo import Mongo
from naas_drivers.tools.toucan import Toucan
from naas_drivers.tools.linkedin import LinkedIn
from naas_drivers.tools.notion import Notion
from naas_drivers.tools.hubspot import Hubspot
from naas_drivers.tools.thinkific import Thinkific
from naas_drivers.tools.markdown import Markdown
from naas_drivers.tools.streamlit import Streamlit
from naas_drivers.tools.huggingface import Huggingface
from naas_drivers.tools.naas_auth import NaasAuth
from naas_drivers.tools.naas_credits import NaasCredits

import requests
import os

__version__ = "0.74.1"

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
emailbuilder = EmailBuilder()
emailBuilder = EmailBuilder(True)
templates = AwesomeNotebooks()
html = EmailBuilder(True)
markdown = Markdown()
teams = Teams()
slack = Slack()
qonto = Qonto()
huggingface = Huggingface()
naasauth = NaasAuth()

# Out drivers
bubble = Bubble()
email = Email()
healthcheck = Healthcheck()
ifttt = Ifttt()
integromat = Integromat()
zapier = Zapier()
streamlit = Streamlit()

# InOut drivers
bobapp = Bobapp()
airtable = Airtable()
jupyter = Jupyter()
ftp = Ftp()
git = Git()
gsheet = Gsheet()
notion = Notion()
mongo = Mongo()
toucan = Toucan()
linkedin = LinkedIn()
hubspot = Hubspot()
thinkific = Thinkific()
naascredits = NaasCredits()

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
