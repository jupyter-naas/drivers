import requests
import os
from mprop import mproperty
from subprocess import Popen, PIPE
import sys

__version__ = "0.106.0"

__github_repo = "jupyter-naas/drivers"

__doc_url = "https://naas.gitbook.io/drivers/"

__loaded_drivers = {}

if os.environ.get("NAAS_DRIVER_LIGHT_INIT"):
    exit()


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


def load_driver(_func=None, *, extra_requires=""):
    def fn_wrapper(loader_fn):
        @mproperty
        def wrapper(mod):

            name = loader_fn.__name__

            if name not in __loaded_drivers:
                try:
                    loaded = loader_fn()
                    __loaded_drivers[name] = loaded
                except Exception as e:
                    if extra_requires != "":
                        naas_drivers_path = "/".join(__file__.split("/")[:-2])
                        if (
                            os.path.isfile(os.path.join(naas_drivers_path, "setup.py"))
                            is False
                        ):
                            naas_drivers_path = "naas-drivers"
                        cmd = [
                            "pip",
                            "install",
                            "--user",
                            f"{naas_drivers_path}[{extra_requires}]",
                        ]
                        print(
                            f"""
        👉 Running this command automatically to install missing requirements. $> {(" ").join(cmd)}

        ⚠️ You may need to restart your kernel / execution to be able to use the installed packages.

        💡 You can also run this command prior to execution next time to install these packages the way you want (venv, etc).
            """
                        )
                        process = Popen(cmd, stdout=PIPE, stderr=PIPE)
                        stdout, stderr = process.communicate()
                        print(stdout.decode("utf-8"))
                        print(stderr.decode("utf-8"), file=sys.stderr)
                        loaded = loader_fn()
                        __loaded_drivers[name] = loaded
                    else:
                        raise e
            return __loaded_drivers[name]

        return wrapper

    if _func is None:
        return fn_wrapper
    else:
        return fn_wrapper(_func)


@load_driver
def optimise():
    from naas_drivers.tools.optimise import Optimise

    return Optimise()


@load_driver
def cityfalcon():
    from naas_drivers.tools.cityfalcon import Cityfalcon

    return Cityfalcon()


@load_driver(extra_requires="geolocator")
def geolocator():
    from naas_drivers.tools.geolocator import Geolocator

    return Geolocator()


@load_driver(extra_requires="newsapi")
def newsapi():
    from naas_drivers.tools.newsapi import Newsapi

    return Newsapi()


@load_driver(extra_requires="cython,prediction")
def prediction():
    from naas_drivers.tools.prediction import Prediction

    return Prediction()


@load_driver(extra_requires="sentiment")
def sentiment():
    from naas_drivers.tools.sentiment import Sentiment

    return Sentiment()


@load_driver
def yahoofinance():
    from naas_drivers.tools.yahoofinance import Yahoofinance

    return Yahoofinance()


@load_driver
def pdf():
    from naas_drivers.tools.pdf import Pdf

    return Pdf()


@load_driver(extra_requires="plotly")
def plotly():
    from naas_drivers.tools.plotly import Plotly

    return Plotly()


@load_driver(extra_requires="ipython,emailbuilder")
def emailbuilder():
    from naas_drivers.tools.emailbuilder import EmailBuilder

    return EmailBuilder()


@load_driver(extra_requires="ipython,emailbuilder")
def emailBuilder():
    from naas_drivers.tools.emailbuilder import EmailBuilder

    return EmailBuilder(deprecated=True)


@load_driver(extra_requires="ipython")
def templates():
    from naas_drivers.tools.awesomenotebook import AwesomeNotebooks

    return AwesomeNotebooks()


@load_driver
def html():
    from naas_drivers.tools.emailbuilder import EmailBuilder

    return EmailBuilder(deprecated=True)


@load_driver(extra_requires="markdown")
def markdown():
    from naas_drivers.tools.markdown import Markdown

    return Markdown()


@load_driver(extra_requires="teams")
def teams():
    from naas_drivers.tools.teams import Teams

    return Teams()


@load_driver(extra_requires="slack")
def slack():
    from naas_drivers.tools.slack import Slack

    return Slack()


@load_driver
def qonto():
    from naas_drivers.tools.qonto import Qonto

    return Qonto()


@load_driver
def naasauth():
    from naas_drivers.tools.naas_auth import NaasAuth

    return NaasAuth()


@load_driver
def bubble():
    from naas_drivers.tools.bubble import Bubble

    return Bubble()


@load_driver(extra_requires="email")
def email():
    from naas_drivers.tools.email import Email

    return Email()


@load_driver
def healthcheck():
    from naas_drivers.tools.healthcheck import Healthcheck

    return Healthcheck()


@load_driver
def ifttt():
    from naas_drivers.tools.ifttt import Ifttt

    return Ifttt()


@load_driver
def integromat():
    from naas_drivers.tools.integromat import Integromat

    return Integromat()


@load_driver
def zapier():
    from naas_drivers.tools.zapier import Zapier

    return Zapier()


@load_driver(extra_requires="streamlit")
def streamlit():
    from naas_drivers.tools.streamlit import Streamlit

    return Streamlit()


@load_driver
def bobapp():
    from naas_drivers.tools.bobapp import Bobapp

    return Bobapp()


@load_driver(extra_requires="airtable")
def airtable():
    from naas_drivers.tools.airtable import Airtable

    return Airtable()


@load_driver
def jupyter():
    from naas_drivers.tools.jupyter import Jupyter

    return Jupyter()


@load_driver(extra_requires="ftp")
def ftp():
    from naas_drivers.tools.ftp import Ftp

    return Ftp()


@load_driver(extra_requires="git")
def git():
    from naas_drivers.tools.git import Git

    return Git()


@load_driver
def gsheet():
    from naas_drivers.tools.gsheet import Gsheet

    return Gsheet()


@load_driver(extra_requires="notion")
def notion():
    from naas_drivers.tools.notion import Notion

    return Notion()


@load_driver(extra_requires="mongo")
def mongo():
    from naas_drivers.tools.mongo import Mongo

    return Mongo()


@load_driver(extra_requires="ipython,toucan")
def toucan():
    from naas_drivers.tools.toucan import Toucan

    return Toucan()


@load_driver
def linkedin():
    from naas_drivers.tools.linkedin import LinkedIn

    return LinkedIn()


@load_driver
def hubspot():
    from naas_drivers.tools.hubspot import Hubspot

    return Hubspot()


@load_driver
def thinkific():
    from naas_drivers.tools.thinkific import Thinkific

    return Thinkific()


@load_driver
def naascredits():
    from naas_drivers.tools.naas_credits import NaasCredits

    return NaasCredits()


@load_driver()
def naasevents():
    from naas_drivers.tools.naas_events import NaasEvents

    return NaasEvents()


@load_driver
def budgetinsight():
    from naas_drivers.tools.budgetinsight import BudgetInsight

    return BudgetInsight()


@load_driver
def taggun():
    from naas_drivers.tools.taggun import Taggun

    return Taggun()


@load_driver(extra_requires="youtube,ml,pydash")
def youtube():
    from naas_drivers.tools.youtube import Youtube

    return Youtube()


@load_driver(extra_requires="google")
def googleanalytics():
    from naas_drivers.tools.googleanalytics import GoogleAnalytics

    return GoogleAnalytics()


@load_driver(extra_requires="pydash")
def github():
    from naas_drivers.tools.github import Github

    return Github()


@load_driver
def bazimo():
    from naas_drivers.tools.bazimo import Bazimo

    return Bazimo()


@load_driver(extra_requires="sharepoint")
def sharepoint():
    from naas_drivers.tools.sharepoint import Sharepoint

    return Sharepoint()


@load_driver(extra_requires="ml")
def huggingface():
    from naas_drivers.tools.huggingface import Huggingface

    return Huggingface()


@load_driver(extra_requires="twitter")
def twitter():
    from naas_drivers.tools.twitter import Twitter

    return Twitter()


@load_driver(extra_requires="snowflake")
def snowflake():
    from naas_drivers.tools.snowflake import Snowflake

    return Snowflake()


@load_driver(extra_requires="google-cloud-bigquery,pandas-gbq")
def bigquery():
    from naas_drivers.tools.bigquery import BigQuery

    return BigQuery()
