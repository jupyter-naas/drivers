import sys
import requests
import os

__version__ = "0.81.2"

__github_repo = "jupyter-naas/drivers"

__doc_url = "https://naas.gitbook.io/drivers/"

if os.environ.get("NAAS_DRIVER_LIGHT_INIT"):
    exit()


class Drivers:
    def __init__(self, props):
        for prop in props:
            prop_name, module_name, module_instance, params = prop
            setattr(
                self.__class__,
                prop_name,
                property(
                    self.base_prop(prop_name, module_name, module_instance, params)
                ),
            )

    def base_prop(self, prop_name, module_name, module_instance, params):
        def base(self):
            private_prop_name = f"__{prop_name}"
            try:
                getattr(self, private_prop_name)
            except:  # noqa: E722
                setattr(self, private_prop_name, None)
            if getattr(self, private_prop_name) is None:
                imported = __import__(module_name)
                setattr(
                    self,
                    private_prop_name,
                    getattr(imported, module_instance)(**params),
                )
            return getattr(self, private_prop_name)

        return base

    def doc(self):
        return __doc_url

    def version(self):
        print(__version__)

    def get_last_version(self):
        url = f"https://api.github.com/repos/{__github_repo}/tags"
        response = requests.get(
            url, headers={"Accept": "application/vnd.github.v3+json"}
        )
        return response.json()[0]["name"]

    def up_to_date(self):
        return self.get_last_version() == self.version()


drivers = Drivers(
    [
        ("optimise", "naas_drivers.tools.optimise", "Optimise", {}),
        ("cityfalcon", "naas_drivers.tools.cityfalcon", "Cityfalcon", {}),
        ("geolocator", "naas_drivers.tools.geolocator", "Geolocator", {}),
        ("newsapi", "naas_drivers.tools.newsapi", "Newsapi", {}),
        ("prediction", "naas_drivers.tools.prediction", "Prediction", {}),
        ("sentiment", "naas_drivers.tools.sentiment", "Sentiment", {}),
        ("yahoofinance", "naas_drivers.tools.yahoofinance", "Yahoofinance", {}),
        ("pdf", "naas_drivers.tools.pdf", "Pdf", {}),
        ("plotly", "naas_drivers.tools.plotly", "Plotly", {}),
        ("emailbuilder", "naas_drivers.tools.emailbuilder", "EmailBuilder", {}),
        (
            "emailBuilder",
            "naas_drivers.tools.emailbuilder",
            "EmailBuilder(True)",
            {"deprecated": True},
        ),
        ("templates", "naas_drivers.tools.awesomenotebook", "AwesomeNotebooks", {}),
        (
            "html",
            "naas_drivers.tools.emailbuilder",
            "EmailBuilder(True)",
            {"deprecated": True},
        ),
        ("markdown", "naas_drivers.tools.markdown", "Markdown", {}),
        ("teams", "naas_drivers.tools.teams", "Teams", {}),
        ("slack", "naas_drivers.tools.slack", "Slack", {}),
        ("qonto", "naas_drivers.tools.qonto", "Qonto", {}),
        ("naasauth", "naas_drivers.tools.naas_auth", "NaasAuth", {}),
        ("bubble", "naas_drivers.tools.bubble", "Bubble", {}),
        ("email", "naas_drivers.tools.email", "Email", {}),
        ("healthcheck", "naas_drivers.tools.healthcheck", "Healthcheck", {}),
        ("ifttt", "naas_drivers.tools.ifttt", "Ifttt", {}),
        ("integromat", "naas_drivers.tools.integromat", "Integromat", {}),
        ("zapier", "naas_drivers.tools.zapier", "Zapier", {}),
        ("streamlit", "naas_drivers.tools.streamlit", "Streamlit", {}),
        ("bobapp", "naas_drivers.tools.bobapp", "Bobapp", {}),
        ("airtable", "naas_drivers.tools.airtable", "Airtable", {}),
        ("jupyter", "naas_drivers.tools.jupyter", "Jupyter", {}),
        ("ftp", "naas_drivers.tools.ftp", "Ftp", {}),
        ("git", "naas_drivers.tools.git", "Git", {}),
        ("gsheet", "naas_drivers.tools.gsheet", "Gsheet", {}),
        ("notion", "naas_drivers.tools.notion", "Notion", {}),
        ("mongo", "naas_drivers.tools.mongo", "Mongo", {}),
        ("toucan", "naas_drivers.tools.toucan", "Toucan", {}),
        ("linkedin", "naas_drivers.tools.linkedin", "LinkedIn", {}),
        ("hubspot", "naas_drivers.tools.hubspot", "Hubspot", {}),
        ("thinkific", "naas_drivers.tools.thinkific", "Thinkific", {}),
        ("naascredits", "naas_drivers.tools.naas_credits", "NaasCredits", {}),
        ("budgetinsight", "naas_drivers.tools.budgetinsight", "BudgetInsight", {}),
        ("taggun", "naas_drivers.tools.taggun", "Taggun", {}),
        ("youtube", "naas_drivers.tools.youtube", "Youtube", {}),
        (
            "googleanalytics",
            "naas_drivers.tools.googleanalytics",
            "GoogleAnalytics",
            {},
        ),
    ]
)

sys.modules[__name__] = drivers
