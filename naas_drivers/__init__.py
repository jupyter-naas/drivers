from .darkknight import DarkKnight
from .ftp import Ftp
from .ftps import Ftps
from .geolocator import GeoLocator
from .git import Git
from .google_spreadsheet import GoogleSpreadsheet
from .healthcheck import HealthCheck
from .email import Email
from .mongo import Mongo
from .pdf import Pdf
from .plot import Plot
from .html import Html
from .cityfalcon import Cityfalcon
from .prediction import Prediction
from .sentiment_analysis import SentimentAnalysis
from .toucan import Toucan

__version__ = "0.7.3"

darkknight = DarkKnight
ftp = Ftp
git = Git
ftps = Ftps
geoLocator = GeoLocator
google_spreadsheet = GoogleSpreadsheet
healthCheck = HealthCheck
email = Email
mongo = Mongo
pdf = Pdf
plot = Plot
html = Html
cityfalcon = Cityfalcon
prediction = Prediction
sentiment_analysis = SentimentAnalysis
toucan = Toucan


def help():
    print("=== Drivers === \n")
    print("drivers.darkknight() => Init the driver to connect to our backend \n")
    print("drivers.ftp() => Init the driver to connect to ftp\n")
    print("drivers.ftps() => Init the driver to connect to ftps\n")
    print("drivers.git() => Init the driver to connect to git\n")
    print("drivers.healthcheck() => Init the driver to connect to healthcheck\n")
    print(
        "drivers.google_spreadsheet() => Init the driver to connect to GoogleSpreadsheet\n"
    )
    print("drivers.mailer() => Init the driver to send email\n")
    print("drivers.mongo() => Get the Mongo driver\n")
    print("drivers.pdf() => Get the pdf generator driver\n")
    print("drivers.sentiment() => Get the sentiment driver\n")
    print("drivers.ml() => Get the machine learning driver\n")
    print("drivers.geo() => Get the GeoLocator driver\n")
    print("drivers.plot() => Get the plot driver\n")
    print("drivers.pdf() => Get the pdf driver\n")
