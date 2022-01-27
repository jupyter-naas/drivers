from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

extras_requires = {
        "dev": [
            "pytest==6.2.3",
            "pytest-mock==3.6.0",
            "requests-mock==1.9.1",
            "twine==3.4.1",
            "flake8==3.9.1",
            "black==21.4b2",
            "commitizen==2.17.4",
            "pytest-cov==2.11.1",
        ],
        "airtable": [
            "airtable-python-wrapper==0.15.2",
        ],
        "email": [
            "imap_tools==0.39.0"
        ],
        "ipython": [
            "ipython==7.22.0",
        ],
        "emailbuilder": [
            "htmlbuilder==0.1.2",
        ],
        "ftp": [
            "pysftp==0.2.9",
        ],
        "geolocator": [
            "geopy==2.1.0",
        ],
        "git": [
            "GitPython==3.1.14",
        ],
        "google": [
            "google==3.0.0",
            "google-api-python-client==2.3.0",
            "google-auth-httplib2==0.1.0",
            "google-auth-oauthlib==0.4.4",
        ],
        "markdown": [
            "markdown2==2.4.0",
        ],
        "mongo": [
            "pymongo==3.11.3",
        ],
        "newsapi": [
            "newsapi-python==0.2.6",
        ],
        "notion": [
            "notion-client==0.7.1",
            "dacite==1.6.0",
        ],
        "plotly": [
            "plotly==4.14.3",
        ],
        "prediction": [
            "pmdarima==1.8.2",
            "scikit-learn==0.24.2",
        ],
        "sentiment": [
            "vaderSentiment==3.3.2",
        ],
        "slack": [
            "slackclient==2.9.3",
        ],
        "streamlit": [
            "pyngrok==5.0.5",
            "streamlit==0.82.0",
        ],
        "teams": [
            "pymsteams==0.1.14",
        ],
        "toucan": [
            "cson==0.8",
            "pyjwt==2.1.0",
        ],
        "youtube": [
            "transformers==4.12.5",
            "youtube_transcript_api==0.4.3",
            "pydash==5.1.0",
        ],
        "ml": [
            "tensorflow==2.6.0",
            "torch==1.8.1",
            "keras==2.6.0"
        ],
        "extra": [
            #"pyppeteer==0.2.5",
            #"pdfkit==0.6.1",
            #"notion==0.0.28",
            #"Cython==0.29.23",
            #"inflection==0.5.1",
            #"joblib==1.0.1",
            #"more-itertools==8.7.0",
            #"patsy==0.5.1",
            #"python-dotenv==0.17.0",
            #"kaleido==0.2.1",
            #"Quandl==3.6.1",
            #"scipy==1.6.3",
            #"statsmodels==0.12.2",
            #"xlrd==2.0.1",
            #"md2pdf==0.5",
            #"sendgrid==6.7.0",
            #"escapism==1.0.1",
            #"openpyxl==3.0.7",
            #"gspread==3.7.0",
            #"oauth2client==4.1.3",
            #"opencv-python==4.5.1.48",
            #"pytesseract==0.3.7",
            #"wkhtmltopdf==0.2",
            
            #"six==1.15.0",
            #"urllib3==1.26.4",
            #"chardet==4.0.0",
            #"idna==2.9",
            #"requests==2.25.1",
            #"python-dateutil==2.8.1",
            #"pytz==2021.1",
        ]
    }

extras_requires_full = [env for env in extras_requires for env in extras_requires[env]]

setup(
    name="naas-drivers",
    version="0.85.0b1",
    author="Martin Donadieu",
    author_email="martindonadieu@gmail.com",
    license="BSD",
    description="Drivers made to easy connect to any services",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jupyter-naas/drivers",
    packages=find_packages(exclude=["tests"]),
    extras_require={
        "dev": extras_requires["dev"],
        "airtable": extras_requires["airtable"],
        "email": extras_requires["email"],
        "ipython": extras_requires["ipython"],
        "emailbuilder": extras_requires["emailbuilder"],
        "ftp": extras_requires["ftp"],
        "geolocator": extras_requires["geolocator"],
        "git": extras_requires["git"],
        "google": extras_requires["google"],
        "markdown": extras_requires["markdown"],
        "mongo": extras_requires["mongo"],
        "newsapi": extras_requires["newsapi"],
        "notion": extras_requires["notion"],
        "plotly": extras_requires["plotly"],
        "prediction": extras_requires["prediction"],
        "sentiment": extras_requires["sentiment"],
        "slack": extras_requires["slack"],
        "streamlit": extras_requires["streamlit"],
        "teams": extras_requires["teams"],
        "toucan": extras_requires["toucan"],
        "youtube": extras_requires["youtube"],
        "ml": extras_requires["ml"],
        "extra": extras_requires["extra"],
        "full": extras_requires_full,
        "all": extras_requires_full
    },
    install_requires=[
        "mprop==0.16.0",
        "pandas==1.2.4",
        "pandas-datareader==0.9.0",
    ],
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: BSD License",
        "Framework :: Jupyter",
        "Operating System :: OS Independent",
    ],
)
