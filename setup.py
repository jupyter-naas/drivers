from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

extras_require = {
    "dev": [
        "pytest==6.2.4",
        "pytest-mock==3.6.0",
        "requests-mock==1.9.3",
        "twine==3.5.0",
        "flake8==4.0.1",
        "black>=21.4b2",
        "commitizen==2.17.13",
        "pytest-cov==2.12.1",
    ],
    "airtable": [
        "airtable-python-wrapper==0.15.2",
    ],
    "bigquery": [
        "google-cloud-bigquery==3.3.0",
        "pandas-gbq==0.17.7"
    ],
    "email": ["imap_tools==0.39.0"],
    "ipython": [
        "ipython==7.23.1",
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
    "cython": ["Cython==0.29.23"],
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
        "streamlit>=1.8.1",
    ],
    "teams": [
        "pymsteams==0.1.14",
    ],
    "toucan": [
        "cson==0.8",
        "pyjwt==2.1.0",
    ],
    "twitter": [
        "tweepy==4.10.0",
    ],
    "youtube": ["youtube_transcript_api==0.4.3"],
    "pydash": [
        "pydash==5.1.0",
    ],
    "ml": ["transformers==4.12.5", "tensorflow==2.6.0", "torch==1.8.1", "keras==2.6.0"],
    "sharepoint": ["SharePlum==0.5.1"],
    "snowflake": [
        "snowflake-connector-python==2.7.8"
    ],
    "extra": [
        "pyppeteer==0.2.5",
        "pdfkit==0.6.1",
        "notion==0.0.28",
        "Cython==0.29.23",
        "inflection==0.5.1",
        "joblib==1.0.1",
        "more-itertools==8.7.0",
        "patsy==0.5.1",
        "python-dotenv==0.17.0",
        "kaleido==0.2.1",
        "Quandl==3.6.1",
        "scipy==1.6.3",
        "statsmodels==0.12.2",
        "xlrd==2.0.1",
        "md2pdf==0.5",
        "sendgrid==6.7.0",
        "escapism==1.0.1",
        "openpyxl==3.0.7",
        "gspread==3.7.0",
        "oauth2client==4.1.3",
        "opencv-python==4.5.1.48",
        "pytesseract==0.3.7",
        "wkhtmltopdf==0.2",
        "six==1.15.0",
        "urllib3==1.26.4",
        "chardet==4.0.0",
        "idna==2.9",
        "requests>=2.25.1",
        "python-dateutil==2.8.1",
        "pytz==2021.1",
    ],
}

extras_require["full"] = [
    env for env in extras_require if env != "dev" for env in extras_require[env]
]
extras_require["fulldev"] = [
    env for env in extras_require for env in extras_require[env]
]
extras_require["all"] = extras_require["full"]

setup(
    name="naas-drivers",
    version="0.109.0",
    author="Maxime Jublou",
    author_email="maxime@naas.ai",
    license="BSD",
    description="Drivers made to easy connect to any services",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jupyter-naas/drivers",
    packages=find_packages(exclude=["tests"]),
    extras_require=extras_require,
    install_requires=[
        "pandas==1.2.4",
        "pandas-datareader==0.9.0",
        "requests>=2.25.1",
        "mprop==0.16.0",
        "numpy~=1.19.0",
    ],
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: BSD License",
        "Framework :: Jupyter",
        "Operating System :: OS Independent",
    ],
)
