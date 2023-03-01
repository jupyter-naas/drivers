import pandas as pd
import requests
import time
import urllib
import secrets
import naas
from naas_drivers.tools.emailbuilder import EmailBuilder
from naas_drivers.tools.naas_auth import NaasAuth

emailbuilder = EmailBuilder()
naasauth = NaasAuth()

LINKEDIN_API = "https://3hz1hdpnlf.execute-api.eu-west-1.amazonaws.com/prod"
DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TIME_SLEEP = secrets.randbelow(5) + 5
HEADERS = {"Content-Type": "application/json"}
EMAIL_COOKIES = "⚠️ Naas.ai - Update your Linkedin cookies"


class LinkedIn:
    deprecated = True

    @staticmethod
    def get_user_email():
        email = None
        user = naasauth.connect().user.me()
        email = user.get("username")
        return email

    @staticmethod
    def email_linkedin_limit(email):
        content = {
            "header_naas": (
                "<a href='https://www.naas.ai/'>"
                "<img align='center' width='30%' target='_blank' style='border-radius:5px;'"
                "src='https://landen.imgix.net/jtci2pxwjczr/assets/5ice39g4.png?w=160'"
                "alt='Please provide more information.'/>"
                "</a>"
            ),
            "txt_0": emailbuilder.text(
                "Hi there,<br><br>"
                "Your LinkedIn cookies needs to be renewed.<br><br>"
                "Please go to naas and update them in your notebook 'Setup LinkedIn'.<br>"
            ),
            "button": emailbuilder.button(
                f"https://app.naas.ai/user/{email}/", "Go to Naas"
            ),
            "signature": "Naas Team",
            "footer": emailbuilder.footer_company(naas=True),
        }
        email_content = emailbuilder.generate(display="iframe", **content)
        return email_content

    @staticmethod
    def send_email_renewed_cookies():
        email = LinkedIn.get_user_email()
        email_content = LinkedIn.email_linkedin_limit(email)
        naas.notification.send(
            email_to=email, subject=EMAIL_COOKIES, html=email_content
        )

    @staticmethod
    def manage_api_error(res):
        if res.status_code != 200:
            if int(res.status_code) == 302:
                LinkedIn.send_email_renewed_cookies()
                raise requests.TooManyRedirects(res.status_code, res.text)
            else:
                raise BaseException(res.status_code, res.text)

    def connect(
        self,
        li_at: str = None,
        jessionid: str = None,
        li_a: str = None,
    ):
        # Init lk attribute
        self.li_at = li_at
        self.jessionid = jessionid
        self.li_a = li_a

        # Init cookies
        self.cookies = {
            "li_at": self.li_at,
            "JSESSIONID": f'"{self.jessionid}"',
            "li_a": self.li_a,
        }

        # Init headers
        self.headers = {"Content-Type": "application/json"}

        # Init end point
        self.leads = Leads(self.cookies, self.headers)

        # Set connexion to active
        self.connected = True
        return self


class Leads(LinkedIn):
    def __init__(self, cookies, headers):
        LinkedIn.__init__(self)
        self.cookies = cookies
        self.headers = headers

    def get_list(self, url, start=0, count=100, limit=1000):
        # Init
        df = pd.DataFrame()
        if limit != -1 and limit < count:
            count = limit
        # Requests API
        while True:
            params = {
                "url": url,
                "start": start,
                "count": count,
            }
            req_url = f"{LINKEDIN_API}/leads/getList?{urllib.parse.urlencode(params, safe='(),')}"
            res = requests.post(req_url, json=self.cookies, headers=self.headers)
            res.raise_for_status()

            # Manage LinkedIn API errors
            LinkedIn.manage_api_error(res)

            # Get json result
            res_json = res.json()
            if len(res_json) == 0:
                break
            tmp_df = pd.DataFrame(res_json)
            df = pd.concat([df, tmp_df], axis=0)
            start += count
            if limit != -1 and start >= limit:
                break
            elif limit != -1 and limit - start < count:
                count = limit - start
            time.sleep(TIME_SLEEP)
        return df.reset_index(drop=True)
