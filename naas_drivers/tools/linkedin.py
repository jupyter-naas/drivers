import pandas as pd
import requests
import time
import urllib
from datetime import datetime
import secrets
import pydash as _pd
import naas
from naas_drivers.tools.emailbuilder import EmailBuilder
from naas_drivers.tools.naas_auth import NaasAuth
import json

emailbuilder = EmailBuilder()
naasauth = NaasAuth()

LINKEDIN_API = "https://3hz1hdpnlf.execute-api.eu-west-1.amazonaws.com/prod"
RELEASE_MESSAGE = (
    "Feature not release yet."
    "Please create or comment issue on Jupyter Naas Github: "
    "https://github.com/orgs/jupyter-naas/projects/4"
)
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

    @staticmethod
    def get_activity_id(url):
        if "-activity-" in url:
            return url.split("-activity-")[-1].split("-")[0]
        if ":activity:" in url:
            return url.split(":activity:")[-1]

    def print_deprecated(self, new_funct):
        if self.deprected:
            print(f"This function is deprecated, please use {new_funct}")

    def get_profile_id(self, url):
        return url.rsplit("/in/")[-1].rsplit("/")[0]

    def get_profile_urn(self, url):
        lk_id = self.get_profile_id(url)
        res = requests.get(
            f"https://www.linkedin.com/voyager/api/identity/profiles/{lk_id}",
            cookies=self.cookies,
            headers=self.headers,
        )
        # Check if requests is successful
        try:
            res.raise_for_status()
            res_json = res.json()
            return (
                res_json.get("data", {})
                .get("entityUrn")
                .replace("urn:li:fs_profile:", "")
            )
        except requests.HTTPError as e:
            return e

    def get_birthdate(self, bd):
        if bd is None:
            return "No birthdate"
        bd_day = bd.get("day", "Day Unknown")
        bd_month = bd.get("month", "Month Unknown")
        bd_year = bd.get("year", "Year Unknown")
        return f"{bd_day}/{bd_month}/{bd_year}"

    def clear_occupation(self, occupation):
        if occupation is not None:
            occupation = occupation.strip().replace("\n", " ")
        return occupation

    def connect(
        self,
        li_at: str = None,
        jessionid: str = None,
    ):
        # Init lk attribute
        self.li_at = li_at
        self.jessionid = jessionid

        # Init cookies
        self.cookies = {"li_at": self.li_at, "JSESSIONID": f'"{self.jessionid}"'}

        # Init headers
        self.headers = {
            "X-Li-Lang": "en_US",
            "Accept": "application/vnd.linkedin.normalized+json+2.1",
            "Cache-Control": "no-cache",
            "csrf-Token": self.jessionid.replace('"', ""),
            "X-Requested-With": "XMLHttpRequest",
            "X-Restli-Protocol-Version": "2.0.0",
        }

        # Init end point
        self.profile = Profile(self.cookies, self.headers)
        self.network = Network(self.cookies, self.headers)
        self.invitation = Invitation(self.cookies, self.headers)
        self.message = Message(self.cookies, self.headers)
        self.post = Post(self.cookies, self.headers)
        self.event = Event(self.cookies, self.headers)
        self.company = Company(self.cookies, self.headers)

        # Set connexion to active
        self.connected = True
        return self


class Profile(LinkedIn):
    def __init__(self, cookies, headers):
        LinkedIn.__init__(self)
        self.cookies = cookies
        self.headers = headers

    def get_identity(self, profile_url=None):
        """
        Return an dataframe object with 15 columns:
        - FIRSTNAME
        - LASTNAME
        - SUMMARY
        - OCCUPATION
        - INDUSTRY_NAME
        - ADDRESS
        - REGION
        - COUNTRY
        - LOCATION
        - BIRTHDATE
        - PROFILE_ID
        - PROFILE_URL
        - PUBLIC_ID
        - BACKGROUND_PICTURE
        - PROFILE_PICTURE

        Parameters
        ----------
        profile_url: str:
            Profile URL from LinkedIn.
            Example : "https://www.linkedin.com/in/florent-ravenel/"
        """
        res_json = {}
        if profile_url is None:
            print("❌ No profile URL. Please enter a profile URL from LinkedIn")
            return res_json
        result = {}
        lk_public_id = self.get_profile_id(profile_url)
        req_url = (
            f"https://www.linkedin.com/voyager/api/identity/profiles/{lk_public_id}"
        )
        res = requests.get(req_url, cookies=self.cookies, headers=self.headers)
        # Raise error
        res.raise_for_status()
        # Parse json
        res_json = res.json()
        data = res_json.get("data", {})
        included = res_json.get("included", {})

        # Init var
        bg_pic_url = None
        profile_pic_url = None

        # Get data from included json
        if len(included) > 0:
            included = included[0]

            # Get background picture
            if included.get("backgroundImage"):
                background_url_end = None
                background_root = None
                background_artifacts = _pd.get(included, "backgroundImage.artifacts")
                if len(background_artifacts) > 0:
                    background_url_end = background_artifacts[
                        len(background_artifacts) - 1
                    ].get("fileIdentifyingUrlPathSegment")
                background_root = _pd.get(included, "backgroundImage.rootUrl")
                if background_url_end and background_root:
                    bg_pic_url = f"{background_root}{background_url_end}"
            # Get profile picture
            if included.get("picture"):
                profile_url_end = None
                profile_root = None
                profile_artifacts = _pd.get(included, "picture.artifacts")
                if len(profile_artifacts) > 0:
                    profile_url_end = profile_artifacts[len(profile_artifacts) - 1].get(
                        "fileIdentifyingUrlPathSegment"
                    )
                profile_root = _pd.get(included, "picture.rootUrl")
                if profile_root and profile_url_end:
                    profile_pic_url = f"{profile_root}{profile_url_end}"
        lk_id = data.get("entityUrn", "").replace("urn:li:fs_profile:", "")
        result = {
            "FIRSTNAME": data.get("firstName"),
            "LASTNAME": data.get("lastName"),
            "SUMMARY": data.get("summary"),
            "OCCUPATION": data.get("headline"),
            "INDUSTRY_NAME": data.get("industryName"),
            "ADDRESS": data.get("address"),
            "REGION": data.get("geoLocationName"),
            "COUNTRY": data.get("geoCountryName"),
            "LOCATION": data.get("locationName"),
            "BIRTHDATE": self.get_birthdate(data.get("birthDateOn")),
            "PROFILE_ID": lk_id,
            "PROFILE_URL": f"https://www.linkedin.com/in/{lk_id}",
            "PUBLIC_ID": lk_public_id,
            "BACKGROUND_PICTURE": bg_pic_url,
            "PROFILE_PICTURE": profile_pic_url,
        }
        time.sleep(TIME_SLEEP)
        return pd.DataFrame([result])

    def get_network(self, profile_url=None):
        """
        Return an dataframe object with 7 columns:
        - PROFILE_ID
        - PROFILE_URL
        - PUBLIC_ID
        - DISTANCE
        - FOLLOWING
        - FOLLOWABLE
        - FOLLOWERS_COUNT

        Parameters
        ----------
        profile_url: str:
            Profile URL from LinkedIn.
            Example : "https://www.linkedin.com/in/florent-ravenel/"
        """
        res_json = {}
        if profile_url is None:
            print("❌ No profile URL. Please enter a profile URL from LinkedIn")
            return res_json
        result = {}
        lk_id = self.get_profile_id(profile_url)
        req_url = f"https://www.linkedin.com/voyager/api/identity/profiles/{lk_id}/networkinfo"
        res = requests.get(req_url, cookies=self.cookies, headers=self.headers)
        # Raise error
        res.raise_for_status()
        # Parse json
        res_json = res.json()
        data = res_json.get("data", {})
        result = {
            "PROFILE_ID": data.get("entityUrn", "").replace(
                "urn:li:fs_profileNetworkInfo:", ""
            ),
            "PROFILE_URL": f"https://www.linkedin.com/in/{lk_id}",
            "PUBLIC_ID": lk_id,
            "DISTANCE": data.get("distance", {}).get("value"),
            "FOLLOWING": data.get("following"),
            "FOLLOWABLE": data.get("followable"),
            "FOLLOWERS_COUNT": data.get("followersCount"),
        }
        time.sleep(TIME_SLEEP)
        return pd.DataFrame([result])

    def get_contact(self, profile_url=None):
        """
        Return an dataframe object with 11 columns:
        - PROFILE_ID
        - PROFILE_URL
        - PUBLIC_ID
        - EMAIL
        - CONNECTED_AT
        - BIRTHDATE
        - ADDRESS
        - TWITER
        - PHONENUMBER
        - WEBSITES
        - INTERESTS

        Parameters
        ----------
        profile_url: str:
            Profile URL from LinkedIn.
            Example : "https://www.linkedin.com/in/florent-ravenel/"
        """
        res_json = {}
        if profile_url is None:
            print("❌ No profile URL. Please enter a profile URL from LinkedIn")
            return res_json
        result = {}
        lk_id = self.get_profile_id(profile_url)
        req_url = f"https://www.linkedin.com/voyager/api/identity/profiles/{lk_id}/profileContactInfo"
        res = requests.get(req_url, cookies=self.cookies, headers=self.headers)
        res.raise_for_status()
        # Parse json
        res_json = res.json()
        data = res_json.get("data", {})

        # Specific
        connected_at = data.get("connectedAt")
        if connected_at is not None:
            connected_at = datetime.fromtimestamp(int(str(connected_at)[:-3])).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        lk_phone = None
        lk_phones = data.get("phoneNumbers")
        if lk_phones is not None:
            for rows in lk_phones:
                if rows["type"] == "MOBILE":
                    lk_phone = rows["number"]
                    break
        lk_twiter = None
        lk_twiters = data.get("twitterHandles")
        if lk_twiters is not None:
            for rows in lk_twiters:
                lk_twiter = rows["name"]
                break
        lk_urls = ""
        lk_websites = data.get("websites")
        if lk_websites is not None:
            for rows in lk_websites:
                lk_url = rows["url"]
                lk_urls = f"{lk_urls}{lk_url}, "
        result = {
            "PROFILE_ID": data.get("entityUrn", "").replace(
                "urn:li:fs_contactinfo:", ""
            ),
            "PROFILE_URL": f"https://www.linkedin.com/in/{lk_id}",
            "PUBLIC_ID": lk_id,
            "EMAIL": data.get("emailAddress"),
            "CONNECTED_AT": connected_at,
            "BIRTHDATE": self.get_birthdate(data.get("birthDateOn")),
            "ADDRESS": data.get("address"),
            "TWITER": lk_twiter,
            "PHONENUMBER": lk_phone,
            "WEBSITES": lk_urls,
            "INTERESTS": data.get("interests"),
        }
        time.sleep(TIME_SLEEP)
        return pd.DataFrame([result])

    def get_resume(self, profile_url=None, profile_urn=None):
        """
        Return an dataframe object with 13 columns:
        - PROFILE_ID                    object
        - PROFILE_URL                   object
        - FULL_NAME                     object
        - CATEGORY                      object
        - TITLE                         object
        - DATE_START                    object
        - DATE_END                      object
        - PLACE_ID                      object
        - PLACE                         object
        - FIELD                         object
        - LOCATION                      object
        - DESCRIPTION                   object
        - DATE_EXTRACT                  object

        Parameters
        ----------
        profile_url: str:
            Profile URL from LinkedIn.
            Example : "https://www.linkedin.com/in/florent-ravenel/"
        """
        res_json = {}
        if profile_url is None:
            print("❌ No profile URL. Please enter a profile URL from LinkedIn")
            return res_json
        if profile_urn is None:
            profile_urn = LinkedIn.get_profile_urn(self, profile_url)
            if profile_urn is None:
                return "Please enter a valid profile_url or profile_urn"
        req_url = f"{LINKEDIN_API}/profile/getResume?profile_urn={profile_urn}"
        res = requests.post(req_url, json=self.cookies, headers=HEADERS)
        res.raise_for_status()

        # Manage LinkedIn API errors
        LinkedIn.manage_api_error(res)

        # Get json result
        res_json = res.json()
        df = pd.DataFrame(res_json)
        return df.reset_index(drop=True)

    def get_posts_feed(
        self,
        profile_url,
        profile_id=None,
        count=1,
        limit=10,
        until={},
        sleep=True,
        pagination_token=None,
    ):
        """
        Return an dataframe object with 32 columns:
        - ACTIVITY_ID                   object
        - PAGINATION_TOKEN              object
        - PUBLISHED_DATE                object
        - AUTHOR_NAME                   object
        - AUTHOR_URL                    object
        - SUBDESCRIPTION                object
        - TITLE                         object
        - TEXT                          object
        - CHARACTER_COUNT               int64
        - TAGS                          object
        - TAGS_COUNT                    int64
        - EMOJIS                        object
        - EMOJIS_COUNT                  int64
        - LINKS                         object
        - LINKS_COUNT                   int64
        - PROFILE_MENTION               object
        - COMPANY_MENTION               object
        - CONTENT                       object
        - CONTENT_TITLE                 object
        - CONTENT_URL                   object
        - CONTENT_ID                    object
        - IMAGE_URL                     object
        - POLL_ID                       object
        - POLL_QUESTION                 object
        - POLL_RESULTS                  object
        - POST_URL                      object
        - VIEWS                         int64
        - COMMENTS                      int64
        - LIKES                         int64
        - SHARES                        int64
        - ENGAGEMENT_SCORE              float64
        - DATE_EXTRACT                  object

        Parameters
        ----------
        profile_url: str:
            Profile url from Linkedin.
            Example : "https://www.linkedin.com/in/florent-ravenel/"

        profile_id: str (default None):
            Linkedin unique profile id identifier
            Example : "ACoAABCNSioBW3YZHc2lBHVG0E_TXYWitQkmwog"

        count: int (default 1, max 100):
            Number of requests sent to LinkedIn API.
            (!) If count > 1, published date will not be returned.

        limit: int (default 10, unlimited=-1):
            Number of posts return by function. It will start with the most recent post.

        until: dict (default {})
            Dict to be set by end user to limit function:
            - key must a columns of the dataframe
            - value must exists in key columns
            Example : "{"POST_URL": "https://www.linkedin.com/posts/naas-ai_opensource-data-activity-6890025972754710529-akfv"

        sleep: boolean (default True):
            Sleeping time between function will be randomly between 3 to 5 seconds.

        pagination_token: str (default None):
            Token related to post used to start function from this post.
            If None, function starts from the last post.

        """
        # Get profile
        if profile_id is None:
            profile_id = LinkedIn.get_profile_urn(self, profile_url)
            if profile_id is None:
                return "Please enter a valid profile_url or profile_urn"
        # Until init
        until_check = False
        keys = []
        if isinstance(until, dict) and len(until) > 0:
            keys = [k for k, v in until.items()]
        # Loop init
        start = 0
        df = pd.DataFrame()
        if limit != -1 and count > limit:
            limit = count
        while True:
            if pagination_token is not None:
                req_url = f"{LINKEDIN_API}/profile/getPostsFeed?profile_id={profile_id}&count={count}&pagination_token={pagination_token}"
            else:
                req_url = f"{LINKEDIN_API}/profile/getPostsFeed?profile_id={profile_id}&count={count}"
            res = requests.post(req_url, json=self.cookies, headers=HEADERS)
            res.raise_for_status()

            # Manage LinkedIn API errors
            LinkedIn.manage_api_error(res)

            # Get json result
            res_json = res.json()
            if len(res_json) == 0:
                break
            # Get response in dataframe
            tmp_df = pd.DataFrame(res_json)

            # Check until limit
            for k in keys:
                v = until.get(k)
                if k in tmp_df.columns:
                    values = tmp_df[k].astype(str).unique().tolist()
                    if str(v) in values:
                        until_check = True
                        break
            # Get pagination token + update start
            pagination_token = tmp_df.loc[0, "PAGINATION_TOKEN"]

            # Concat dataframe
            df = pd.concat([df, tmp_df], axis=0)

            # Break if until condition is True
            if until_check:
                break
            # Time sleep to avoid linkedin ban
            if sleep:
                time.sleep(TIME_SLEEP)
            start += count
            if limit != -1 and start >= limit:
                break
        return df.reset_index(drop=True)


class Network(LinkedIn):
    def __init__(self, cookies, headers):
        LinkedIn.__init__(self)
        self.cookies = cookies
        self.headers = headers

    def get_followers(self, start=0, count=100, limit=1000):
        """
        Return an dataframe object with 13 columns:
        - PROFILE_ID                    object
        - PROFILE_URL                   object
        - PUBLIC_ID                     object
        - FIRSTNAME                     object
        - LASTNAME                      object
        - FULLNAME                      object
        - OCCUPATION                    object
        - PROFILE_PICTURE               object
        - BACKGROUND_PICTURE            object
        - FOLLOWER_COUNT                int64
        - FOLLOWING                     bool
        - INFLUENCER                    bool
        - DATE_EXTRACT                  object

        Parameters
        ----------
        start: int (default 0):
            Starting requests to LinkedIn API.

        count: int (default 100, max 100):
            Number of requests sent to LinkedIn API.

        limit: int (default 1000, unlimited=-1):
            Number of result return by function.

        """
        limit_init = limit
        df_followers = pd.DataFrame()
        if limit != -1 and limit < count:
            count = limit
        while True:
            req_url = f"{LINKEDIN_API}/network/getFollowers?start={start}&count={count}&limit={limit}"
            res = requests.post(req_url, json=self.cookies, headers=HEADERS)
            res.raise_for_status()

            # Manage LinkedIn API errors
            LinkedIn.manage_api_error(res)

            # Get json result
            res_json = res.json()
            if len(res_json) == 0:
                break
            df = pd.DataFrame(res_json)
            df_followers = pd.concat([df_followers, df], axis=0)
            start += count
            if limit != -1 and start >= limit:
                break
            time.sleep(TIME_SLEEP)
        if len(df_followers) > 0:
            df_followers = df_followers.drop_duplicates("PROFILE_ID").reset_index(
                drop=True
            )
            if limit != -1:
                df_followers = df_followers[:limit_init]
        return df_followers.reset_index(drop=True)

    def get_connections(self, start=0, count=100, limit=1000):
        """
        Return an dataframe object with 9 columns:
        - FIRSTNAME                     object
        - LASTNAME                      object
        - OCCUPATION                    object
        - CREATED_AT                    object
        - PROFILE_URL                   object
        - PROFILE_PICTURE               object
        - PROFILE_ID                    object
        - PUBLIC_ID                     object
        - DATE_EXTRACT                  object

        Parameters
        ----------
        start: int (default 0):
            Starting requests to LinkedIn API.

        count: int (default 100, max 100):
            Number of requests sent to LinkedIn API.

        limit: int (default 1000, unlimited=-1):
            Number of result return by function.

        """
        df_connections = pd.DataFrame()
        if limit != -1 and limit < count:
            count = limit
        while True:
            req_url = f"{LINKEDIN_API}/network/getConnections?start={start}&count={count}&limit={limit}"
            res = requests.post(req_url, json=self.cookies, headers=HEADERS)
            res.raise_for_status()

            # Manage LinkedIn API errors
            LinkedIn.manage_api_error(res)

            # Get json result
            res_json = res.json()
            if len(res_json) == 0:
                break
            df = pd.DataFrame(res_json)
            df_connections = pd.concat([df_connections, df], axis=0)
            start += count
            if limit != -1 and start >= limit:
                break
            time.sleep(TIME_SLEEP)
        df_connections = (
            df_connections.drop_duplicates()
            .sort_values(by="CREATED_AT", ascending=False)
            .astype(str)
        )
        return df_connections.reset_index(drop=True)


class Invitation(LinkedIn):
    def __init__(self, cookies, headers):
        LinkedIn.__init__(self)
        self.cookies = cookies
        self.headers = headers

    def get_received(self, start=0, count=100, limit=-1):
        """
        Return an dataframe object with 16 columns:
        - PROFILE_ID
        - PROFILE_URL
        - PUBLIC_ID
        - FIRSTNAME
        - LASTNAME
        - FULLNAME
        - OCCUPATION
        - PROFILE_PICTURE
        - MESSAGE
        - UNSEEN
        - SENT_AT
        - INVITATION_TYPE
        - INVITATION_DESC
        - INVITATION_STATUS
        - INVITATION_ID
        - SHARED_SECRET

        Parameters
        ----------
        start: int (default 0):
            Starting requests to LinkedIn API.

        count: int (default 100, max 100):
            Number of requests sent to LinkedIn API.

        limit: int (default -1, unlimited=-1):
            Number of result return by function.

        """
        df = pd.DataFrame()
        while True:
            if limit != -1 and limit < count:
                count = limit
            req_url = f"{LINKEDIN_API}/invitation/get?start={start}&count={count}"
            res = requests.post(req_url, json=self.cookies, headers=HEADERS)
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
            if limit != -1:
                limit -= count
            time.sleep(TIME_SLEEP)
        return df.reset_index(drop=True)

    def get_sent(self, start=0, count=100, limit=-1):
        """
        Return an dataframe object with 15 columns:
        - PROFILE_ID                    object
        - PROFILE_URL                   object
        - PUBLIC_ID                     object
        - FIRSTNAME                     object
        - LASTNAME                      object
        - FULLNAME                      object
        - OCCUPATION                    object
        - PROFILE_PICTURE               object
        - MESSAGE                       object
        - SENT_AT                       object
        - INVITATION_TYPE               object
        - INVITATION_DESC               object
        - INVITATION_STATUS             object
        - INVITATION_ID                 object
        - DATE_EXTRACT                  object

        Parameters
        ----------
        start: int (default 0):
            Starting requests to LinkedIn API.

        count: int (default 100, max 100):
            Number of requests sent to LinkedIn API.

        limit: int (default -1, unlimited=-1):
            Number of result return by function.

        """
        df = pd.DataFrame()
        while True:
            if limit != -1 and limit < count:
                count = limit
            req_url = f"{LINKEDIN_API}/invitation/getSent?start={start}&count={count}"
            res = requests.post(req_url, json=self.cookies, headers=HEADERS)
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
            if limit != -1:
                limit -= count
            time.sleep(TIME_SLEEP)
        return df.reset_index(drop=True)

    def response(
        self,
        action="accept",
        invitation_id=None,
        invitation_shared_secret=None,
        is_generic=False,
    ):
        """
        Print result : "Accept" or "Ignore"
        Return dataframe profile if invitation type = "Profile"

        Parameters
        ----------
        action: str (default 'accept')
            "accept" or "ignore"

        invitation_id: str (default None)
            Argument given in invitations.get : "INVITATION_ID"

        invitation_shared_secret: str (default None)
            Argument given in invitations.get : "SHARED_SECRET"

        is_generic: boolean (default False):
            Must be True for generic invitation, if "INVITATION_TYPE" != "Profile"
        """
        params = {
            "action": action,
            "invitation_id": invitation_id,
            "invitation_shared_secret": invitation_shared_secret,
            "is_generic": is_generic,
        }
        req_url = f"{LINKEDIN_API}/invitation/response?{urllib.parse.urlencode(params, safe='(),')}"
        res = requests.post(req_url, json=self.cookies, headers=HEADERS)
        res.raise_for_status()
        res_json = res.json()
        if action == "accept":
            print("🤝 Invitation accepted !")
        elif action == "ignore":
            print("❌ Invitation ignored !")
        return pd.DataFrame(res_json)

    def accept(
        self, invitation_id=None, invitation_shared_secret=None, is_generic=False
    ):
        """
        Return dataframe profile if invitation type = "Profile"

        Parameters
        ----------
        invitation_id: str (default None)
            Argument given in invitations.get : "INVITATION_ID"

        invitation_shared_secret: str (default None)
            Argument given in invitations.get : "SHARED_SECRET"

        is_generic: boolean (default False):
            Must be True for generic invitation, if "INVITATION_TYPE" != "Profile"
        """
        return self.response(
            "accept", invitation_id, invitation_shared_secret, is_generic
        )

    def ignore(
        self, invitation_id=None, invitation_shared_secret=None, is_generic=False
    ):
        """
        Return dataframe profile if invitation type = "Profile"

        Parameters
        ----------
        invitation_id: str (default None)
            Argument given in invitations.get : "INVITATION_ID"

        invitation_shared_secret: str (default None)
            Argument given in invitations.get : "SHARED_SECRET"

        is_generic: boolean (default False):
            Must be True for generic invitation, if "INVITATION_TYPE" != "Profile"
        """
        return self.response(
            "ignore", invitation_id, invitation_shared_secret, is_generic
        )

    def send(self, recipient_url=None, message=None):
        """
        Return res.json()

        Parameters
        ----------
        recipient_url: str: (default None)
            Profile url from Linkedin.
            Example : "https://www.linkedin.com/in/florent-ravenel/"

        message: str: (default None)
            Message send with invitation. Must not exceed characters 300 characters.
            Example : "Hi, \nI will be happy to connect with you."
        """
        if recipient_url is not None:
            recipient_id = self.get_profile_urn(recipient_url)
        # Set payload
        payload = {"inviteeProfileUrn": f"urn:li:fsd_profile:{recipient_id}"}
        # Add message in your invitations
        if message:
            # Validating message length (max size is 300 characters)
            if len(message) < 300:
                payload["customMessage"] = message
            else:
                raise BaseException(
                    f"Message too long ({len(message)} characters). Max size is 300 characters"
                )
        # Post request
        req_url = "https://www.linkedin.com/voyager/api/voyagerRelationshipsDashMemberRelationships?action=verifyQuotaAndCreate"
        res = requests.post(
            req_url,
            data=json.dumps(payload),
            cookies=self.cookies,
            headers=self.headers,
        )
        res.raise_for_status()
        time.sleep(TIME_SLEEP)
        return "✉️ Invitation successfully sent !"


class Message(LinkedIn):
    def __init__(self, cookies, headers):
        LinkedIn.__init__(self)
        self.cookies = cookies
        self.headers = headers

    def get_conversations(self, limit=-1, count=20):
        limit_max = 500
        params = {
            "limit": limit_max if limit > limit_max or limit == -1 else limit,
            "count": count,
        }
        df_result = None
        while True:
            if limit != -1:
                limit -= limit_max
                if limit < 0:
                    limit = 0
            req_url = f"{LINKEDIN_API}/message/getConversations?{urllib.parse.urlencode(params, safe='(),')}"
            res = requests.post(req_url, json=self.cookies, headers=HEADERS)
            res.raise_for_status()

            # Manage LinkedIn API errors
            LinkedIn.manage_api_error(res)

            # Get json result
            res_json = res.json()
            df = pd.DataFrame(res_json)
            created_before = (
                (int)(
                    datetime.strptime(
                        df["LAST_ACTIVITY"].iloc[-1], DATETIME_FORMAT
                    ).timestamp()
                )
            ) * 1000
            params["created_before"] = created_before
            if df_result is None:
                df_result = df
            else:
                df_result = pd.concat([df_result, df])
            time.sleep(TIME_SLEEP)
            if limit == 0 or len(df) < params["limit"]:
                break
        return df_result.reset_index(drop=True)

    def get_messages(
        self, conversation_url=None, conversation_urn=None, start=0, limit=-1, count=20
    ):
        req_url = f"{LINKEDIN_API}/message/getMessages?start={start}&limit={limit}&count{count}"
        if conversation_url:
            req_url += f"&conversation_url={conversation_url}"
        if conversation_urn:
            req_url += f"&conversation_urn={conversation_urn}"
        res = requests.post(req_url, json=self.cookies, headers=HEADERS)
        res.raise_for_status()

        # Manage LinkedIn API errors
        LinkedIn.manage_api_error(res)

        # Get json result
        res_json = res.json()
        df = pd.DataFrame(res_json)
        time.sleep(TIME_SLEEP)
        return df.reset_index(drop=True)

    def send(self, content, recipients_url=None, recipients_urn=None):
        recipient_errors = []
        params = {"action": "create"}
        message_event = {
            "eventCreate": {
                "value": {
                    "com.linkedin.voyager.messaging.create.MessageCreate": {
                        "body": content,
                        "attachments": [],
                        "attributedBody": {
                            "text": content,
                            "attributes": [],
                        },
                        "mediaAttachments": [],
                    }
                }
            }
        }
        if type(recipients_url) is str:
            recipients_url = [recipients_url]
        if type(recipients_urn) is str:
            recipients_urn = [recipients_urn]
        if type(recipients_url) is list:
            recipients_urn = []
            for recipient in recipients_url:
                recipient_urn = LinkedIn.get_profile_urn(self, recipient)
                if type(recipient_urn) is requests.exceptions.HTTPError:
                    recipient_errors.append(recipient_urn)
                recipients_urn.append(recipient_urn)
        if len(recipient_errors) > 0:
            return recipient_errors
        message_event["recipients"] = recipients_urn
        message_event["subtype"] = "MEMBER_TO_MEMBER"
        payload = {
            "keyVersion": "LEGACY_INBOX",
            "conversationCreate": message_event,
        }
        res = requests.post(
            "https://www.linkedin.com/voyager/api/messaging/conversations",
            params=params,
            json=payload,
            cookies=self.cookies,
            headers=self.headers,
        )
        time.sleep(TIME_SLEEP)
        try:
            res.raise_for_status()
            return "💬 Message successfully sent !"
        except requests.HTTPError as e:
            return e


class Post(LinkedIn):
    def __init__(self, cookies, headers):
        LinkedIn.__init__(self)
        self.cookies = cookies
        self.headers = headers

    def get_stats(self, post_url=None, activity_id=None):
        """
        Return an dataframe object with 30 columns:
        - ACTIVITY_ID                   object
        - AUTHOR_NAME                   object
        - AUTHOR_URL                    object
        - SUBDESCRIPTION                object
        - TITLE                         object
        - TEXT                          object
        - CHARACTER_COUNT               int64
        - TAGS                          object
        - TAGS_COUNT                    int64
        - EMOJIS                        object
        - EMOJIS_COUNT                  int64
        - LINKS                         object
        - LINKS_COUNT                   int64
        - PROFILE_MENTION               object
        - COMPANY_MENTION               object
        - CONTENT                       object
        - CONTENT_TITLE                 object
        - CONTENT_URL                   object
        - CONTENT_ID                    object
        - IMAGE_URL                     object
        - POLL_ID                       object
        - POLL_QUESTION                 object
        - POLL_RESULTS                  object
        - POST_URL                      object
        - VIEWS                         int64
        - COMMENTS                      int64
        - LIKES                         int64
        - SHARES                        int64
        - ENGAGEMENT_SCORE              int64
        - DATE_EXTRACT                  object

        Parameters
        ----------
        post_url: str:
            Post url from Linkedin.
            Example : "https://www.linkedin.com/posts/j%C3%A9r%C3%A9my-ravenel-8a396910_"
                      "thoughts-monday-work-activity-6891437034473426945-OOOg"

        activity_id: str (default None):
            Linkedin unique post id identifier
            Example : "6891437034473426945"

        """
        # Get profile
        if activity_id is None:
            activity_id = LinkedIn.get_activity_id(post_url)
            if activity_id is None:
                return "Please enter a valid post_url or activity_id"
        req_url = f"{LINKEDIN_API}/post/getStats?activity_id={activity_id}"
        res = requests.post(req_url, json=self.cookies, headers=HEADERS)
        res.raise_for_status()

        # Manage LinkedIn API errors
        LinkedIn.manage_api_error(res)

        # Get json result
        return pd.DataFrame(res.json()).reset_index(drop=True)

    def get_polls(self, post_url=None, activity_id=None):
        """
        Return an dataframe object with 14 columns:
        - PROFILE_ID                    object
        - PROFILE_URL                   object
        - PUBLIC_ID                     object
        - FIRSTNAME                     object
        - LASTNAME                      object
        - FULLNAME                      object
        - OCCUPATION                    object
        - PROFILE_PICTURE               object
        - BACKGROUND_PICTURE            object
        - POLL_ID                       object
        - POLL_QUESTION                 object
        - POLL_RESULT                   object
        - POST_URL                      object
        - DATE_EXTRACT                  object

        Parameters
        ----------
        post_url: str:
            Post url from Linkedin.
            Example : "https://www.linkedin.com/posts/j%C3%A9r%C3%A9my-ravenel-8a396910_"
                      "thoughts-monday-work-activity-6891437034473426945-OOOg"

        activity_id: str (default None):
            Linkedin unique post id identifier
            Example : "6891437034473426945"

        """
        # Get profile
        if activity_id is None:
            activity_id = LinkedIn.get_activity_id(post_url)
            if activity_id is None:
                return "Please enter a valid post_url or activity_id"
        req_url = f"{LINKEDIN_API}/post/getPolls?activity_id={activity_id}"
        res = requests.post(req_url, json=self.cookies, headers=HEADERS)
        res.raise_for_status()

        # Manage LinkedIn API errors
        LinkedIn.manage_api_error(res)

        # Get json result
        return pd.DataFrame(res.json()).reset_index(drop=True)

    def get_comments(
        self, post_url=None, activity_id=None, start=0, count=100, limit=-1, sleep=True
    ):
        """
        Return an dataframe object with 18 columns:
        - PROFILE_ID                    object
        - PROFILE_URL                   object
        - PUBLIC_ID                     object
        - FIRSTNAME                     object
        - LASTNAME                      object
        - FULLNAME                      object
        - OCCUPATION                    object
        - PROFILE_PICTURE               object
        - BACKGROUND_PICTURE            object
        - PROFILE_TYPE                  object
        - TEXT                          object
        - CREATED_TIME                  object
        - LANGUAGE                      object
        - DISTANCE                      object
        - COMMENTS                      int64
        - LIKES                         int64
        - POST_URL                      object
        - DATE_EXTRACT                  object

        Parameters
        ----------
        post_url: str:
            Post url from Linkedin.
            Example : "https://www.linkedin.com/posts/j%C3%A9r%C3%A9my-ravenel-8a396910_"
                      "thoughts-monday-work-activity-6891437034473426945-OOOg"

        activity_id: str (default None):
            Linkedin unique post id identifier
            Example : "6891437034473426945"

        start: int (default 0):
            Number of requests sent to LinkedIn API.
            (!) If count > 1, published date will not be returned.

        count: int (default 1, max 100):
            Number of requests sent to LinkedIn API.
            (!) If count > 1, followed at will not be returned.

        limit: int (default 10, unlimited=-1):
            Number of followers return by function. It will start with the most recent followers.

        sleep: boolean (default True):
            Sleeping time between function will be randomly between 3 to 5 seconds.

        """
        # Get profile
        if activity_id is None:
            activity_id = LinkedIn.get_activity_id(post_url)
            if activity_id is None:
                return "Please enter a valid post_url or activity_id"
        df = pd.DataFrame()
        while True:
            if limit != -1 and limit < count:
                count = limit
            req_url = f"{LINKEDIN_API}/post/getComments?activity_id={activity_id}&start={start}&count={count}"
            res = requests.post(req_url, json=self.cookies, headers=HEADERS)
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
            if limit != -1:
                limit -= count
            if sleep:
                time.sleep(TIME_SLEEP)
        return df.reset_index(drop=True)

    def get_likes(
        self, post_url=None, activity_id=None, start=0, count=100, limit=-1, sleep=True
    ):
        """
        Return an dataframe object with 13 columns:
        - PROFILE_ID                    object
        - PROFILE_URL                   object
        - PUBLIC_ID                     object
        - FIRSTNAME                     object
        - LASTNAME                      object
        - FULLNAME                      object
        - OCCUPATION                    object
        - PROFILE_PICTURE               object
        - BACKGROUND_PICTURE            object
        - PROFILE_TYPE                  object
        - REACTION_TYPE                 object
        - POST_URL                      object
        - DATE_EXTRACT                  object

        Parameters
        ----------
        post_url: str:
            Post url from Linkedin.
            Example : "https://www.linkedin.com/posts/j%C3%A9r%C3%A9my-ravenel-8a396910_"
                      "thoughts-monday-work-activity-6891437034473426945-OOOg"

        activity_id: str (default None):
            Linkedin unique post id identifier
            Example : "6891437034473426945"

        start: int (default 0):
            Number of requests sent to LinkedIn API.
            (!) If count > 1, published date will not be returned.

        count: int (default 1, max 100):
            Number of requests sent to LinkedIn API.
            (!) If count > 1, followed at will not be returned.

        limit: int (default 10, unlimited=-1):
            Number of followers return by function. It will start with the most recent followers.

        sleep: boolean (default True):
            Sleeping time between function will be randomly between 3 to 5 seconds.

        """
        # Get profile
        if activity_id is None:
            activity_id = LinkedIn.get_activity_id(post_url)
            if activity_id is None:
                return "Please enter a valid post_url or activity_id"
        df = pd.DataFrame()
        while True:
            if limit != -1 and limit < count:
                count = limit
            req_url = f"{LINKEDIN_API}/post/getLikes?activity_id={activity_id}&start={start}&count={count}"
            res = requests.post(req_url, json=self.cookies, headers=HEADERS)
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
            if limit != -1:
                limit -= count
            if sleep:
                time.sleep(TIME_SLEEP)
        return df.reset_index(drop=True)


class Event(LinkedIn):
    def __init__(self, cookies, headers):
        LinkedIn.__init__(self)
        self.cookies = cookies
        self.headers = headers

    def get_guests(
        self, event_url="https://www.linkedin.com/events/6762355783188525056/"
    ):
        """
        Return an dataframe object with 8 columns:
        - FULLNAME                      object
        - PROFILE_ID                    object
        - PROFILE_URL                   object
        - PUBLIC_ID                     object
        - OCCUPATION                    object
        - LOCATION                      object
        - DISTANCE                      object
        - DATE_EXTRACT                  object

        Parameters
        ----------
        event_url: str:
            Event url from Linkedin.
            Example : "https://www.linkedin.com/events/6762355783188525056/"

        """
        req_url = f"{LINKEDIN_API}/event/getGuests?event_link={event_url}"
        res = requests.post(req_url, json=self.cookies, headers=HEADERS)
        res.raise_for_status()
        return pd.DataFrame(res.json()).reset_index(drop=True)


class Company(LinkedIn):
    def __init__(self, cookies, headers):
        LinkedIn.__init__(self)
        self.cookies = cookies
        self.headers = headers

    def get_info(self, company_url="https://www.linkedin.com/company/naas-ai/"):
        """
        Return an dataframe object with 18 columns:
        - COMPANY_ID                    object
        - COMPANY_URL                   object
        - COMPANY_NAME                  object
        - UNIVERSAL_NAME                object
        - LOGO_URL                      object
        - INDUSTRY_ID                   object
        - INDUSTRY                      object
        - WEBSITE                       object
        - TAGLINE                       object
        - SPECIALITIES                  object
        - DESCRIPTION                   object
        - COUNTRY                       object
        - REGION                        object
        - CITY                          object
        - STAFF_COUNT                   object
        - STAFF_RANGE                   object
        - FOLLOWER_COUNT                object
        - DATE_EXTRACT                  object

        Parameters
        ----------
        company_url: str:
            Company url from Linkedin.
            Example : "https://www.linkedin.com/company/naas-ai/"

        """
        df = pd.DataFrame()
        req_url = f"{LINKEDIN_API}/company/getInfo?company_url={company_url}"
        res = requests.post(req_url, json=self.cookies, headers=HEADERS)
        res.raise_for_status()

        # Manage LinkedIn API errors
        LinkedIn.manage_api_error(res)

        # Get json result
        df = pd.DataFrame(res.json())
        return df.reset_index(drop=True)

    def get_followers(
        self,
        company_url="https://www.linkedin.com/company/naas-ai/",
        start=0,
        count=1,
        limit=10,
        sleep=True,
    ):
        """
        Return an dataframe object with 11 columns:
        - FIRSTNAME                     object
        - LASTNAME                      object
        - OCCUPATION                    object
        - PROFILE_PICTURE               object
        - PROFILE_URL                   object
        - PROFILE_ID                    object
        - PUBLIC_ID                     object
        - FOLLOWED_AT                   object
        - DISTANCE                      object
        - DATE_EXTRACT                  object

        Parameters
        ----------
        company_url: str:
            Company url from Linkedin.
            Example : "https://www.linkedin.com/company/naas-ai/"

        start: int (default 0):
            Number of requests sent to LinkedIn API.
            (!) If count > 1, published date will not be returned.

        count: int (default 1, max 100):
            Number of requests sent to LinkedIn API.
            (!) If count > 1, followed at will not be returned.

        limit: int (default 10, unlimited=-1):
            Number of followers return by function. It will start with the most recent followers.

        sleep: boolean (default True):
            Sleeping time between function will be randomly between 3 to 5 seconds.

        """
        df = pd.DataFrame()
        while True:
            if limit != -1 and limit < count:
                count = limit
            req_url = f"{LINKEDIN_API}/company/getFollowers?company_url={company_url}&start={start}&count={count}"
            res = requests.post(req_url, json=self.cookies, headers=HEADERS)
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
            if limit != -1:
                limit -= count
            if sleep:
                time.sleep(TIME_SLEEP)
        if len(df) > 0:
            df = df.sort_values(by="FOLLOWED_AT", ascending=False)
        return df.reset_index(drop=True)

    def __get_posts_views(self, activity_id):
        views = 0
        req_url = f"{LINKEDIN_API}/company/getPostsViews?activity_id={activity_id}"
        res = requests.post(req_url, json=self.cookies, headers=HEADERS)
        res.raise_for_status()

        # Manage LinkedIn API errors
        LinkedIn.manage_api_error(res)

        # Get result
        views = res.json().get("VIEWS")
        time.sleep(3)
        return views

    def get_posts_feed(
        self,
        company_url,
        start=0,
        count=100,
        limit=-1,
        sleep=True,
    ):
        """
        Return an dataframe object with 31 columns:
        - ACTIVITY_ID                   object
        - PUBLISHED_DATE                object
        - AUTHOR_NAME                   object
        - AUTHOR_URL                    object
        - SUBDESCRIPTION                object
        - TITLE                         object
        - TEXT                          object
        - CHARACTER_COUNT               int64
        - TAGS                          object
        - TAGS_COUNT                    int64
        - EMOJIS                        object
        - EMOJIS_COUNT                  int64
        - LINKS                         object
        - LINKS_COUNT                   int64
        - PROFILE_MENTION               object
        - COMPANY_MENTION               object
        - CONTENT                       object
        - CONTENT_TITLE                 object
        - CONTENT_URL                   object
        - CONTENT_ID                    object
        - IMAGE_URL                     object
        - POLL_ID                       object
        - POLL_QUESTION                 object
        - POLL_RESULTS                  object
        - POST_URL                      object
        - VIEWS                         int64
        - COMMENTS                      int64
        - LIKES                         int64
        - SHARES                        int64
        - ENGAGEMENT_SCORE              float64
        - DATE_EXTRACT                  object

        Parameters
        ----------
        company_url: str:
            Company url from Linkedin.
            Example : "https://www.linkedin.com/company/naas-ai/"

        start: int (default 0):
            Starting request sent to LinkedIn API.

        count: int (default 1, max 100):
            Number of requests sent to LinkedIn API.
            (!) If count > 1, published date will not be returned.

        limit: int (default 10, unlimited=-1):
            Number of posts return by function. It will start with the most recent post.

        sleep: boolean (default True):
            Sleeping time between function will be randomly between 5 to 10 seconds.

        """
        # Loop init
        df = pd.DataFrame()
        if limit != -1 and count > limit:
            count = limit
        while True:
            req_url = f"{LINKEDIN_API}/company/getPostsFeed?company_url={company_url}&start={start}&count={count}"
            res = requests.post(req_url, json=self.cookies, headers=HEADERS)
            res.raise_for_status()

            # Manage LinkedIn API errors
            LinkedIn.manage_api_error(res)

            # Get json result
            res_json = res.json()
            if len(res_json) == 0:
                break
            # Get response in dataframe
            tmp_df = pd.DataFrame(res_json)

            # Concat dataframe
            df = pd.concat([df, tmp_df], axis=0)

            # Time sleep to avoid linkedin ban
            if sleep:
                time.sleep(TIME_SLEEP)
            start += count
            if limit != -1 and start >= limit:
                break
        # Cleaning
        if len(df) > 0:
            # Add views + engagement score
            df["VIEWS"] = df.apply(
                lambda row: self.__get_posts_views(row.ACTIVITY_ID), axis=1
            )
            df["ENGAGEMENT_SCORE"] = 0
            df.loc[df["VIEWS"] != 0, "ENGAGEMENT_SCORE"] = (
                df["COMMENTS"] + df["LIKES"]
            ) / df["VIEWS"]
        return df.reset_index(drop=True)
