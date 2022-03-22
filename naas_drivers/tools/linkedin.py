from naas_drivers.driver import InDriver, OutDriver
import pandas as pd
import requests
import time
import urllib
from datetime import datetime
import secrets

LINKEDIN_API = "https://3hz1hdpnlf.execute-api.eu-west-1.amazonaws.com/prod"
RELEASE_MESSAGE = (
    "Feature not release yet."
    "Please create or comment issue on Jupyter Naas Github: "
    "https://github.com/orgs/jupyter-naas/projects/4"
)
DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TIME_SLEEP = secrets.randbelow(3) + 2


class LinkedIn(InDriver, OutDriver):
    deprecated = True

    @staticmethod
    def get_activity_id(url):
        return url.split("activity-")[-1].split("-")[0]

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

    def connect(self, li_at: str, jessionid: str):
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

    def get_identity(self, url=None, urn=None):
        res_json = {}
        result = {}
        lk_id = self.get_profile_id(url)
        req_url = f"https://www.linkedin.com/voyager/api/identity/profiles/{lk_id}"
        res = requests.get(req_url, cookies=self.cookies, headers=self.headers)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            print(e)
        else:
            res_json = res.json()
        # Parse json
        data = res_json.get("data", {})
        result = {
            "PROFILE_URN": data.get("entityUrn", "").replace("urn:li:fs_profile:", ""),
            "PROFILE_ID": lk_id,
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
        }
        time.sleep(TIME_SLEEP)
        return pd.DataFrame([result])

    def get_network(self, url=None, urn=None):
        res_json = {}
        result = {}
        lk_id = self.get_profile_id(url)
        req_url = f"https://www.linkedin.com/voyager/api/identity/profiles/{lk_id}/networkinfo"
        res = requests.get(req_url, cookies=self.cookies, headers=self.headers)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            print(e)
        else:
            res_json = res.json()
        # Parse json
        data = res_json.get("data", {})
        result = {
            "PROFILE_URN": data.get("entityUrn", "").replace(
                "urn:li:fs_profileNetworkInfo:", ""
            ),
            "PROFILE_ID": lk_id,
            "DISTANCE": data.get("distance", {}).get("value"),
            "FOLLOWING": data.get("following"),
            "FOLLOWABLE": data.get("followable"),
            "FOLLOWERS_COUNT": data.get("followersCount"),
        }
        time.sleep(TIME_SLEEP)
        return pd.DataFrame([result])

    def get_contact(self, url=None, urn=None):
        res_json = {}
        result = {}
        lk_id = self.get_profile_id(url)
        req_url = f"https://www.linkedin.com/voyager/api/identity/profiles/{lk_id}/profileContactInfo"
        res = requests.get(req_url, cookies=self.cookies, headers=self.headers)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            print(e)
        else:
            res_json = res.json()
        # Parse json
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
            "PROFILE_URN": data.get("entityUrn", "").replace(
                "urn:li:fs_contactinfo:", ""
            ),
            "PROFILE_ID": lk_id,
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
        res_json = {}
        if profile_urn is None:
            profile_urn = LinkedIn.get_profile_urn(self, profile_url)
            if profile_urn is None:
                return "Please enter a valid profile_url or profile_urn"
        req_url = f"{LINKEDIN_API}/profile/getResume?profile_urn={profile_urn}"
        headers = {"Content-Type": "application/json"}
        res = requests.post(req_url, json=self.cookies, headers=headers)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        else:
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
        Return an dataframe object with 30 columns:
        - ACTIVITY_ID       object
        - PAGINATION_TOKEN  object
        - PUBLISHED_DATE    object
        - AUTHOR_NAME       object
        - SUBDESCRIPTION    object
        - TITLE             object
        - TEXT              object
        - CHARACTER_COUNT   int64
        - TAGS              object
        - TAGS_COUNT        int64
        - EMOJIS            object
        - EMOJIS_COUNT      int64
        - LINKS             object
        - LINKS_COUNT       int64
        - PROFILE_MENTION   object
        - COMPANY_MENTION   object
        - CONTENT           object
        - CONTENT_TITLE     object
        - CONTENT_URL       object
        - CONTENT_ID        object
        - IMAGE_URL         object
        - POLL_ID           object
        - POLL_QUESTION     object
        - POLL_RESULTS      object
        - POST_URL          object
        - VIEWS             int64
        - COMMENTS          int64
        - LIKES             int64
        - SHARES            int64
        - ENGAGEMENT_SCORE  float64

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
        while True:
            if limit != -1 and count > limit:
                limit = count
            if limit != -1 and start > limit - 1:
                break
            if pagination_token is not None:
                req_url = f"{LINKEDIN_API}/profile/getPostsFeed?profile_id={profile_id}&count={count}&pagination_token={pagination_token}"
            else:
                req_url = f"{LINKEDIN_API}/profile/getPostsFeed?profile_id={profile_id}&count={count}"
            headers = {"Content-Type": "application/json"}
            res = requests.post(req_url, json=self.cookies, headers=headers)
            try:
                res.raise_for_status()
            except requests.HTTPError as e:
                return e
            else:
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
            start += count

            # Concat dataframe
            df = pd.concat([df, tmp_df], axis=0)

            # Break if until condition is True
            if until_check:
                break
            # Time sleep to avoid linkedin ban
            if sleep:
                time.sleep(TIME_SLEEP)
        # Cleaning
        df.PUBLISHED_DATE = pd.to_datetime(df.PUBLISHED_DATE).dt.tz_localize(
            "Europe/Paris"
        )
        df.PUBLISHED_DATE = df.PUBLISHED_DATE.astype(str)
        return df.reset_index(drop=True)


class Network(LinkedIn):
    def __init__(self, cookies, headers):
        LinkedIn.__init__(self)
        self.cookies = cookies
        self.headers = headers

    def get_followers(self, start=0, count=100, limit=1000):
        limit_init = limit
        df_followers = pd.DataFrame()
        while True:
            if limit != -1 and limit < count:
                count = limit
            req_url = f"{LINKEDIN_API}/network/getFollowers?start={start}&count={count}&limit={limit}"
            headers = {"Content-Type": "application/json"}
            res = requests.post(req_url, json=self.cookies, headers=headers)
            try:
                res.raise_for_status()
            except requests.HTTPError:
                res_json = {}
            else:
                res_json = res.json()
            if len(res_json) == 0:
                break
            df = pd.DataFrame(res_json)
            df_followers = pd.concat([df_followers, df], axis=0)
            start += count
            if limit != -1:
                limit -= count
            time.sleep(TIME_SLEEP)
        df_followers = df_followers.drop_duplicates("PROFILE_URN").reset_index(
            drop=True
        )
        if limit != -1:
            df_followers = df_followers[:limit_init]
        return df_followers

    def get_connections(self, start=0, count=100, limit=1000):
        df_connections = pd.DataFrame()
        while True:
            if limit != -1 and limit < count:
                count = limit
            req_url = f"{LINKEDIN_API}/network/getConnections?start={start}&count={count}&limit={limit}"
            headers = {"Content-Type": "application/json"}
            res = requests.post(req_url, json=self.cookies, headers=headers)
            try:
                res.raise_for_status()
            except requests.HTTPError:
                res_json = {}
            else:
                res_json = res.json()
            if len(res_json) == 0:
                break
            df = pd.DataFrame(res_json)
            df_connections = pd.concat([df_connections, df], axis=0)
            start += count
            if limit != -1:
                limit -= count
            time.sleep(TIME_SLEEP)
        df_connections = df_connections.sort_values(
            by="CREATED_AT", ascending=False
        ).astype(str)
        return df_connections.drop_duplicates().reset_index(drop=True)


class Invitation(LinkedIn):
    def __init__(self, cookies, headers):
        LinkedIn.__init__(self)
        self.cookies = cookies
        self.headers = headers

    def send(self, recipient_url=None, message="", recipient_urn=None):
        if recipient_url is not None:
            recipient_urn = self.get_profile_urn(recipient_url)
        if recipient_urn is None:
            return True
        if message:
            message = ',"message":' '"' + message + '"'
        data = (
            (
                '{"trackingId":"yvzykVorToqcOuvtxjSFMg==","invitations":[],"excludeInvitations":[],'
                '"invitee":{"com.linkedin.voyager.growth.invitation.InviteeProfile":{"profileId":'
            )
            + '"'
            + recipient_urn
            + '"'
            + "}}"
            + message
            + "}"
        )
        head = self.headers
        head["accept"] = "application/vnd.linkedin.normalized+json+2.1"
        res = requests.post(
            "https://www.linkedin.com/voyager/api/growth/normInvitations",
            data=data,
            headers=head,
            cookies=self.cookies,
        )
        time.sleep(TIME_SLEEP)
        try:
            res.raise_for_status()
            return "✉️ Invitation successfully sent !"
        except requests.HTTPError as e:
            return e


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
        headers = {"Content-Type": "application/json"}
        df_result = None
        while True:
            req_url = f"{LINKEDIN_API}/message/getConversations?{urllib.parse.urlencode(params, safe='(),')}"
            res = requests.post(req_url, json=self.cookies, headers=headers)
            if limit != -1:
                limit -= limit_max
                if limit < 0:
                    limit = 0
            try:
                res.raise_for_status()
            except requests.RequestException as e:
                return e
            else:
                res_json = res.json()
            if res.status_code != 200:
                return res.text
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
        headers = {"Content-Type": "application/json"}
        res = requests.post(req_url, json=self.cookies, headers=headers)
        try:
            res.raise_for_status()
        except requests.HTTPError:
            res_json = {}
        else:
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

    def get_stats(self, post_url, activity_id=None):
        """
        Return an dataframe object with 28 columns:
        - ACTIVITY_ID       object
        - AUTHOR_NAME       object
        - SUBDESCRIPTION    object
        - TITLE             object
        - TEXT              object
        - CHARACTER_COUNT   int64
        - TAGS              object
        - TAGS_COUNT        int64
        - EMOJIS            object
        - EMOJIS_COUNT      int64
        - LINKS             object
        - LINKS_COUNT       int64
        - PROFILE_MENTION   object
        - COMPANY_MENTION   object
        - CONTENT           object
        - CONTENT_TITLE     object
        - CONTENT_URL       object
        - CONTENT_ID        object
        - IMAGE_URL         object
        - POLL_ID           object
        - POLL_QUESTION     object
        - POLL_RESULTS      object
        - POST_URL          object
        - VIEWS             int64
        - COMMENTS          int64
        - LIKES             int64
        - SHARES            int64
        - ENGAGEMENT_SCORE  float64

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
        headers = {"Content-Type": "application/json"}
        res = requests.post(req_url, json=self.cookies, headers=headers)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        res_json = res.json()
        return pd.DataFrame(res_json)

    def get_polls(self, post_url, activity_id=None):
        """
        Return an dataframe object with 8 columns:
        - PROFILE_ID            object
        - PUBLIC_ID             object
        - FIRSTNAME             object
        - FULLNAME              object
        - OCCUPATION            object
        - PROFILE_PICTURE       object
        - BACKGROUND_PICTURE    object
        - POLL_RESULT           object

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
        headers = {"Content-Type": "application/json"}
        res = requests.post(req_url, json=self.cookies, headers=headers)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        res_json = res.json()
        return pd.DataFrame(res_json)

    def get_comments(self, post_url):
        req_url = f"{LINKEDIN_API}/post/getComments?post_link={post_url}"
        headers = {"Content-Type": "application/json"}
        res = requests.post(req_url, json=self.cookies, headers=headers)
        try:
            res.raise_for_status()
        except requests.HTTPError:
            res_json = {}
        else:
            res_json = res.json()
        df = pd.DataFrame(res_json)
        return df.reset_index(drop=True)

    def get_likes(self, post_url):
        req_url = f"{LINKEDIN_API}/post/getLikes?post_link={post_url}"
        headers = {"Content-Type": "application/json"}
        res = requests.post(req_url, json=self.cookies, headers=headers)
        try:
            res.raise_for_status()
        except requests.HTTPError:
            res_json = {}
        else:
            res_json = res.json()
        df = pd.DataFrame(res_json)
        return df.reset_index(drop=True)


class Event(LinkedIn):
    def __init__(self, cookies, headers):
        LinkedIn.__init__(self)
        self.cookies = cookies
        self.headers = headers

    def get_guests(self, url):
        req_url = f"{LINKEDIN_API}/event/getGuests?event_link={url}"
        headers = {"Content-Type": "application/json"}
        res = requests.post(req_url, json=self.cookies, headers=headers)
        try:
            res.raise_for_status()
        except requests.HTTPError:
            res_json = {}
        else:
            res_json = res.json()
        df = pd.DataFrame(res_json)
        return df.reset_index(drop=True)


class Company(LinkedIn):
    def __init__(self, cookies, headers):
        LinkedIn.__init__(self)
        self.cookies = cookies
        self.headers = headers

    def get_info(self, company_url):
        req_url = f"{LINKEDIN_API}/company/getInfo?company_url={company_url}"
        headers = {"Content-Type": "application/json"}
        res = requests.post(req_url, json=self.cookies, headers=headers)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        else:
            res_json = res.json()
        return pd.DataFrame(res_json).reset_index(drop=True)
