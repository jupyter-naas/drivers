from naas_drivers.driver import InDriver, OutDriver
import pandas as pd
import requests
import time
import urllib
from datetime import datetime, timedelta
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

    def print_deprecated(self, new_funct):
        if self.deprected:
            print(f"This function is deprecated, please use {new_funct}")

    def get_profile_id(self, url):
        return url.rsplit("/in/")[-1].rsplit("/")[0]

    def get_activity_id(self, url):
        return url.split("activity-")[-1].split("-")[0]

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
            "WEBSITES": data.get("websites"),
            "INTERESTS": data.get("interests"),
        }
        time.sleep(TIME_SLEEP)
        return pd.DataFrame([result])

    def get_posts_stats(self, profile_url=None, profile_urn=None):
        res_json = {}
        if profile_urn is None:
            profile_urn = LinkedIn.get_profile_urn(self, profile_url)
            if profile_urn is None:
                return "Please enter a valid profile_url or profile_urn"
        req_url = f"{LINKEDIN_API}/profile/getPostsStats?profile_urn={profile_urn}"
        headers = {"Content-Type": "application/json"}
        res = requests.post(req_url, json=self.cookies, headers=headers)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        else:
            res_json = res.json()
        df = pd.DataFrame(res_json)
        time.sleep(TIME_SLEEP)
        return df.reset_index(drop=True)


class Network(LinkedIn):
    def __init__(self, cookies, headers):
        LinkedIn.__init__(self)
        self.cookies = cookies
        self.headers = headers

    def get_followers(self, start=0, count=100, limit=1000):
        df_followers = pd.DataFrame()
        while True:
            req_url = f"{LINKEDIN_API}/network/getFollowers?start={start}&count={count}&limit={limit}"
            headers = {"Content-Type": "application/json"}
            res = requests.post(req_url, json=self.cookies, headers=headers)
            try:
                res.raise_for_status()
            except requests.HTTPError:
                res_json = {}
            else:
                res_json = res.json()
            df = pd.DataFrame(res_json)
            df_followers = pd.concat([df_followers, df], axis=0)
            start += limit
            if len(df) == 0:
                break
            time.sleep(TIME_SLEEP)
        return df_followers.reset_index(drop=True)

    def get_connections(self, start=0, count=100, limit=1000):
        df_connections = pd.DataFrame()
        while True:
            req_url = f"{LINKEDIN_API}/network/getConnections?start={start}&count={count}&limit={limit}"
            headers = {"Content-Type": "application/json"}
            res = requests.post(req_url, json=self.cookies, headers=headers)
            try:
                res.raise_for_status()
            except requests.HTTPError:
                res_json = {}
            else:
                res_json = res.json()
            df = pd.DataFrame(res_json)
            df_connections = pd.concat([df_connections, df], axis=0)
            start += limit
            if len(df) == 0:
                break
            time.sleep(TIME_SLEEP)
        return df_connections.reset_index(drop=True)


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

    def __get_post_update(self, data):
        result = None
        if data.get(
            "$type"
        ) == "com.linkedin.voyager.feed.render.UpdateV2" and data.get("commentary"):
            # Get post url
            post_url = None
            actions = data.get("updateMetadata", {}).get("actions", {})
            for action in actions:
                if action.get("$type") == "com.linkedin.voyager.feed.actions.Action":
                    post_url = action.get("url")
                    if post_url is not None:
                        break
            # Get time delta & month
            time_delta = data.get("actor", {}).get("subDescription", {}).get("text", {})
            if time_delta is not None:
                t = time_delta.rsplit(" •")[0]
                if t[-1:] == "h":
                    date_approx = (
                        datetime.now() - timedelta(hours=int(t[:-1]))
                    ).strftime(DATE_FORMAT)
                if t[-1:] == "d":
                    date_approx = (
                        datetime.now() - timedelta(days=int(t[:-1]))
                    ).strftime(DATE_FORMAT)
                if t[-1:] == "w":
                    date_approx = (
                        datetime.now() - timedelta(weeks=int(t[:-1]))
                    ).strftime(DATE_FORMAT)
                if t[-2:] == "mo":
                    date_approx = (
                        datetime.now() - timedelta(days=int(t[:-2]) * 30)
                    ).strftime(DATE_FORMAT)
                if t[-2:] == "yr":
                    date_approx = (
                        datetime.now() - timedelta(days=int(t[:-2]) * 360)
                    ).strftime(DATE_FORMAT)
            result = {
                "POST_URN": data.get("updateMetadata", {})
                .get("urn")
                .replace("urn:li:activity:", ""),
                "POST_URL": post_url,
                "TITLE": data.get("commentary", {})
                .get("text", {})
                .get("text", "")
                .rsplit("\n")[0],
                "TEXT": data.get("commentary", {})
                .get("text", {})
                .get("text", "")
                .replace("\n", ""),
                "TIME_DELTA": t,
                "DATE_APPROX": date_approx,
                #                 "TAGS_COUNT": data.get("commentary", {})
                #                 .get("text", {})
                #                 .get("text")
                #                 .count("#"),
            }
        #             for i in range(1, result.get("TAGS_COUNT", 1)):
        #                 tag = result.get("TEXT", "").rsplit("#")[i]
        #                 for x in [" ", "\n", ".", ","]:
        #                     tag = tag.rsplit(x)[0]
        #                 result[f"TAG_{i}"] = tag
        #             for elem in data.get("updateMetadata", {}).get("actions", []):
        #                 if data.get("url") is not None:
        #                     result["URL"] = elem.get("url")
        #                     break
        return result

    def __get_social_detail(self, data):
        result = None
        if data.get("$type") == "com.linkedin.voyager.feed.SocialDetail":
            result = {
                "POST_URN": data.get("urn").replace("urn:li:activity:", ""),
                "LIKES": data.get("likes", {}).get("paging", {}).get("total", 0),
                "DIRECT_COMMENTS": data.get("comments", {})
                .get("paging", {})
                .get("total", 0),
            }
            if "urn:li:ugcPost:" in result["POST_URN"]:
                result = None
        return result

    def __get_social_activity_count(self, data, activity_id=None):
        result = None
        if data.get(
            "$type"
        ) == "com.linkedin.voyager.feed.shared.SocialActivityCounts" and (
            activity_id is None
            or activity_id == data.get("urn").replace("urn:li:activity:", "")
        ):
            result = {
                "POST_URN": data.get("urn").replace("urn:li:activity:", ""),
                "COMMENTS": data.get("numComments", 0),
                "LIKES": data.get("numLikes", 0),
                "VIEWS": data.get("numViews", 0),
            }
            if data.get("reactionTypeCounts", []):
                for elem in data.get("reactionTypeCounts", []):
                    result[f'LIKES_{elem.get("reactionType")}'] = elem.get("count", 0)
        return result

    def get_stats(self, post_url=None, activity_id=None):
        if post_url is not None:
            activity_id = self.get_activity_id(post_url)
        if activity_id is None:
            return "Please enter a valid post url"
        post = requests.get(
            f"https://www.linkedin.com/voyager/api/feed/updates/urn:li:activity:{activity_id}",
            cookies=self.cookies,
            headers=self.headers,
        ).json()

        included = post.get("included")
        update = []
        social = []
        activity = []
        for include in included:
            u = self.__get_post_update(include)
            if u is not None:
                update.append(u)
            s = self.__get_social_detail(include)
            if s is not None:
                social.append(s)
            a = self.__get_social_activity_count(include, activity_id)
            if a is not None:
                activity.append(a)
        # Set up dataframe
        df_update = pd.DataFrame(update)
        df_social = pd.DataFrame(social)
        df_activity = pd.DataFrame(activity)

        # Merge
        df = pd.merge(df_update, df_social, on=["POST_URN"], how="left")
        df = pd.merge(df, df_activity, on=["POST_URN", "LIKES"], how="left")
        return df

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
        for index in res_json.keys():
            res_json[index] = [res_json[index]]
        return pd.DataFrame(res_json).reset_index(drop=True)
