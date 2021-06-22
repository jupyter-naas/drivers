from naas_drivers.driver import InDriver, OutDriver
import pandas as pd
import requests
import time
import urllib
from datetime import datetime, timedelta

LINKEDIN_API = "https://3hz1hdpnlf.execute-api.eu-west-1.amazonaws.com/prod"
RELEASE_MESSAGE = (
    "Feature not release yet."
    "Please create or comment issue on Jupyter Naas Github: "
    "https://github.com/orgs/jupyter-naas/projects/4"
)
DATE_FORMAT = "%Y-%m-%d"


class LinkedIn(InDriver, OutDriver):
    deprecated = True

    def print_deprecated(self, new_funct):
        if self.deprected:
            print(f"This function is deprecated, please use {new_funct}")

    def get_profile_id(self, url):
        return url.rsplit("in/")[-1].rsplit("/")[0]

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

    # >>> Deprecated code to be remove
    def __get_id(self, url):
        url = url.rsplit("in/")[-1].rsplit("/")[0]
        return url

    def get_identity(self, username: str):
        self.print_deprecated("profile.get_identity()")
        username = self.__get_id(username)
        data = requests.get(
            "https://www.linkedin.com/voyager/api/identity/profiles/"
            + username.replace("\n", ""),
            cookies=self.cookies,
            headers=self.headers,
        )
        return data.json()

    def get_network(self, username: str):
        self.print_deprecated("profile.get_network()")
        username = self.__get_id(username)
        data = requests.get(
            "https://www.linkedin.com/voyager/api/identity/profiles/"
            + username
            + "/networkinfo",
            cookies=self.cookies,
            headers=self.headers,
        )
        return data.json()

    def get_contact(self, username: str):
        self.print_deprecated("profile.get_contact()")
        username = self.__get_id(username)
        data = requests.get(
            "https://www.linkedin.com/voyager/api/identity/profiles/"
            + username.replace("\n", "")
            + "/profileContactInfo",
            cookies=self.cookies,
            headers=self.headers,
        )
        return data.json()

    def get_profil(self, username: str, output="dataframe"):
        username = self.__get_id(username)
        # Get data from identity
        time.sleep(2)
        profil = self.get_identity(username)
        time.sleep(2)
        network = self.get_network(username)
        time.sleep(2)
        contact = self.get_contact(username)

        # Get profil info
        firstname = None
        lastname = None
        country = None
        adress = None
        lk_headline = None
        lk_industry = None
        bd = None
        bd_day = None
        bd_month = None
        bd_year = None

        pf = profil.get("data")
        if pf is not None:
            firstname = pf.get("firstName")
            lastname = pf.get("lastName").upper()
            birthdate = pf.get("birthDate")
            if birthdate is not None:
                bd_day = birthdate.get("day", "Day Unknown")
                bd_month = birthdate.get("month", "Month Unknown")
                bd_year = birthdate.get("year", "Year Unknown")
                bd = f"{bd_day}, {bd_month} - {bd_year}"
            country = pf.get("geoCountryName")
            adress = pf.get("geoLocationName")
            lk_headline = pf.get("headline")
            lk_industry = pf.get("industryName")
        # Get network info
        nw = network.get("data")
        lk_followers = None
        if nw is not None:
            lk_followers = nw.get("followersCount")
        # Get contact info
        ct = contact.get("data")
        lk_phone = None
        lk_email = None
        lk_twiter = None
        if ct is not None:
            lk_phone = None
            lk_phones = ct.get("phoneNumbers")
            if lk_phones is not None:
                for rows in lk_phones:
                    if rows["type"] == "MOBILE":
                        lk_phone = rows["number"]
                        break
            lk_email = ct.get("emailAddress")
            lk_twiter = None
            lk_twiters = ct.get("twitterHandles")
            if lk_twiters is not None:
                for rows in lk_twiters:
                    lk_twiter = rows["name"]
                    break
        # Profile dict
        lk_profile = {
            "FIRSTNAME": firstname,
            "LASTNAME": lastname,
            "BIRTHDATE_DAY": bd_day,
            "BIRTHDATE_MONTH": bd_month,
            "BIRTHDATE_YEAR": bd_year,
            "BIRTHDATE": bd,
            "COUNTRY": country,
            "ADDRESS": adress,
            "LK_HEADLINE": lk_headline,
            "LK_SECTOR": lk_industry,
            "LK_FOLLOWERS": lk_followers,
            "LK_PHONE": lk_phone,
            "LK_EMAIL": lk_email,
            "LK_TWITER": lk_twiter,
        }

        if output == "json":
            return lk_profile
        if output == "dataframe":
            df = pd.DataFrame.from_records([lk_profile])
            return df

    def get_conversations(self):
        data = requests.get(
            "https://www.linkedin.com/voyager/api/messaging/conversations",
            cookies=self.cookies,
            headers=self.headers,
        )
        return data.json()

    def get_messages(self):
        # Get lk conversation
        message = self.get_conversations()

        # Transform conversation
        messages = message.get("included")
        lk_profile = []
        lk_conversation = []
        lk_event = []
        for m in messages:
            lk_type = m.get("$type")
            if lk_type == "com.linkedin.voyager.identity.shared.MiniProfile":
                # Init variable
                firstname = None
                lastname = None
                occupation = None
                publicidentifier = None
                profile_id = None

                # Get variable from dict
                firstname = m.get("firstName")
                lastname = m.get("lastName")
                occupation = m.get("occupation")
                publicidentifier = m.get("publicIdentifier")
                profile_id = m.get("entityUrn")
                profile_id = profile_id.rsplit("urn:li:fs_miniProfile:")[-1]

                # Create profile dict
                if profile_id != "UNKNOWN":
                    tmp_dict = {}
                    tmp_dict = {
                        "FIRSTNAME": firstname,
                        "LASTNAME": lastname,
                        "OCCUPATION": occupation,
                        "PROFILE_ID": profile_id,
                        "PROFILE_PUBLIC_ID": publicidentifier,
                    }
                    lk_profile.append(tmp_dict)
            if lk_type == "com.linkedin.voyager.messaging.Conversation":
                # Init variable
                profile_id = None
                message_id = None
                lastactivityat = None
                lastreadat = None

                # Get variable from dict
                profile_id = m.get("*participants")
                message_id = m.get("entityUrn")
                message_id = message_id.rsplit("urn:li:fs_conversation:")[-1]
                lastactivityat = m.get("lastActivityAt")
                lastreadat = m.get("lastReadAt")
                if lastactivityat is not None:
                    lastactivityat = datetime.fromtimestamp(lastactivityat / 1000.0)
                if lastreadat is not None:
                    lastreadat = datetime.fromtimestamp(lastreadat / 1000.0)
                # Create conversation dict
                profile_id = str(profile_id).rsplit(",")[-1].rsplit(")")[0]
                if profile_id != "UNKNOWN":
                    tmp_dict = {}
                    tmp_dict = {
                        "PROFILE_ID": profile_id,
                        "MESSAGE_ID": message_id,
                        "LAST_ACTIVITY": lastactivityat,
                        "LAST_READ_AT": lastreadat,
                    }
                    lk_conversation.append(tmp_dict)
            if lk_type == "com.linkedin.voyager.messaging.Event":
                # Init variable
                message_id = None
                message_type = None
                message_text = None

                # Get variable from dict
                message_id = m.get("entityUrn")
                message_id = message_id.rsplit("urn:li:fs_event:(")[-1].rsplit(",")[0]
                message_type = m.get("subtype")
                if message_type != "SPONSORED_INMAIL":
                    for key in m.get("eventContent"):
                        if key == "attributedBody":
                            message_text = m.get("eventContent").get(key).get("text")
                            break
                # Create event dict
                tmp_dict = {}
                tmp_dict = {
                    "MESSAGE_ID": message_id,
                    "MESSAGE_TEXT": message_text,
                    "MESSAGE_TYPE": message_type,
                }
                lk_event.append(tmp_dict)
        # Convert dict to dataframe
        df_profile = pd.DataFrame.from_records(lk_profile)
        df_conversation = pd.DataFrame.from_records(lk_conversation)
        df_event = pd.DataFrame.from_records(lk_event)

        # Merge dataframe
        df_message = pd.merge(df_profile, df_conversation, on="PROFILE_ID", how="left")

        df_message = pd.merge(df_message, df_event, on="MESSAGE_ID", how="left")
        # Cleaning
        to_drop = ["MESSAGE_ID", "PROFILE_ID"]
        df_message = df_message.drop(to_drop, axis=1)
        return df_message.reset_index(drop=True)

    def get_post(self, url):
        self.print_deprecated("post.get_info()")
        activity_id = url.split("activity-")[-1].split("-")[0]
        data = requests.get(
            f"https://www.linkedin.com/voyager/api/feed/updates/urn:li:activity:{activity_id}",
            cookies=self.cookies,
            headers=self.headers,
        )
        return data.json()

    def get_post_data(self, url):
        self.print_deprecated("post.get_info()")
        activity_id = url.split("activity-")[-1].split("-")[0]
        # Get lk conversation
        post = self.get_post(url)

        # Init var
        title = None
        datepost = None
        tot_views = 0
        tot_comments = 0
        tot_likes = 0
        num_lik = 0
        num_pra = 0
        num_int = 0
        num_app = 0
        num_emp = 0

        # Parse json
        posts = post.get("included")
        if posts is not None:
            for p in posts:
                lk_type = p.get("$type")
                if lk_type == "com.linkedin.voyager.feed.shared.SocialActivityCounts":
                    uid = p.get("entityUrn")
                    if (
                        uid
                        == f"urn:li:fs_socialActivityCounts:urn:li:activity:{activity_id}"
                        or "urn:li:fs_socialActivityCounts:urn:li:ugcPost" in uid
                    ):
                        tot_likes = p.get("numLikes")
                        tot_views = p.get("numViews")
                        tot_comments = p.get("numComments")
                        likes = p.get("reactionTypeCounts")
                        if likes is not None:
                            for like in likes:
                                reaction = like.get("reactionType")
                                if reaction == "LIKE":
                                    num_lik = like.get("count")
                                if reaction == "PRAISE":
                                    num_pra = like.get("count")
                                if reaction == "INTEREST":
                                    num_int = like.get("count")
                                if reaction == "APPRECIATION":
                                    num_app = like.get("count")
                                if reaction == "EMPATHY":
                                    num_emp = like.get("count")
                if lk_type == "com.linkedin.voyager.feed.render.UpdateV2":
                    commentary = p.get("commentary")
                    if commentary is not None:
                        title = commentary.get("text").get("text").rsplit("\n")[0]
                    datepost = (
                        p.get("actor").get("subDescription").get("accessibilityText")
                    )
        # Data
        data = {
            "URL": url,
            "TITLE": title,
            "DATE": datepost,
            "VIEWS": tot_views,
            "COMMENTS": tot_comments,
            "LIKES": tot_likes,
            "LIKES_LIKE": num_lik,
            "LIKES_PRAISE": num_pra,
            "LIKES_INTEREST": num_int,
            "LIKES_APPRECIATION": num_app,
            "LIKES_EMPATHY": num_emp,
        }

        # DataFrame
        df = pd.DataFrame([data])
        return df

    @staticmethod
    def get_post_urn(post_link):
        response = requests.get(post_link).text
        urn_index = response.index("urn:li:activity:")
        finish_index = response.index('"', urn_index)
        activity_urn = response[urn_index:finish_index]
        return activity_urn.rsplit("?")[0]

    def get_post_likes(self, post_link=None, thread_urn=None, count=100, start=0):
        self.print_deprecated("post.get_likes()")
        if post_link:
            thread_urn = urllib.parse.quote(LinkedIn.get_post_urn(post_link), safe="")
        if not thread_urn:
            print("Error, specify a 'post_link' or a 'thread_urn'")
            return None
        user = {
            "URN_ID": [],
            "PUBLIC_IDENTIFIER": [],
            "FIRSTNAME": [],
            "LASTNAME": [],
            "JOB_TITLE": [],
        }

        reacts = {"URN_ID": [], "REACTION_TYPE": []}
        url = f"https://www.linkedin.com/voyager/api/feed/reactions?count={count}&q=reactionType&start={start}&threadUrn={thread_urn}"
        while True:
            try:
                res = requests.get(
                    url,
                    cookies=self.cookies,
                    headers=self.headers,
                ).json()
                for elem in res.get("included"):
                    if (
                        elem.get("$type")
                        == "com.linkedin.voyager.identity.shared.MiniProfile"
                    ):
                        user["URN_ID"].append(
                            elem.get("entityUrn").replace("urn:li:fs_miniProfile:", "")
                        )
                        user["PUBLIC_IDENTIFIER"].append(elem.get("publicIdentifier"))
                        user["FIRSTNAME"].append(elem.get("firstName"))
                        user["LASTNAME"].append(elem.get("lastName"))
                        user["JOB_TITLE"].append(elem.get("occupation"))
                    if elem.get("$type") == "com.linkedin.voyager.feed.social.Reaction":
                        reacts["URN_ID"].append(
                            elem.get("actorUrn").replace("urn:li:fs_miniProfile:", "")
                        )
                        reacts["REACTION_TYPE"].append(elem["reactionType"])
                if "paging" in res.get("data"):
                    start += count
                    if res.get("data").get("paging").get("total") < start:
                        break
            except requests.exceptions.RequestException as e:
                print(e)
                break
        df_user = pd.DataFrame(user)
        df_reacts = pd.DataFrame(reacts)

        df = pd.merge(df_user, df_reacts, on="URN_ID", how="left")
        df["POST_URL"] = post_link
        return df

    def get_user_urn(self, user_url):
        user_url = self.__get_id(user_url)
        res = requests.get(
            f"https://www.linkedin.com/voyager/api/identity/profiles/{user_url}",
            cookies=self.cookies,
            headers=self.headers,
        )
        return res.get("data", {}).get("entityUrn").replace("urn:li:fs_profile:", "")

    def send_message(self, content, recipients_url=None, recipients_urn=None):
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
        if type(recipients_url) is not list and recipients_url is not None:
            recipients_url = [recipients_url]
        if recipients_urn is not list:
            if recipients_urn is str:
                recipients_urn = [recipients_urn]
            else:
                recipients_urn = []
        if recipients_url is not None:
            for recipient in recipients_url:
                recipients_urn.append(self.get_user_urn(recipient))
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
        return res.status_code != 201


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
        data = res_json.get("data")
        result = {
            "PROFILE_URN": data.get("entityUrn").replace("urn:li:fs_profile:", ""),
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
        data = res_json.get("data")
        result = {
            "PROFILE_URN": data.get("entityUrn").replace(
                "urn:li:fs_profileNetworkInfo:", ""
            ),
            "PROFILE_ID": lk_id,
            "DISTANCE": data.get("distance").get("value"),
            "FOLLOWING": data.get("following"),
            "FOLLOWABLE": data.get("followable"),
            "FOLLOWERS_COUNT": data.get("followersCount"),
        }
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
        data = res_json.get("data")
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
            "PROFILE_URN": data.get("entityUrn").replace("urn:li:fs_contactinfo:", ""),
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
        return df_connections.reset_index(drop=True)


class Invitation(LinkedIn):
    def __init__(self, cookies, headers):
        LinkedIn.__init__(self)
        self.cookies = cookies
        self.headers = headers

    def send(self, recipient_url=None, message="", recipient_urn=None):
        if recipient_url is not None:
            recipient_urn = self.get_user_urn(recipient_url)
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
        req_url = f"{LINKEDIN_API}/message/getConversations?limit={limit}&count{count}"
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
                "TAGS_COUNT": data.get("commentary", {})
                .get("text", {})
                .get("text")
                .count("#"),
            }
            for i in range(1, result.get("TAGS_COUNT", 1)):
                tag = result.get("TEXT", "").rsplit("#")[i]
                for x in [" ", "\n", ".", ","]:
                    tag = tag.rsplit(x)[0]
                result[f"TAG_{i}"] = tag
            for elem in data.get("updateMetadata", {}).get("actions", []):
                if data.get("url") is not None:
                    result["URL"] = elem.get("url")
                    break
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

    def get_stats(self, post_url=None, activity_id=None):
        if post_url is not None:
            activity_id = self.get_activity_id(post_url)
        if activity_id is None:
            print("Error")
            return None
        post = requests.get(
            f"https://www.linkedin.com/voyager/api/feed/updates/urn:li:activity:{activity_id}",
            cookies=self.cookies,
            headers=self.headers,
        ).json()

        result = {"POST_URN": None, "POST_URL": None, "TITLE": None, "TEXT": None}
        included = post.get("included", [])
        for include in included:
            activity_count = self.__get_social_activity_count(include, activity_id)
            if activity_count:
                result.update(activity_count)
            post_update = self.__get_post_update(include)
            if post_update:
                result.update(post_update)
            social_detail = self.__get_social_detail(include)
            if social_detail:
                result.update(social_detail)
        return pd.DataFrame([result])

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
