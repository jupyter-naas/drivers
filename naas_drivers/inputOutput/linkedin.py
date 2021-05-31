from naas_drivers.driver import InDriver, OutDriver
import pandas as pd
import requests
import time
import urllib
from datetime import datetime


class LinkedIn(InDriver, OutDriver):
    # Get LK info from LK
    def __get_id(self, url):
        url = url.rsplit("in/")[-1].rsplit("/")[0]
        return url

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

        # Set connexion to active
        self.connected = True
        return self

    def get_identity(self, username: str):
        username = self.__get_id(username)
        data = requests.get(
            "https://www.linkedin.com/voyager/api/identity/profiles/"
            + username.replace("\n", ""),
            cookies=self.cookies,
            headers=self.headers,
        )
        return data.json()

    def get_network(self, username: str):
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
        activity_id = url.split("activity-")[-1].split("-")[0]
        data = requests.get(
            f"https://www.linkedin.com/voyager/api/feed/updates/urn:li:activity:{activity_id}",
            cookies=self.cookies,
            headers=self.headers,
        )
        return data.json()

    def get_post_data(self, url):
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

    def __get_post_urn(self, post_link):
        response = requests.get(post_link).text
        urn_index = response.index("urn:li:activity:")
        finish_index = response.index('"', urn_index)
        activity_urn = response[urn_index:finish_index]
        return activity_urn.rsplit("?")[0]

    def get_post_likes(self, post_link=None, thread_urn=None, count=100, start=0):
        if post_link:
            thread_urn = urllib.parse.quote(self.get_post_urn(post_link), safe="")

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
