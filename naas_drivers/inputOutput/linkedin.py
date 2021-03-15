from naas_drivers.driver import InDriver, OutDriver
import pandas as pd
import requests
import time
from datetime import datetime

class LinkedIn(InDriver, OutDriver):
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
        data = requests.get(
            "https://www.linkedin.com/voyager/api/identity/profiles/"
            + username.replace("\n", ""),
            cookies=self.cookies,
            headers=self.headers,
        )
        return data.json()

    def get_network(self, username: str):
        data = requests.get(
            "https://www.linkedin.com/voyager/api/identity/profiles/"
            + username
            + "/networkinfo",
            cookies=self.cookies,
            headers=self.headers,
        )
        return data.json()

    def get_contact(self, username: str):
        data = requests.get(
            "https://www.linkedin.com/voyager/api/identity/profiles/"
            + username.replace("\n", "")
            + "/profileContactInfo",
            cookies=self.cookies,
            headers=self.headers,
        )
        return data.json()

    def get_profil(self, username: str, output="dataframe"):
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
        
        pf = profil.get('data')
        if pf != None:
            firstname = pf.get('firstName')
            lastname = pf.get('lastName').upper()
            birthdate = pf.get("birthDate")
            if birthdate != None:
                bd_day = birthdate.get("day", "Day Unknown")
                bd_month = birthdate.get("month", "Month Unknown")
                bd_year = birthdate.get("year", "Year Unknown")
                bd = f"{bd_day}, {bd_month} - {bd_year}"
            country = pf.get("geoCountryName")
            adress = pf.get("geoLocationName")
            lk_headline = pf.get("headline")
            lk_industry = pf.get("industryName")

        # Get network info
        nw = network.get('data')
        lk_followers = None
        if nw != None:
            lk_followers = nw.get("followersCount")

        # Get contact info
        ct = contact.get('data')
        lk_phone = None
        lk_email = None
        lk_twiter = None
        if ct != None:
            lk_phone = None
            lk_phones = ct.get("phoneNumbers")
            if lk_phones != None:
                for rows in lk_phones:
                    if rows["type"] == "MOBILE":
                        lk_phone = rows["number"]
                        break
            lk_email = ct.get("emailAddress")
            lk_twiter = None
            lk_twiters = ct.get("twitterHandles")
            if lk_twiters != None:
                for rows in lk_twiters:
                    lk_twiter = rows["name"]
                    break

        if output == "json":
            return lk_profile

        if output == "dataframe":
            df = pd.DataFrame.from_records([lk_profile])
            return df
        
    def get_conversations(self):
        data = requests.get('https://www.linkedin.com/voyager/api/messaging/conversations',
                            cookies=self.cookies,
                            headers=self.headers)
        return data.json()

    def get_message(self):
        # Get lk conversation
        message = self.get_conversations()

        # Transform conversation
        messages = message["included"]
        lk_profile = []
        lk_conversation = []
        lk_event = []
        for m in messages:
            lk_type = m['$type']
            if lk_type == 'com.linkedin.voyager.identity.shared.MiniProfile':
                # Init variable
                firstname = None
                lastname = None
                occupation = None
                publicidentifier = None
                profile_id = None

                # Get variable from dict
                firstname = m["firstName"]
                lastname = m["lastName"]
                occupation = m["occupation"]
                publicidentifier = m["publicIdentifier"]
                profile_id = m["entityUrn"]
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

            if lk_type == 'com.linkedin.voyager.messaging.Conversation':
                # Init variable
                profile_id = None
                message_id = None
                lastactivityat = None
                lastreadat = None

                # Get variable from dict
                profile_id = m['*participants']
                message_id = m['entityUrn']
                message_id = message_id.rsplit("urn:li:fs_conversation:")[-1]
                lastactivityat = m["lastActivityAt"]
                lastreadat = m["lastReadAt"]
                if lastactivityat != None:
                    lastactivityat = datetime.fromtimestamp(lastactivityat/1000.)
                if lastreadat != None:
                    lastreadat = datetime.fromtimestamp(lastreadat/1000.)

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

            if lk_type == 'com.linkedin.voyager.messaging.Event':
                # Init variable
                message_id = None
                message_type = None
                message_text = None

                # Get variable from dict
                message_id = m["entityUrn"]
                message_id = message_id.rsplit("urn:li:fs_event:(")[-1].rsplit(',')[0]
                message_type = m['subtype']
                if message_type != 'SPONSORED_INMAIL':
                    for key in m["eventContent"]:
                        if key == "attributedBody":
                            message_text = m["eventContent"][key]["text"]
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
        df_message = pd.merge(df_profile,
                              df_conversation,
                              on="PROFILE_ID",
                              how="left")

        df_message = pd.merge(df_message,
                              df_event,
                              on="MESSAGE_ID",
                              how="left")
        # Cleaning
        df_message = df_message.drop("MESSAGE_ID", axis=1)
        return df_message.reset_index(drop=True)