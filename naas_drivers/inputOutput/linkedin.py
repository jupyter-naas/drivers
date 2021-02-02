from naas_drivers.driver import InDriver, OutDriver
import pandas as pd
import requests


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
        profil = self.get_identity(username)
        network = self.get_network(username)
        contact = self.get_contact(username)

        # Get profil info
        pf = profil["data"]
        firstname = pf["firstName"]
        lastname = pf["lastName"].upper()
        bd_day = None
        bd_month = None
        bd_year = None
        bd = None
        if pf["birthDate"] is not None:
            bd_day = pf["birthDate"]["day"]
            bd_month = pf["birthDate"]["month"]
            bd_year = pf["birthDate"]["year"]
            bd = f"{bd_day}/{bd_month}/{bd_year}"
        country = pf["geoCountryName"]
        adress = pf["geoLocationName"]
        lk_headline = pf["headline"]
        lk_industry = pf["industryName"]

        # Get network info
        nw = network["data"]
        lk_followers = nw["followersCount"]

        # Get contact info
        ct = contact["data"]
        lk_phone = None
        lk_phones = ct["phoneNumbers"]
        if lk_phones is not None:
            for rows in lk_phones:
                if rows["type"] == "MOBILE":
                    lk_phone = rows["number"]
                    break
        lk_email = ct["emailAddress"]
        lk_twiter = None
        lk_twiters = ct["twitterHandles"]
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
            "ADRESS": adress,
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
