from naas_drivers.driver import InDriver, OutDriver
import pandas as pd
import requests
import os


class Linkedin(InDriver, OutDriver):
    def __init__(self):
        self.auth_proxy = os.getenv("NAAS_AUTH_PROXY")

    def connect(
        self,
        email: str,
        password: str,
    ):
        tokens = self.__get_li_cookies(email, password)
        li_at = next(token for token in tokens if token["name"] == "li_at")
        JSESSIONID = next(token for token in tokens if token["name"] == "JSESSIONID")
        self.cookies = {"li_at": li_at, "JSESSIONID": JSESSIONID}
        self.headers = {
            "X-Li-Lang": "en_US",
            "Accept": "application/vnd.linkedin.normalized+json+2.1",
            "Cache-Control": "no-cache",
            "csrf-Token": self.cookies["JSESSIONID"].replace('"', ""),
            "X-Requested-With": "XMLHttpRequest",
            "X-Restli-Protocol-Version": "2.0.0",
        }
        return self

    def __get_li_cookies(
        self,
        email: str,
        password: str,
    ):
        cookie_response = requests.get(
            self.auth_proxy
            + "/token?url=https://www.linkedin.com/login&email="
            + email
            + "&password="
            + password
        )
        return cookie_response.json()["cookies"]

    def connections_count(
        self,
        username: str,
    ):

        profile_data = requests.get(
            "https://www.linkedin.com/voyager/api/identity/profiles/"
            + username
            + "/networkinfo",
            cookies=self.cookies,
            headers=self.headers,
        )
        return profile_data.json()["data"]["followersCount"]

    def get_conn_count(self, file):
        links = open(file, "r").readlines()
        followers_df = pd.DataFrame(columns=list(["LK_URL", "LK_FOLLOWERS"]))
        for link in links:
            username = link.rsplit("/")[-1]
            followers_df = followers_df.append(
                {
                    "LK_URL": link.replace("\n", ""),
                    "LK_FOLLOWERS": self.connections_count(username.replace("\n", "")),
                },
                ignore_index=True,
            )
        return followers_df

    def profile_details(self, file):
        links = open(file, "r").readlines()
        profiles_df = pd.DataFrame(
            columns=list(
                [
                    "LK_URL",
                    "LK_FIRST_NAME",
                    "LK_LAST_NAME",
                    "LK_HEADLINE",
                    "LK_EMAIL",
                    "LK_PHONE",
                    "LK_TWITTER",
                ]
            )
        )
        for link in links:
            username = link.rsplit("/")[-1]
            contact_data = requests.get(
                "https://www.linkedin.com/voyager/api/identity/profiles/"
                + username.replace("\n", "")
                + "/profileContactInfo",
                cookies=self.cookies,
                headers=self.headers,
            ).json()
            profile_data = requests.get(
                "https://www.linkedin.com/voyager/api/identity/profiles/"
                + username.replace("\n", ""),
                cookies=self.cookies,
                headers=self.headers,
            ).json()
            profiles_df = profiles_df.append(
                {
                    "LK_URL": link.replace("\n", ""),
                    "LK_FIRST_NAME": profile_data["data"]["firstName"],
                    "LK_LAST_NAME": profile_data["data"]["lastName"],
                    "LK_HEADLINE": profile_data["data"]["headline"] or "NA",
                    "LK_EMAIL": contact_data["data"]["emailAddress"] or "NA",
                    "LK_PHONE": contact_data["data"]["phoneNumbers"]
                    and contact_data["data"]["phoneNumbers"][0]["number"]
                    or "NA",
                    "LK_TWITTER": contact_data["data"]["twitterHandles"]
                    and contact_data["data"]["twitterHandles"][0]["name"]
                    or "NA",
                },
                ignore_index=True,
            )
        return profiles_df
