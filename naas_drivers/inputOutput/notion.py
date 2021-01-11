from naas_drivers.driver import InDriver, OutDriver
import pandas as pd
import requests
from notion.client import NotionClient
import os


class Notion(InDriver, OutDriver):
    def __init__(self):
        self.auth_proxy = os.getenv("NAAS_AUTH_PROXY")

    def connect(
        self,
        email: str,
        password: str,
    ):
        self.client = NotionClient(token_v2=self.__token_v2(email, password))
        self.connected = True
        return self

    def __token_v2(
        self,
        email: str,
        password: str,
    ):
        cookie_response = requests.get(
            self.auth_proxy
            + "/token?url=https://www.notion.so/login&filter=token_v2&email="
            + email
            + "&password="
            + password
        )
        return cookie_response.json()["cookies"][0]["value"]

    def get(
        self,
        url: str,
    ):
        self.check_connect()
        cv = self.client.get_collection_view(url)
        data = [
            block_row.get_all_properties() for block_row in cv.collection.get_rows()
        ]
        return pd.DataFrame(data)
