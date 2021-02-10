from naas_drivers.driver import InDriver, OutDriver
from notion.client import NotionClient
import pandas as pd
import requests
import os


class Notion(InDriver, OutDriver):
    def __init__(self):
        self.auth_proxy = os.getenv("NAAS_AUTH_PROXY")

    def connect(
        self,
        token: str = None,
        email: str = None,
        password: str = None,
    ):
        if token:
            self.client = NotionClient(token)
        elif email and password:
            self.client = NotionClient(token_v2=self.__token_v2(email, password))
        else:
            self.print_error("You should provide, token or email/pasword")
        self.connected = True
        return self

    def __token_v2(
        self,
        email: str,
        password: str,
    ):
        url = f"https://www.notion.so/login&filter=token_v2&email={email}&password={password}"
        cookie_response = requests.get(f"{self.auth_proxy}/token?url={url}")
        return cookie_response.json()["cookies"][0]["value"]

    def get(self, url: str):
        self.check_connect()
        page = self.client.get_block(url)
        return page

    def get_collection(
        self,
        url: str,
        raw: bool = False,
    ):
        self.check_connect()
        cv = self.client.get_collection_view(url)
        if raw:
            return cv
        else:
            data = [
                block_row.get_all_properties() for block_row in cv.collection.get_rows()
            ]
            return pd.DataFrame(data)
