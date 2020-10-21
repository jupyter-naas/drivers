import os
from urllib.parse import urljoin

import requests
import pandas as pd


BIG_NUM_TO_GETALL = 1000000


class Gsheet:
    def __init__(self):
        self.sheets_api = os.getenv("GSHEETS_API")
        if not self.sheets_api:
            raise ValueError("GSHEETS_API not defined!")

    def get(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        items_per_page: int = BIG_NUM_TO_GETALL,
    ) -> pd.DataFrame:
        resp = requests.get(
            urljoin(self.sheets_api, f"{spreadsheet_id}/{sheet_name}"),
            params={"perPage": items_per_page},
        )
        if resp.status_code != 200:
            raise ValueError(resp.text)
        data = resp.json()
        df = pd.DataFrame(data=data["data"], columns=data["columns"])
        return df
