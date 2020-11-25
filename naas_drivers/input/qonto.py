from naas_drivers.driver import InDriver, OutDriver
import pandas as pd
import requests
import os
from datetime import datetime


class Organizations:
    def __init__(self, user_id, api_key):
        self.base_url = os.environ.get(
            "QONTO_API_URL", "http://thirdparty.qonto.eu/v2"
        )
        self.req_headers = {
            "authorization": f'{user_id}:{api_key}'
        }
        self.url = f"{self.base_url}/organizations"
        self.user_id = user_id
        
    def get(self):
        try:
            req = requests.get(
                url=f"{self.url}/{user_id}",
                headers=self.req_headers
            )
            req.raise_for_status()
            items = req.json()['organization']['bank_accounts']
            df = pd.DataFrame.from_records(items)
            return df
        except requests.HTTPError as err:
            err_code = err.response.status_code
            err_msg = err.response.json()
            to_print = f"{err_code}: {err_msg}"
            print(to_print)

            
class Transactions(Organizations):
    def get_all(self):
        # Get organizations
        df_organisations = self.get()

        # For each bank account, get all transactions
        df_transaction = pd.DataFrame()
        for _, row in df_organisations.iterrows():
            slug = row['slug']
            iban = row['iban']
            
            # Get transactions
            current_page = "1"
            has_more = True
            while has_more:
                req = requests.get(
                    url=f'{self.base_url}/transactions?current_page={current_page}?per_page=100&slug={slug}&iban={iban}',
                    headers=self.req_headers
                )
                items = req.json()
                transactions = items['transactions']
                df = pd.DataFrame.from_records(transactions)
                df['iban'] = iban
                df_transaction = pd.concat([df_transaction, df], axis=0)
                
                # Check if next page exists
                next_page = items["meta"]["next_page"]
                if next_page is None:
                    has_more = False
                else:
                    current_page = str(next_page)
        return df_transaction

            
class Qonto(InDriver):
    user_id = None
    api_token = None

    def connect(self, user_id, api_token):
        # Init thinkific attribute
        self.user_id = user_id
        self.token = api_token
        
        # Init end point
        self.organizations = Organizations(self.user_id, self.token)
        self.transactions = Transactions(self.user_id, self.token)
        
        # Set connexion to active
        self.connected = True
        return self