import pandas as pd
import requests
from datetime import datetime

DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
NUMBER_FORMAT = "{:,.2f} €"


class BudgetInsight:
    def connect(self, domain, client_id, client_secret, uuid=None):
        # Init atributes
        self.api_url = f"https://{domain}/2.0"

        if uuid is not None:
            req_url = f"{self.api_url}/auth/renew"
            res = requests.post(
                req_url,
                data={
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "id_user": uuid,
                },
            )
            try:
                res.raise_for_status()
            except requests.HTTPError as e:
                return e
            res_json = res.json()
            token = res_json.get("access_token")
        else:
            req_url = f"{self.api_url}/auth/init"
            res = requests.post(
                req_url, data={"client_id": client_id, "client_secret": client_secret}
            )
            try:
                res.raise_for_status()
            except requests.HTTPError as e:
                return e
            res_json = res.json()
            id_user = res_json.get("id_user")
            print(f"User created. Keep your user id for next connection : {id_user}")
            token = res_json.get("auth_token")
        # Init headers
        self.headers = {"authorization": f"Bearer {token}"}

        # Init end point
        self.connections = Connections(self.api_url, self.headers)
        self.accounts = Accounts(self.api_url, self.headers)
        self.transactions = Transactions(self.api_url, self.headers)
        self.connectors = Connectors(self.api_url)

        # Set connexion to active
        self.connected = True
        return self


class Connectors(BudgetInsight):
    def __init__(self, api_url):
        BudgetInsight.__init__(self)
        self.api_url = api_url

    def get(self):
        req_url = f"{self.api_url}/connectors/"
        res = requests.get(req_url)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        res_json = res.json()
        connectors = res_json.get("connectors")
        df = pd.DataFrame(connectors)
        return df

    def get_fields(self, bq_id):
        req_url = f"{self.api_url}/connectors/{bq_id}/fields"
        res = requests.get(req_url)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        res_json = res.json()
        fields = res_json.get("fields")
        return fields

    def get_referential(self):
        df = Connectors.get(self)
        df = df[["id", "name"]]
        df = df.rename(columns={"id": "id_bank", "name": "bank_name"})
        return df


class Connections(BudgetInsight):
    def __init__(self, api_url, headers):
        BudgetInsight.__init__(self)
        self.api_url = api_url
        self.headers = headers

    def create(self, data):
        req_url = f"{self.api_url}/users/me/connections"
        res = requests.post(req_url, data=data, headers=self.headers)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        res_json = res.json()
        return res_json

    def update(self, data, connection_id):
        req_url = f"{self.api_url}/users/me/connections/{connection_id}"
        res = requests.post(req_url, data=data, headers=self.headers)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        res_json = res.json()
        return res_json

    def force(self, connection_id):
        req_url = f"{self.api_url}/users/me/connections/{connection_id}"
        res = requests.put(req_url, headers=self.headers)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        res_json = res.json()
        return res_json

    def get(self):
        req_url = f"{self.api_url}/users/me/connections"
        res = requests.get(req_url, headers=self.headers)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        res_json = res.json()
        connections = res_json.get("connections")
        df = pd.DataFrame(connections)
        return df

    def delete(self, connection_id):
        req_url = f"{self.api_url}/users/me/connections/{connection_id}"
        res = requests.delete(req_url, headers=self.headers)
        try:
            res.raise_for_status()
        except ValueError:
            print(
                f"❌ Connection (id={connection_id}) does not exist. Please enter a valid id."
            )
            return self.get()
        return f"✔️ Connection (id={connection_id}) successfully deleted."

    def get_referential(self):
        # Get ref connector
        ref_connectors = Connectors.get_referential(self)

        # Get connections
        df = Connections.get(self)
        df = df[["id", "id_user", "id_bank", "state", "error"]]

        # Create ref connections
        df = pd.merge(df, ref_connectors, on="id_bank")
        df = df.rename(
            columns={
                "id": "id_connection",
                "state": "connection_state",
                "error": "connection_error",
            }
        )
        return df


class Accounts(BudgetInsight):
    def __init__(self, api_url, headers):
        BudgetInsight.__init__(self)
        self.api_url = api_url
        self.headers = headers

    def get(self, account_id=None):
        if account_id is None:
            req_url = f"{self.api_url}/users/me/accounts?all"
        else:
            req_url = f"{self.api_url}/users/me/accounts/{account_id}?all"
        res = requests.get(req_url, headers=self.headers)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        res_json = res.json()
        accounts = res_json.get("accounts")
        if accounts is None:
            return res_json
        else:
            return pd.DataFrame(accounts)

    def update(self, account_id, data=None):
        req_url = f"{self.api_url}/users/me/accounts/{account_id}?all"
        res = requests.put(req_url, data=data, headers=self.headers)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        res_json = res.json()
        return res_json

    def enable(self, account_id):
        account = self.get(account_id)
        if account.get("disabled") is not None:
            data = {"disabled": False}
            self.update(account_id, data)
            return f"✔️ Account (id={account_id}) enabled. You can now access to all your transactions."
        else:
            return "Account already enabled."

    def enable_all(self):
        enables = []
        df = self.get()
        for _, row in df.iterrows():
            account_id = row.id
            number = row.number
            name = row.original_name
            disabled = row.disabled
            if disabled is None:
                self.enable(account_id)
                enable = "Account already enabled."
            else:
                enable = "✔️ Account enabled"
            data = {
                "ACCOUNT_ID": account_id,
                "ACCOUNT_NUMBER": number,
                "ACCOUNT_NAME": name,
                "ENABLE": enable,
            }
            enables.append(data)
        return pd.DataFrame(enables)

    def get_referential(self):
        # Get ref connections
        ref_connections = Connections.get_referential(self)

        # Get accounts
        df_accounts = Accounts.get(self)

        # Create ref accounts
        ref_accounts = pd.merge(
            df_accounts, ref_connections, on=["id_connection", "id_user"]
        )
        to_keep = [
            "id_user",
            "id_connection",
            "connection_state",
            "connection_error",
            "id",
            "number",
            "original_name",
            "type",
            "iban",
            "bic",
            "id_bank",
            "bank_name",
        ]
        ref_accounts = ref_accounts[to_keep]
        to_rename = {
            "id": "id_account",
            "number": "account_number",
            "original_name": "account_name",
            "type": "account_type",
        }
        ref_accounts = ref_accounts.rename(columns=to_rename)
        ref_accounts = ref_accounts.drop_duplicates(
            subset=["account_number"]
        ).reset_index(drop=True)
        return ref_accounts

    def get_statement(self):
        # Get accounts
        df = Accounts.get(self)
        to_rename = {"id": "id_account"}
        df = df.rename(columns=to_rename)

        # Get ref accounts
        ref = Accounts.get_referential(self)

        # Create df positions
        df_statement = pd.merge(df, ref, on=["id_connection", "id_user", "id_account"])

        to_keep = [
            "id_connection",
            "connection_state",
            "bank_name",
            "id_account",
            "account_number",
            "account_name",
            "account_type",
            "balance",
            "last_update",
        ]
        df_statement = df_statement[to_keep]
        df_statement.columns = df_statement.columns.str.upper()
        df_statement = df_statement.drop_duplicates(
            subset=["ACCOUNT_NUMBER"]
        ).reset_index(drop=True)
        df_statement["DATE_EXTRACTION"] = datetime.now().strftime(DATETIME_FORMAT)
        return df_statement


class Transactions(BudgetInsight):
    def __init__(self, api_url, headers):
        BudgetInsight.__init__(self)
        self.api_url = api_url
        self.headers = headers

    def get(self):
        req_url = f"{self.api_url}/users/me/transactions?all"
        res = requests.get(req_url, headers=self.headers)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        res_json = res.json()
        transactions = res_json.get("transactions")
        df = pd.DataFrame(transactions)
        return df

    def get_statement(self):
        # Get transactions
        df = Transactions.get(self)
        to_rename = {
            "id": "id_transaction",
            "original_wording": "transaction_label",
            "type": "transaction_type",
            "value": "amount",
        }
        df = df.rename(columns=to_rename)

        # Get ref accounts
        ref = Accounts.get_referential(self)

        # Create statement
        df_statement = pd.merge(df, ref, on="id_account")

        # Select column to keep
        to_keep = [
            "account_number",
            "account_name",
            "bank_name",
            "id_transaction",
            "date",
            "transaction_label",
            "transaction_type",
            "amount",
        ]
        df_statement = df_statement[to_keep]
        df_statement.columns = df_statement.columns.str.upper()
        df_statement = df_statement.drop_duplicates(
            subset=["ACCOUNT_NUMBER", "ID_TRANSACTION"]
        ).reset_index(drop=True)
        df_statement["DATE_EXTRACTION"] = datetime.now().strftime(DATETIME_FORMAT)
        return df_statement
