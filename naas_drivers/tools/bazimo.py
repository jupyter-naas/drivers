import requests
import pandas as pd


class Bazimo:
    def connect(self, email: str, password: str):

        # Connect to Bazimo
        url = "https://bazimo-api.azurewebsites.net/api/tenant/app/authentications/jwt"

        json = {
            "email": f"{email}",
            "password": f"{password}",
            "authenticationType": "bearer",
        }
        res = requests.post(url, json=json)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        res_json = res.json()
        token = res_json.get("token")

        # Init token
        self.token = token

        # Init end point
        self.exports = Exports(self.token)

        # Set connexion to active
        self.connected = True
        return self


class Exports(Bazimo):
    def __init__(self, token):
        Bazimo.__init__(self)
        self.token = token

    def get(self, name):
        if name == "Locataires":
            scope = "1"
            header = 6
            sheet_name = "Locataires"
        if name == "Lots":
            scope = "2"
            header = 6
            sheet_name = "Lots"
        if name == "Baux":
            scope = "3"
            header = 6
            sheet_name = "Baux"
        if name == "Actifs":
            scope = "9"
            header = 6
            sheet_name = "Actifs"
        if name == "Factures":
            scope = "11"
            header = 5
            sheet_name = "Détail factures"
        # Request url
        url = (
            f"https://bazimo-api.azurewebsites.net/api/tenant/app/exports?scope={scope}"
        )
        headers = {"Authorization": f"bearer {self.token}"}
        res = requests.get(url, headers=headers)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        # Read Excel
        df = pd.read_excel(res.content, sheet_name=sheet_name, header=header)
        return df
