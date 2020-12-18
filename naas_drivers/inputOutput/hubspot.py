from naas_drivers.driver import InDriver, OutDriver
import pandas as pd
import requests
from datetime import datetime
import os
import string


class HSCRUD:
    # class HSCRUD(CRUD):
    def __init__(self, base_url, req_headers, params):
        self.req_headers = req_headers
        self.params = params
        self.base_url = base_url
        self.model_name = self.base_url.split("/")[-1]

        # Manage message
        message_dict = {"contacts": "Contact", "company": "Company", "deals": "Deal"}
        self.msg = message_dict[self.model_name]

    def __values_format(self, data):
        for key, value in data["properties"].items():
            # Check if value is not None
            if value is not None:
                # Force format to string  & delete space
                value = str(value).strip()
                # Specific rules
                if key == "firstname":
                    value = value.replace("-", " ")
                    value = string.capwords(value).replace(" ", "-")
                elif key == "lastname":
                    value = value.upper()
                elif key == "phone":
                    value = value.replace(" ", "").replace(".", "")
                elif key == "closedate":
                    try:
                        value = datetime.strptime(value, "%d/%m/%Y")
                        value = str(int(value.timestamp())) + "000"
                    except ValueError:
                        print(
                            f"Close date '{value}' is in wrong format.\n"
                            "Please change it to %d/%m/%Y."
                        )
                elif key == "amount":
                    value = value.replace(".", "")
                # Change value in dict
                data["properties"][key] = value
        return data

    def __get_by_page(self, params):
        req = requests.get(
            url=f"{self.base_url}/",
            headers=self.req_headers,
            params=params,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req.json()

    def get_all(self):
        items = []
        params = self.params
        more_page = True
        while more_page:
            data = self.__get_by_page(params)
            for row in data["results"]:
                properties = row["properties"]
                items.append(properties)
            if "paging" in data:
                params["after"] = data["paging"]["next"]["after"]
            else:
                more_page = False
        params.pop("after")
        self.params = params
        df = pd.DataFrame(items).reset_index(drop=True)
        return df

    def get(self, uid):
        try:
            req = requests.get(
                url=f"{self.base_url}/{uid}",
                headers=self.req_headers,
                params=self.params,
                allow_redirects=False,
            )
            req.raise_for_status()
            res = req.json()
            return res
        except requests.HTTPError as err:
            err_code = err.response.status_code
            if err_code == 404:
                print(f"{self.msg} id='{uid}' does not exist.")

    def patch(self, uid, data):
        data = self.__values_format(data)
        try:
            req = requests.patch(
                url=f"{self.base_url}/{uid}",
                headers=self.req_headers,
                params=self.params,
                json=data,
                allow_redirects=False,
            )
            req.raise_for_status()
            res = req.json()
            # Message success
            print(f"{self.msg} (id={uid}) successfully updated.")
            return res
        except requests.HTTPError as err:
            err_code = err.response.status_code
            err_msg = err.response.json()
            if err_code == 404:
                print(f"{self.msg} id='{uid}' does not exist.")
            else:
                print(f"{err_code}: {err_msg}")

    def send(self, data):
        data = self.__values_format(data)
        try:
            req = requests.post(
                url=f"{self.base_url}/",
                headers=self.req_headers,
                params=self.params,
                json=data,
                allow_redirects=False,
            )
            req.raise_for_status()
            res = req.json()
            uid = res["id"]
            # Message success
            print(f"{self.msg} (id={uid}) successfully created.")
            return uid
        except requests.HTTPError as err:
            err_code = err.response.status_code
            err_msg = err.response.json()
            print(f"{err_code}: {err_msg}")

    def delete(self, uid):
        res = self.get(uid)
        if res is not None:
            try:
                req = requests.delete(
                    url=f"{self.base_url}/{uid}",
                    headers=self.req_headers,
                    params=self.params,
                    allow_redirects=False,
                )
                req.raise_for_status()
                # Message success
                print(f"{self.msg} (id={uid}) deleted.")
            except requests.HTTPError as err:
                err_code = err.response.status_code
                err_msg = err.response.json()
                print(f"{err_code}: {err_msg}")


class Contact(HSCRUD):
    def update(
        self,
        uid,
        email=None,
        firstname=None,
        lastname=None,
        phone=None,
        jobtitle=None,
        website=None,
        company=None,
        hubspot_owner_id=None,
    ):
        data = {
            "properties": {
                "email": email,
                "firstname": firstname,
                "lastname": lastname,
                "phone": phone,
                "jobtitle": jobtitle,
                "website": website,
                "company": company,
                "hubspot_owner_id": hubspot_owner_id,
            }
        }
        res = self.patch(uid, data)
        return res

    def create(
        self,
        email,
        firstname=None,
        lastname=None,
        phone=None,
        jobtitle=None,
        website=None,
        company=None,
        hubspot_owner_id=None,
    ):
        data = {
            "properties": {
                "email": email,
                "firstname": firstname,
                "lastname": lastname,
                "phone": phone,
                "jobtitle": jobtitle,
                "website": website,
                "company": company,
                "hubspot_owner_id": hubspot_owner_id,
            }
        }
        res = self.send(data)
        return res


class Deal(HSCRUD):
    def update(
        self,
        uid,
        dealname=None,
        dealstage=None,
        closedate=None,
        amount=None,
        hubspot_owner_id=None,
    ):
        data = {
            "properties": {
                "dealstage": dealstage,
                "dealname": dealname,
                "closedate": closedate,
                "amount": amount,
                "hubspot_owner_id": hubspot_owner_id,
            }
        }
        res = self.patch(uid, data)
        return res

    def create(
        self, dealname, dealstage, closedate=None, amount=None, hubspot_owner_id=None
    ):
        data = {
            "properties": {
                "dealstage": dealstage,
                "dealname": dealname,
                "closedate": closedate,
                "amount": amount,
                "hubspot_owner_id": hubspot_owner_id,
            }
        }
        res = self.send(data)
        return res


class Pipeline:
    def __init__(self, base_url, req_headers, params):
        self.req_headers = req_headers
        self.params = params
        self.base_url = base_url
        self.model_name = self.base_url.split("/")[-1]

    def get_all_pipeline(self):
        req = requests.get(
            url=f"{self.base_url}/",
            headers=self.req_headers,
            params=self.params,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req.json()


class Pipelines(Pipeline):
    def get_all(self):
        data = self.get_all_pipeline()
        df = pd.DataFrame.from_records(data["results"])
        df = df.drop(["stages"], axis=1)
        df = df.sort_values(by=["displayOrder"]).reset_index(drop=True)
        return df


class Dealstage(Pipeline):
    def get_all(self):
        data = self.get_all_pipeline()
        items = []
        for row in data["results"]:
            pipeline = row["label"]
            stages = row["stages"]
            id_pipeline = row["id"]
            for stage in stages:
                label = stage["label"]
                display_order = stage["displayOrder"]
                id_dealstage = stage["id"]
                created_at = stage["createdAt"]
                updated_at = stage["updatedAt"]
                archived = stage["archived"]

                deal_stage = {
                    "pipeline": pipeline,
                    "id_pipeline": id_pipeline,
                    "dealstage": label,
                    "id_dealstage": id_dealstage,
                    "displayOrder": display_order,
                    "createdAt": created_at,
                    "updatedAt": updated_at,
                    "archived": archived,
                }
                items.append(deal_stage)
        df = pd.DataFrame(items)
        df = df.sort_values(by=["pipeline", "displayOrder"])
        df = df.reset_index(drop=True)
        return df


class Association:
    def __init__(self, base_url, req_headers, params):
        self.req_headers = req_headers
        self.params = params
        self.base_url = base_url
        self.model_name = self.base_url.split("/")[-1]

    def get(self, object_name, object_id, associate):
        objects = ["deal"]
        object_check = True
        if object_name not in objects:
            object_check = False
            print(
                f"Object '{object_name}' does not exist.\n"
                f"Please chose one in following list: {objects}"
            )
        associates = ["contact"]
        associate_check = True
        if associate not in associates:
            associate_check = False
            print(
                f"Associate '{associate}' does not exist.\n"
                f"Please chose one in following list: {associates}"
            )
        if object_check and associate_check:
            try:
                req = requests.get(
                    url=f"{self.base_url}/{object_name}/{object_id}/"
                    f"associations/{associate}",
                    headers=self.req_headers,
                    params=self.params,
                    allow_redirects=False,
                )
                req.raise_for_status()
                data = req.json()
                df = pd.DataFrame.from_records(data["results"])
                if len(df) == 0:
                    print(f"Object id='{object_id}' does not have associates.")
                return df
            except requests.HTTPError as err:
                err_code = err.response.status_code
                err_msg = err.response.json()
                print(f"{err_code}: {err_msg}")

    def create(self, object_name, object_id, associate, id_associate):
        objects = ["deal"]
        object_check = True
        if object_name not in objects:
            object_check = False
            print(
                f"Object '{object_name}' does not exist.\n"
                f"Please chose one in following list: {objects}"
            )
        associates = ["contact"]
        associate_check = True
        if associate not in associates:
            associate_check = False
            print(
                f"Associate '{associate}' does not exist.\n"
                f"Please chose one in following list: {associates}"
            )
        if object_check and associate_check:
            try:
                req = requests.put(
                    url=f"{self.base_url}/{object_name}/{object_id}/associations/"
                    f"{associate}/{id_associate}/{object_name}_to_{associate}",
                    headers=self.req_headers,
                    params=self.params,
                    allow_redirects=False,
                )
                req.raise_for_status()
                print(
                    f"{object_name} '{object_id}' and {associate} "
                    f"'{id_associate}' successfully associated !"
                )
            except requests.HTTPError as err:
                err_code = err.response.status_code
                err_msg = err.response.json()
                print(f"{err_code}: {err_msg}")


class Hubspot(InDriver, OutDriver):

    base_url = os.environ.get("HUBSPOT_API_URL", "https://api.hubapi.com/crm/v3")
    api_token = None

    def connect(self, api_token):
        # Init Hubspot attribute
        self.token = api_token
        self.req_headers = {
            "accept": "application/json",
            "content-type": "application/json",
        }
        self.params = {"limit": "100", "archived": "false", "hapikey": api_token}

        # Init end point
        self.obj_url = f"{self.base_url}/objects"
        self.pip_url = f"{self.base_url}/pipelines"
        self.contacts = Contact(
            f"{self.obj_url}/contacts", self.req_headers, self.params
        )
        self.company = HSCRUD(f"{self.obj_url}/company", self.req_headers, self.params)
        self.deals = Deal(f"{self.obj_url}/deals", self.req_headers, self.params)
        self.pipelines = Pipelines(
            f"{self.pip_url}/deals", self.req_headers, self.params
        )
        self.dealstages = Dealstage(
            f"{self.pip_url}/deals", self.req_headers, self.params
        )
        self.associations = Association(
            f"{self.obj_url}", self.req_headers, self.params
        )

        # Set connexion to active
        self.connected = True
        return self
