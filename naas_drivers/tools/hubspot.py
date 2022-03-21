from naas_drivers.driver import InDriver, OutDriver
import pandas as pd
import requests
from datetime import datetime
import os
import string
import json
import re

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


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
                    if value == "nan":
                        value = ""
                    else:
                        value = value.replace(" ", "").replace(".", "")
                elif key == "closedate":
                    try:
                        value = datetime.strptime(value, "%Y-%m-%d")
                        value = str(int(value.timestamp())) + "000"
                    except ValueError:
                        print(
                            f"❌ Close date '{value}' is in wrong format.\n"
                            "Please change it to %Y-%m-%d."
                        )
                elif key == "amount":
                    if value == "nan":
                        value = 0
                    else:
                        value = value.replace(".", "")
                # Change value in dict
                data["properties"][key] = value
        return data

    def __get_by_page(self, params):
        res = requests.get(
            url=f"{self.base_url}/",
            headers=self.req_headers,
            params=params,
            allow_redirects=False,
        )
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        return res.json()

    def get_all(self, columns=None):
        items = []
        params = self.params
        if columns is not None:
            params["properties"] = columns
        more_page = True
        while more_page:
            data = self.__get_by_page(params)
            for row in data.get("results"):
                properties = row["properties"]
                items.append(properties)
            if "paging" in data:
                params["after"] = data.get("paging").get("next").get("after")
            else:
                more_page = False
        df = pd.DataFrame(items).reset_index(drop=True)
        params.pop("after")
        if columns is not None:
            params.pop("properties")
            self.params = params
            for col in columns:
                if col not in df.columns:
                    index = columns.index(col)
                    columns.pop(index)
            # Reorder columns
            df = df[columns]
        return df

    def get(self, uid, columns=None):
        params = self.params
        if columns is not None:
            params["properties"] = columns
        res = requests.get(
            url=f"{self.base_url}/{uid}",
            headers=self.req_headers,
            params=params,
            allow_redirects=False,
        )
        if columns is not None:
            params.pop("properties")
            self.params = params
        self.params = params
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        return res.json()

    def patch(self, uid, data):
        data = self.__values_format(data)
        res = requests.patch(
            url=f"{self.base_url}/{uid}",
            headers=self.req_headers,
            params=self.params,
            json=data,
            allow_redirects=False,
        )
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        # Message success
        print(f"✔️ {self.msg} (id={uid}) successfully updated.")
        return res.json()

    def send(self, data):
        data = self.__values_format(data)
        res = requests.post(
            url=f"{self.base_url}/",
            headers=self.req_headers,
            params=self.params,
            json=data,
            allow_redirects=False,
        )
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        res_json = res.json()
        uid = res_json.get("id")
        # Message success
        print(f"✔️ {self.msg} (id={uid}) successfully created.")
        return uid

    def delete(self, uid):
        result = self.get(uid)
        if result is not None:
            res = requests.delete(
                url=f"{self.base_url}/{uid}",
                headers=self.req_headers,
                params=self.params,
                allow_redirects=False,
            )
            try:
                res.raise_for_status()
            except requests.HTTPError as e:
                return e
            # Message success
            print(f"✔️ {self.msg} (id={uid}) successfully deleted.")
            return uid
        else:
            # Message success
            print(f"❌ {self.msg} (id={uid}) does not exist.")


class Contact(HSCRUD):
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

    def get_id(self, email):
        uid = None
        try:
            # Try to create contact with email
            data = {"properties": {"email": email}}
            uid = self.send(data)
        except requests.HTTPError as err:
            # Get contact id in pattern if error
            pattern = r"\: (\d)+"
            response_txt = err.response.text
            uid = re.search(pattern, response_txt).group(0).replace(": ", "")
        return uid


class Deal(HSCRUD):
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


class Pipeline:
    def __init__(self, base_url, req_headers, params):
        self.req_headers = req_headers
        self.params = params
        self.base_url = base_url
        self.model_name = self.base_url.split("/")[-1]

    def get_all(self, pipeline=None, pipeline_id=None):
        res = requests.get(
            url=f"{self.base_url}/",
            headers=self.req_headers,
            params=self.params,
            allow_redirects=False,
        )
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        deal_stages = []
        results = res.json().get("results")
        for res in results:
            stages = res.get("stages")
            for stage in stages:
                deal_stage = {
                    "pipeline": res.get("label"),
                    "pipeline_id": res.get("id"),
                    "dealstage_label": stage.get("label"),
                    "dealstage_id": stage.get("id"),
                    "displayOrder": stage.get("displayOrder"),
                    "dealclosed": stage.get("metadata", {}).get("isClosed"),
                    "probability": stage.get("metadata", {}).get("probability"),
                    "createdAt": stage.get("createdAt"),
                    "updatedAt": stage.get("updatedAt"),
                    "archived": stage.get("archived"),
                }
                deal_stages.append(deal_stage)
        # Create DataFrame
        df = pd.DataFrame(deal_stages)
        df.createdAt = pd.to_datetime(df.createdAt).dt.strftime(DATETIME_FORMAT)
        df.updatedAt = pd.to_datetime(df.updatedAt).dt.strftime(DATETIME_FORMAT)

        # Filter on pipeline
        if pipeline is not None:
            df = df[df.pipeline == str(pipeline)]
        if pipeline_id is not None:
            df = df[df.pipeline_id == str(pipeline_id)]
        # Cleaning
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
                f"❌ Object '{object_name}' does not exist.\n"
                f"Please chose one in following list: {objects}"
            )
        associates = ["contact", "engagements"]
        associate_check = True
        if associate not in associates:
            associate_check = False
            print(
                f"❌ Associate '{associate}' does not exist.\n"
                f"Please chose one in following list: {associates}"
            )
        if object_check and associate_check:
            res = requests.get(
                url=f"{self.base_url}/{object_name}/{object_id}/"
                f"associations/{associate}",
                headers=self.req_headers,
                params=self.params,
                allow_redirects=False,
            )
            try:
                res.raise_for_status()
            except requests.HTTPError as e:
                return e
            data = res.json()
            df = pd.DataFrame.from_records(data["results"])
            if len(df) == 0:
                print(f"❌ Object id='{object_id}' does not have associates.")
            return df

    def create(self, object_name, object_id, associate, id_associate):
        objects = ["deal"]
        object_check = True
        if object_name not in objects:
            object_check = False
            print(
                f"❌ Object '{object_name}' does not exist.\n"
                f"Please chose one in following list: {objects}"
            )
        associates = ["contact"]
        associate_check = True
        if associate not in associates:
            associate_check = False
            print(
                f"❌ Associate '{associate}' does not exist.\n"
                f"Please chose one in following list: {associates}"
            )
        if object_check and associate_check:
            res = requests.put(
                url=f"{self.base_url}/{object_name}/{object_id}/associations/"
                f"{associate}/{id_associate}/{object_name}_to_{associate}",
                headers=self.req_headers,
                params=self.params,
                allow_redirects=False,
            )
            try:
                res.raise_for_status()
            except requests.HTTPError as e:
                return e
            print(
                f"✔️ {object_name} '{object_id}' and {associate} "
                f"'{id_associate}' successfully associated !"
            )
            return id_associate


class Note:
    def __init__(self, base_url, req_headers, params):
        self.req_headers = req_headers
        self.params = params
        self.base_url = base_url

    def create(
        self, content, contactids=None, dealids=None, companyids=None, ownerid=None
    ):
        payload = json.dumps(
            {
                "engagement": {
                    "active": "true",
                    "ownerId": ownerid,
                    "type": "NOTE",
                    "timestamp": str(int(datetime.now().timestamp())) + "000",
                },
                "associations": {
                    "contactIds": [contactids],
                    "dealIds": [dealids],
                    "companyIds": [companyids],
                },
                "attachments": [{}],
                "metadata": {"body": content},
            }
        )
        res = requests.post(
            self.base_url,
            data=payload,
            headers=self.req_headers,
            params=self.params,
            allow_redirects=False,
        )
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        # Message success
        return "✔️ Note successfully created."


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
        self.pipelines = Pipeline(
            f"{self.pip_url}/deals", self.req_headers, self.params
        )
        self.associations = Association(
            f"{self.obj_url}", self.req_headers, self.params
        )
        self.notes = Note(
            "https://api.hubapi.com/engagements/v1/engagements",
            self.req_headers,
            self.params,
        )

        # Set connexion to active
        self.connected = True
        return self
