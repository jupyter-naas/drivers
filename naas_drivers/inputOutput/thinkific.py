from naas_drivers.driver import InDriver, OutDriver
import pandas as pd
import requests
import os
import string
from datetime import datetime


class TKCRUD:
    # class TKCRUD(CRUD):
    def __init__(self, base_url, subdomain, auth):
        self.req_headers = {
            "X-Auth-API-Key": auth,
            "X-Auth-Subdomain": subdomain,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        self.base_url = base_url
        self.model_name = self.base_url.split("/")[-1]

        # Manage message
        message_dict = {
            "users": "User",
            "enrollments": "Enrollment",
            "courses": "Course",
            "groups": "Group",
            "group_users": "User group",
        }
        self.msg = message_dict[self.model_name]

    def __values_format(self, data):
        for key, value in data.items():
            # Check if value is not None
            if value is not None:
                # Force format to string  & delete space
                value = str(value).strip()

                # Specific rules
                if key == "first_name":
                    value = value.replace("-", " ")
                    value = string.capwords(value).replace(" ", "-")
                elif key == "last_name":
                    value = value.upper()
                elif key in ["activated_at", "expiry_date"]:
                    try:
                        value = datetime.strptime(value, "%d/%m/%Y")
                        value = value.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    except ValueError:
                        print(
                            f"'{value}' is not in the correct format.\n"
                            f"Please change it to %d/%m/%Y.\n"
                        )
                # Change value in dict
                data[key] = value
        return data

    def __get_by_page(self, page):
        data = {"page": page}
        req = requests.get(
            url=f"{self.base_url}/",
            headers=self.req_headers,
            json=data,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req.json()

    def get_all(self):
        items = []
        current_page = 1
        more_page = True
        while more_page:
            data = self.__get_by_page(current_page)
            items.extend(data.get("items"))
            total_pages = data.get("meta").get("pagination").get("total_pages") or 0
            if current_page == total_pages:
                more_page = False
            current_page += 1
        df = pd.DataFrame.from_records(items)
        return df

    def get(self, uid):
        try:
            req = requests.get(
                url=f"{self.base_url}/{uid}",
                headers=self.req_headers,
                allow_redirects=False,
            )
            req.raise_for_status()
            return req.json()
        except requests.HTTPError as err:
            err_code = err.response.status_code
            err_msg = err.response.json()
            to_print = f"{err_code}: {err_msg}"
            if err_code == 404:
                to_print = f"{self.msg} id (id={uid}) not found."
            print(to_print)

    def patch(self, data):
        data = self.__values_format(data)
        try:
            uid = data["id"]
            req = requests.put(
                url=f"{self.base_url}/{uid}",
                headers=self.req_headers,
                json=data,
                allow_redirects=False,
            )
            req.raise_for_status()
            # Message success
            to_print = f"{self.msg} (id={uid}) updated."
            if self.model_name == "users":
                email = data["email"]
                to_print = f"User '{email}' (id={uid}) updated."
            print(to_print)
        except requests.HTTPError as err:
            err_code = err.response.status_code
            err_msg = err.response.json()
            to_print = f"{err_code}: {err_msg}"
            if err_code == 404:
                to_print = f"{self.msg} id (id={uid}) not found."
            print(to_print)

    def send(self, data):
        data = self.__values_format(data)
        try:
            req = requests.post(
                url=f"{self.base_url}/",
                headers=self.req_headers,
                json=data,
                allow_redirects=False,
            )
            req.raise_for_status()
            try:
                res = req.json()
                uid = res["id"]
                print(f"{self.msg} successfully created (id={uid})!")
                return uid
            except ValueError:
                print("Send successfull ! No json returned")
        except requests.HTTPError as err:
            err_code = err.response.status_code
            err_msg = err.response.json()
            to_print = f"{err_code}: {err_msg}"
            print(to_print)

    def delete(self, uid):
        try:
            req = requests.delete(
                url=f"{self.base_url}/{uid}",
                headers=self.req_headers,
                allow_redirects=False,
            )
            req.raise_for_status()
            # Message success
            print(f"{self.msg} id (id={uid}) deleted.")
        except requests.HTTPError as err:
            err_code = err.response.status_code
            err_msg = err.response.json()
            to_print = f"{err_code}: {err_msg}"
            if err_code == 404:
                to_print = f"{self.msg} id (id={uid}) not found."
            print(to_print)


class User(TKCRUD):
    def update(
        self,
        uid,
        email=None,
        password=None,
        first_name=None,
        last_name=None,
        company=None,
    ):
        data = {
            "id": uid,
            "email": email,
            "password": password,
            "first_name": first_name,
            "last_name": last_name,
            "company": company,
        }
        res = self.patch(data)
        return res

    def create(self, email, password, first_name=None, last_name=None, company=None):
        data = {
            "email": email,
            "password": password,
            "first_name": first_name,
            "last_name": last_name,
            "company": company,
        }
        res = self.send(data)
        return res


class Enrollment(TKCRUD):
    def update(self, uid, activated=None, expired=None):
        data = {
            "id": uid,
            "activated_at": activated,
            "expiry_date": expired,
        }
        res = self.patch(data)
        return res

    def create(self, course_id, user_id, activated, expired):
        data = {
            "course_id": course_id,
            "user_id": user_id,
            "activated_at": activated,
            "expiry_date": expired,
        }
        res = self.send(data)
        return res


class Courses(TKCRUD):
    def get_chapters(self, uid):
        req = requests.get(
            url=f"{self.base_url}/{uid}/chapters",
            headers=self.req_headers,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req.json()


class Thinkific(InDriver, OutDriver):

    base_url = os.environ.get(
        "THINKIFIC_API_URL", "https://api.thinkific.com/api/public/v1"
    )
    api_token = None
    subdomain = None

    def connect(self, api_token, subdomain):
        # Init thinkific attribute
        self.token = api_token
        self.subdomain = subdomain

        # Init end point
        self.users = User(f"{self.base_url}/users", self.subdomain, self.token)
        self.enrollments = Enrollment(
            f"{self.base_url}/enrollments", self.subdomain, self.token
        )
        self.courses = Courses(f"{self.base_url}/courses", self.subdomain, self.token)
        self.groups = TKCRUD(f"{self.base_url}/groups", self.subdomain, self.token)
        self.group_users = TKCRUD(
            f"{self.base_url}/group_users", self.subdomain, self.token
        )

        # Set connexion to active
        self.connected = True
        return self
